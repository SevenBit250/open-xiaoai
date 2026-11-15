use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::future::Future;
use std::sync::{Arc, LazyLock};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

use super::rpc::RPC;
use crate::base::AppError;
use crate::utils::task::TaskManager;

use super::data::{AppMessage, Event, Request, Response, Stream};
use super::handler::MessageHandler;

pub enum WsStream {
    Server(WebSocketStream<TcpStream>),
    Client(WebSocketStream<MaybeTlsStream<TcpStream>>),
}

pub enum WsReader {
    Server(SplitStream<WebSocketStream<TcpStream>>),
    Client(SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>),
}

pub enum WsWriter {
    Server(SplitSink<WebSocketStream<TcpStream>, Message>),
    Client(SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>),
}

pub struct MessageManager {
    semaphore: Arc<Semaphore>,
    reader: Arc<Mutex<Option<WsReader>>>,
    writer: Arc<Mutex<Option<WsWriter>>>,
}

static INSTANCE: LazyLock<MessageManager> = LazyLock::new(MessageManager::new);

impl MessageManager {
    fn new() -> Self {
        Self {
            reader: Arc::new(Mutex::new(None)),
            writer: Arc::new(Mutex::new(None)),
            semaphore: Arc::new(Semaphore::new(32)),
        }
    }

    pub fn instance() -> &'static Self {
        &INSTANCE
    }

    pub async fn init(&self, ws_stream: WsStream) {
        match ws_stream {
            WsStream::Client(stream) => {
                let (tx, rx) = stream.split();
                self.reader.lock().await.replace(WsReader::Client(rx));
                self.writer.lock().await.replace(WsWriter::Client(tx));
            }
            WsStream::Server(stream) => {
                let (tx, rx) = stream.split();
                self.reader.lock().await.replace(WsReader::Server(rx));
                self.writer.lock().await.replace(WsWriter::Server(tx));
            }
        }
        RPC::instance()
            .init(|request| async {
                let data = serde_json::to_string(&AppMessage::Request(request)).unwrap();
                MessageManager::instance()
                    .send(Message::Text(data.into()))
                    .await
            })
            .await;
    }

    pub async fn dispose(&self) {
        *self.reader.lock().await = None;
        *self.writer.lock().await = None;
        RPC::instance().dispose().await;
        TaskManager::instance().dispose("MessageManager").await;
    }

    pub async fn send(&self, msg: Message) -> Result<(), AppError> {
        let mut writer_guard = self.writer.lock().await;

        let Some(writer) = &mut *writer_guard else {
            return Err("WebSocket writer is not initialized".into());
        };

        match writer {
            WsWriter::Client(w) => w.send(msg).await?,
            WsWriter::Server(w) => w.send(msg).await?,
        }

        Ok(())
    }

    pub async fn send_event(&self, event: &str, data: Option<Value>) -> Result<(), AppError> {
        let event: Event = Event::new(event, data);
        let data = serde_json::to_string(&AppMessage::Event(event)).unwrap();
        MessageManager::instance()
            .send(Message::Text(data.into()))
            .await
    }

    pub async fn send_stream(
        &self,
        tag: &str,
        bytes: Vec<u8>,
        data: Option<Value>,
    ) -> Result<(), AppError> {
        let stream = encode_stream_frame(&Stream::new(tag, bytes, data));
        MessageManager::instance()
            .send(Message::Binary(stream.into()))
            .await
    }

    pub async fn process_messages(&self) -> Result<(), AppError> {
        if self.reader.lock().await.is_none() {
            return Err("WebSocket reader is not initialized".into());
        }

        loop {
            let next_msg = {
                let mut reader = self.reader.lock().await;
                match reader.as_mut() {
                    None => break,
                    Some(WsReader::Client(reader)) => reader.next().await,
                    Some(WsReader::Server(reader)) => reader.next().await,
                }
            };
            match next_msg {
                None => break,
                Some(Ok(Message::Close(_))) => break,
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(msg)) => {
                    match msg {
                        Message::Text(text) => {
                            let _ = self.on_text(text.to_string()).await;
                        }
                        Message::Binary(bytes) => {
                            let _ = self.on_bytes(bytes.into()).await;
                        }
                        _ => {}
                    };
                }
            }
        }

        Ok(())
    }

    async fn on_bytes(&self, bytes: Vec<u8>) -> Result<(), AppError> {
        let data = decode_stream_frame(&bytes)?;
        MessageHandler::<Stream>::instance().on(data).await
    }

    async fn on_text(&self, text: String) -> Result<(), AppError> {
        let msg = serde_json::from_str::<AppMessage>(&text)?;

        match msg {
            AppMessage::Request(request) => {
                self.run_concurrently(move || {
                    let request = request.clone();
                    async move {
                        MessageHandler::<Request>::instance()
                            .on_request(request)
                            .await?;
                        Ok(())
                    }
                })
                .await
            }
            AppMessage::Response(response) => {
                MessageHandler::<Response>::instance()
                    .on_response(response)
                    .await
            }
            AppMessage::Event(event) => MessageHandler::<Event>::instance().on(event).await,
            _ => Ok(()),
        }
    }

    async fn run_concurrently<F, Fut>(&self, run: F) -> Result<(), AppError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AppError>> + Send + 'static,
    {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => self.semaphore.clone().acquire_owned().await?,
        };

        let task = tokio::spawn(async move {
            let _ = run().await;
            drop(permit);
        });

        TaskManager::instance().add("MessageManager", task).await;
        Ok(())
    }
}

/// Stream 二进制流帧格式（v1），替代旧版「JSON 数字数组」编码（实测膨胀 ~3.9 倍）：
///
/// ```text
/// [0]     magic/version = 0x01（旧 JSON 帧首字节恒为 '{' 0x7B，可无歧义识别回退）
/// [1]     flags：bit0 = 携带 JSON data
/// [2]     tag_len
/// [3..]   tag (UTF-8)
/// [.]     id_len
/// [..]    id (UTF-8)
/// [2B LE] json_len（仅当 flags bit0 置位）
/// [..]    json (UTF-8)
/// [rest]  bytes payload
/// ```
const STREAM_FRAME_VERSION: u8 = 0x01;
const STREAM_FLAG_HAS_JSON: u8 = 0x01;

fn encode_stream_frame(stream: &Stream) -> Vec<u8> {
    let tag = stream.tag.as_bytes();
    let id = stream.id.as_bytes();
    let json = stream
        .data
        .as_ref()
        .and_then(|d| serde_json::to_vec(d).ok());

    let mut buf = Vec::with_capacity(
        3 + tag.len() + 1 + id.len() + json.as_ref().map_or(0, |j| 2 + j.len()) + stream.bytes.len(),
    );
    buf.push(STREAM_FRAME_VERSION);
    buf.push(if json.is_some() { STREAM_FLAG_HAS_JSON } else { 0 });
    buf.push(tag.len() as u8);
    buf.extend_from_slice(tag);
    buf.push(id.len() as u8);
    buf.extend_from_slice(id);
    if let Some(json) = &json {
        buf.extend_from_slice(&(json.len() as u16).to_le_bytes());
        buf.extend_from_slice(json);
    }
    buf.extend_from_slice(&stream.bytes);
    buf
}

fn decode_stream_frame(buf: &[u8]) -> Result<Stream, AppError> {
    // 旧格式兼容：JSON 帧首字节恒为 '{'
    if buf.first() == Some(&b'{') {
        return Ok(serde_json::from_slice::<Stream>(buf)?);
    }
    let err = |msg: &str| -> AppError { format!("无效的 Stream 帧: {msg}").into() };

    let (&version, &flags, &tag_len) = match buf {
        [v, f, t, ..] => (v, f, t),
        _ => return Err(err("帧头不足 3 字节")),
    };
    if version != STREAM_FRAME_VERSION {
        return Err(err(&format!("不支持的版本 {version}")));
    }

    let mut pos = 3usize;
    let tag_end = pos + tag_len as usize;
    let tag = buf
        .get(pos..tag_end)
        .and_then(|b| std::str::from_utf8(b).ok())
        .ok_or_else(|| err("tag 解码失败"))?
        .to_string();
    pos = tag_end;

    let id_len = *buf.get(pos).ok_or_else(|| err("id_len 缺失"))?;
    pos += 1;
    let id_end = pos + id_len as usize;
    let id = buf
        .get(pos..id_end)
        .and_then(|b| std::str::from_utf8(b).ok())
        .ok_or_else(|| err("id 解码失败"))?
        .to_string();
    pos = id_end;

    let data = if flags & STREAM_FLAG_HAS_JSON != 0 {
        let json_len = u16::from_le_bytes(
            buf.get(pos..pos + 2)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| err("json_len 缺失"))?,
        ) as usize;
        pos += 2;
        let json_end = pos + json_len;
        let json = buf.get(pos..json_end).ok_or_else(|| err("json 越界"))?;
        pos = json_end;
        Some(serde_json::from_slice(json)?)
    } else {
        None
    };

    let bytes = buf.get(pos..).unwrap_or(&[]).to_vec();
    Ok(Stream {
        id,
        tag,
        bytes,
        data,
    })
}
