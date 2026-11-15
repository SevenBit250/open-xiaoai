use std::process::Stdio;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::base::AppError;

use super::codec;
use super::config::{AudioConfig, AUDIO_CONFIG};

/// P2-3: aplay ALSA 缓冲下限（帧数）。太小的缓冲（如默认 480 帧=30ms）在
/// 网络推流抖动下必然 underrun，这里保证至少 150ms 的 ALSA 侧缓冲，
/// 配合管道自身缓冲（64KB≈2s@32KB/s）吸收网络突发。
const MIN_APLAY_BUFFER_FRAMES: u32 = 2400;

pub struct AudioPlayer {
    aplay_thread: Arc<Mutex<Option<Child>>>,
    write_thread: Arc<Mutex<Option<ChildStdin>>>,
    sender: Arc<Mutex<Option<mpsc::Sender<Vec<u8>>>>>,
    player_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    decoder: Arc<Mutex<Option<codec::OpusDecoder>>>,
}

static INSTANCE: LazyLock<AudioPlayer> = LazyLock::new(AudioPlayer::new);

impl AudioPlayer {
    fn new() -> Self {
        Self {
            aplay_thread: Arc::new(Mutex::new(None)),
            write_thread: Arc::new(Mutex::new(None)),
            sender: Arc::new(Mutex::new(None)),
            player_task: Arc::new(Mutex::new(None)),
            decoder: Arc::new(Mutex::new(None)),
        }
    }

    pub fn instance() -> &'static Self {
        &INSTANCE
    }

    pub async fn stop(&self) -> Result<(), AppError> {
        let mut sender_guard = self.sender.lock().await;
        if let Some(sender) = sender_guard.take() {
            drop(sender);
        }

        if let Some(task) = self.player_task.lock().await.take() {
            task.abort();
        }

        if let Some(mut write_thread) = self.write_thread.lock().await.take() {
            let _ = timeout(Duration::from_millis(100), write_thread.shutdown()).await;
            drop(write_thread);
        }

        if let Some(mut aplay_thread) = self.aplay_thread.lock().await.take() {
            let _ = aplay_thread.kill().await;
        }

        *self.decoder.lock().await = None;
        Ok(())
    }

    pub async fn start(&self, config: Option<AudioConfig>) -> Result<(), AppError> {
        let is_started = self.sender.lock().await.is_some();
        if is_started {
            self.stop().await?;
        }

        let config = config.unwrap_or_else(|| (*AUDIO_CONFIG).clone());

        // P1-2: 下行流为 Opus 帧时在播放前解码
        let mut decoder_guard = self.decoder.lock().await;
        *decoder_guard = if config.is_opus() {
            Some(codec::OpusDecoder::new(
                config.sample_rate,
                config.channels,
            )?)
        } else {
            None
        };
        drop(decoder_guard);

        // P2-3: ALSA 缓冲不低于 150ms
        let alsa_buffer = config.buffer_size.max(MIN_APLAY_BUFFER_FRAMES);

        let mut aplay_thread = Command::new("aplay")
            .args([
                "--quiet",
                "-t",
                "raw",
                "-f",
                &format!("S{}_LE", config.bits_per_sample),
                "-r",
                &config.sample_rate.to_string(),
                "-c",
                &config.channels.to_string(),
                "--buffer-size",
                &alsa_buffer.to_string(),
                "--period-size",
                &config.period_size.max(160).to_string(),
                "-",
            ])
            .stdin(Stdio::piped())
            .spawn()?;

        let stdin = aplay_thread.stdin.take().unwrap();
        self.aplay_thread.lock().await.replace(aplay_thread);
        self.write_thread.lock().await.replace(stdin);

        // P2-3: 扩大队列容量，允许服务端突发推流（300 块 ≈ 9s@30ms 块）
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(300);

        let write_thread_clone = self.write_thread.clone();
        let player_task = tokio::spawn(async move {
            while let Some(bytes) = rx.recv().await {
                let mut write_guard = write_thread_clone.lock().await;
                if let Some(write_thread) = write_guard.as_mut() {
                    let _ = timeout(Duration::from_millis(100), write_thread.write_all(&bytes)).await;
                } else {
                    break;
                }
            }
        });

        self.player_task.lock().await.replace(player_task);
        self.sender.lock().await.replace(tx);

        Ok(())
    }

    pub async fn play(&self, bytes: Vec<u8>) -> Result<(), AppError> {
        // Opus 模式：先解码为 PCM 再入队
        let pcm = {
            let mut decoder_guard = self.decoder.lock().await;
            match decoder_guard.as_mut() {
                Some(decoder) => codec::samples_to_pcm_bytes(&decoder.decode(&bytes)?),
                None => bytes,
            }
        };

        let sender_guard = self.sender.lock().await;
        if let Some(sender) = sender_guard.as_ref() {
            sender.send(pcm).await?;
        }

        Ok(())
    }
}
