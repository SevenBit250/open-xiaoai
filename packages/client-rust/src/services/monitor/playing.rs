use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use ubus::client::UbusClient;
use ubus::unix::UnixUbusTransport;
use ubus::{json::blobmsg_to_json, BlobMsgTable};

use crate::base::AppError;

/// ubus mediaplayer.player_get_play_status 返回的 info.status 语义（实测 LX06 1.94.13）：
/// 0 = 空闲；1 = 播放中；2 = 暂停（旧文档值）；3 = 暂停后驻留态。
/// 与旧版 mphelper mute_stat 的 stdout 数值完全一致（mphelper 即透传该值）。
const UBUS_SOCKET: &str = "/var/run/ubus.sock";
const UBUS_OBJECT: &str = "mediaplayer";
const UBUS_METHOD: &str = "player_get_play_status";

/// 自适应轮询间隔：播放态需要更快的停止检测（打断场景），空闲态放松
const INTERVAL_PLAYING: Duration = Duration::from_millis(100);
const INTERVAL_IDLE: Duration = Duration::from_millis(200);
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum PlayingMonitorEvent {
    Playing,
    Paused,
    Idle,
}

pub struct PlayingMonitor {
    task_holder: Option<JoinHandle<()>>,
    running: Option<Arc<AtomicBool>>,
}

impl Default for PlayingMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl PlayingMonitor {
    pub fn new() -> Self {
        Self {
            task_holder: None,
            running: None,
        }
    }

    pub async fn start<F, Fut>(&mut self, on_update: F)
    where
        F: Fn(PlayingMonitorEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AppError>> + Send + 'static,
    {
        // 先停掉旧的监控（重复 start 时）
        if let Some(old_running) = self.running.take() {
            old_running.store(false, Ordering::Relaxed);
        }

        let running = Arc::new(AtomicBool::new(true));

        // 事件通道：阻塞式监控线程 -> 异步转发任务
        let (tx, mut rx) = mpsc::channel::<PlayingMonitorEvent>(16);

        // 监控策略：默认原生 ubus socket（零进程 spawn）；
        // 设置环境变量 PLAYING_MONITOR=shell 回退到旧的 mphelper 轮询方案
        let use_shell = std::env::var("PLAYING_MONITOR")
            .map(|v| v.eq_ignore_ascii_case("shell"))
            .unwrap_or(false);

        let thread_running = running.clone();
        let _ = thread::Builder::new()
            .name("playing-monitor".to_string())
            .spawn(move || {
                while thread_running.load(Ordering::Relaxed) {
                    let result = if use_shell {
                        run_shell_monitor(&tx, &thread_running)
                    } else {
                        run_ubus_monitor(&tx, &thread_running)
                    };
                    // 出错不再静默退出（修复 P0-2）：打日志后重连
                    match result {
                        Err(e) => eprintln!(
                            "playing monitor 异常，{}s 后重试: {}",
                            RECONNECT_DELAY.as_secs(),
                            e
                        ),
                        // 正常返回只可能是收到停止信号
                        Ok(()) => break,
                    }
                    thread::sleep(RECONNECT_DELAY);
                }
            });

        // 异步侧：转发事件给回调
        let monitor = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let _ = on_update(event).await;
            }
        });

        if let Some(old_task) = self.task_holder.replace(monitor) {
            println!("Aborting old playing monitor task");
            old_task.abort();
        };
        self.running = Some(running);
    }

    pub async fn stop(&mut self) {
        if let Some(running) = self.running.take() {
            running.store(false, Ordering::Relaxed);
        }
        if let Some(handle) = self.task_holder.take() {
            handle.abort();
        }
    }
}

/// 原生 ubus socket 轮询（P0-1 正解）：长连接 + invoke，零进程 spawn。
/// 实测单次 invoke ~0.95ms，对比旧路径（sh + mphelper）~31ms。
fn run_ubus_monitor(
    tx: &mpsc::Sender<PlayingMonitorEvent>,
    running: &AtomicBool,
) -> Result<(), String> {
    let transport = UnixUbusTransport::connect(UBUS_SOCKET)
        .map_err(|e| format!("连接 ubus socket 失败: {e}"))?;
    let mut client =
        UbusClient::connect(transport).map_err(|e| format!("ubus HELLO 失败: {e}"))?;
    let obj = client
        .lookup(UBUS_OBJECT)
        .map_err(|e| format!("lookup {UBUS_OBJECT} 失败: {e}"))?;

    let mut last: Option<PlayingMonitorEvent> = None;
    while running.load(Ordering::Relaxed) {
        let reply = client
            .invoke(obj, UBUS_METHOD, &BlobMsgTable::empty())
            .map_err(|e| format!("invoke {UBUS_METHOD} 失败: {e}"))?;
        let event = match parse_play_status(&reply) {
            Some(1) => PlayingMonitorEvent::Playing,
            Some(2) => PlayingMonitorEvent::Paused,
            _ => PlayingMonitorEvent::Idle,
        };
        let interval = if event == PlayingMonitorEvent::Playing {
            INTERVAL_PLAYING
        } else {
            INTERVAL_IDLE
        };
        if last.as_ref() != Some(&event) {
            // 接收端（异步转发任务）已关闭则退出
            tx.blocking_send(event.clone())
                .map_err(|_| "事件接收端已关闭".to_string())?;
            last = Some(event);
        }
        thread::sleep(interval);
    }
    Ok(())
}

/// 解析 player_get_play_status 的 blobmsg 回复：
/// {"code":0,"info":"{ \"status\": 1, ... }"} -> Some(1)
fn parse_play_status(reply: &[u8]) -> Option<i64> {
    let value = blobmsg_to_json(reply).ok()?;
    let info = value.get("info")?.as_str()?;
    let info = serde_json::from_str::<serde_json::Value>(info).ok()?;
    info.get("status")?.as_i64()
}

/// 旧方案回退路径：mphelper 轮询（保留给 ubus 布局变化的新固件应急）。
/// 间隔放宽到 200/500ms，且单次失败不再导致监控任务死亡。
fn run_shell_monitor(
    tx: &mpsc::Sender<PlayingMonitorEvent>,
    running: &AtomicBool,
) -> Result<(), String> {
    use std::process::Command;

    let mut last: Option<PlayingMonitorEvent> = None;
    let mut failures = 0u32;
    while running.load(Ordering::Relaxed) {
        let output = Command::new("/bin/sh")
            .arg("-c")
            .arg("mphelper mute_stat 2>/dev/null")
            .output();
        let event = match output {
            Ok(out) if out.status.success() => {
                failures = 0;
                let stdout = String::from_utf8_lossy(&out.stdout);
                match stdout.trim() {
                    "1" => PlayingMonitorEvent::Playing,
                    "2" => PlayingMonitorEvent::Paused,
                    _ => PlayingMonitorEvent::Idle,
                }
            }
            // 单次失败沿用上次状态继续轮询（修复 P0-2 静默死亡）
            Ok(_) | Err(_) => {
                failures += 1;
                if failures >= 10 {
                    return Err("mphelper 连续失败 10 次".to_string());
                }
                last.clone().unwrap_or(PlayingMonitorEvent::Idle)
            }
        };
        let interval = if event == PlayingMonitorEvent::Playing {
            Duration::from_millis(200)
        } else {
            Duration::from_millis(500)
        };
        if last.as_ref() != Some(&event) {
            tx.blocking_send(event.clone())
                .map_err(|_| "事件接收端已关闭".to_string())?;
            last = Some(event);
        }
        thread::sleep(interval);
    }
    Ok(())
}
