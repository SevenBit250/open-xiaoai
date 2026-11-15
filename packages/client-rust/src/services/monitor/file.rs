use inotify::{EventMask, Inotify, WatchMask};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::base::AppError;

/// poll 超时仅用于生命周期检查：空闲时无任何 stat/read 系统调用（P2-1）
const POLL_TIMEOUT_MS: i32 = 500;
/// 安全兜底轮询间隔：实测 LX06 4.9 内核的 fsnotify 可能整体无声失效
/// （inotifyd 也收不到事件，dmesg 无线索），失效时以此间隔兜底发现文件变化。
/// 正常情况下 inotify 事件即时到达，此路径不触发。
const SAFETY_POLL_INTERVAL: Duration = Duration::from_secs(2);
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// 调试开关：FM_DEBUG=1 打印监控内部事件流
static DEBUG: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(Debug, Serialize, Deserialize)]
pub enum FileMonitorEvent {
    NewFile,
    NewLine(String),
}

/// 文件尾部监控器（inotify 事件驱动，P2-1）。
///
/// 对外行为与旧版轮询实现完全一致：
/// - 文件不存在时零成本等待（watch 父目录创建事件）
/// - 从文件末尾开始 tail，新增行触发 `NewLine`
/// - 文件轮转（rename/recreate 或长度回缩）触发 `NewFile` 并从头重读
pub struct FileMonitor {
    task_holder: Option<JoinHandle<()>>,
    running: Option<Arc<AtomicBool>>,
}

impl Default for FileMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl FileMonitor {
    pub fn new() -> Self {
        Self {
            task_holder: None,
            running: None,
        }
    }

    pub async fn start<F, Fut>(&mut self, file_path: &str, on_update: F)
    where
        F: Fn(FileMonitorEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), AppError>> + Send + 'static,
    {
        if std::env::var("FM_DEBUG").is_ok() {
            DEBUG.store(true, Ordering::Relaxed);
        }
        if let Some(old_running) = self.running.take() {
            old_running.store(false, Ordering::Relaxed);
        }

        let running = Arc::new(AtomicBool::new(true));
        let (tx, mut rx) = mpsc::channel::<FileMonitorEvent>(64);

        let thread_running = running.clone();
        let path = PathBuf::from(file_path);
        let _ = thread::Builder::new()
            .name("file-monitor".to_string())
            .spawn(move || {
                while thread_running.load(Ordering::Relaxed) {
                    if let Err(e) = run_monitor(&path, &tx, &thread_running) {
                        eprintln!("file monitor 异常（{}）: {}", path.display(), e);
                    }
                    if !thread_running.load(Ordering::Relaxed) {
                        break;
                    }
                    thread::sleep(RETRY_DELAY);
                }
            });

        let monitor = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let _ = on_update(event).await;
            }
        });

        if let Some(old_task) = self.task_holder.replace(monitor) {
            old_task.abort();
        }
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

type MonitorResult = Result<(), String>;

fn run_monitor(path: &Path, tx: &mpsc::Sender<FileMonitorEvent>, running: &AtomicBool) -> MonitorResult {
    let mut inotify = Inotify::init().map_err(|e| format!("inotify init 失败: {e}"))?;

    wait_for_file(&mut inotify, path, running)?;

    let file = std::fs::File::open(path).map_err(|e| format!("打开 {} 失败: {e}", path.display()))?;
    let mut reader = BufReader::new(file);
    // 语义与旧版一致：从文件末尾开始，只 tail 新增内容
    let mut position = reader
        .seek(SeekFrom::End(0))
        .map_err(|e| format!("seek 失败: {e}"))?;

    let parent = parent_dir(path);
    let mut watch = FileWatch::add(&mut inotify, path, &parent)?;

    let mut buf = [0u8; 4096];
    let mut last_safety_poll = std::time::Instant::now();
    loop {
        if !running.load(Ordering::Relaxed) {
            return Ok(());
        }
        if !wait_readable(&inotify)? {
            // 超时：生命周期检查 + 安全兜底（inotify 失效时仍能在 2s 内发现变化）
            if last_safety_poll.elapsed() >= SAFETY_POLL_INTERVAL {
                last_safety_poll = std::time::Instant::now();
                if !path.exists() {
                    tx.blocking_send(FileMonitorEvent::NewFile)
                        .map_err(|_| "事件接收端已关闭".to_string())?;
                    wait_for_file(&mut inotify, path, running)?;
                    let file = std::fs::File::open(path)
                        .map_err(|e| format!("重开 {} 失败: {e}", path.display()))?;
                    reader = BufReader::new(file);
                    position = 0;
                    watch = FileWatch::add(&mut inotify, path, &parent)?;
                }
                read_available(&mut reader, &mut position, tx)?;
            }
            continue;
        }
        last_safety_poll = std::time::Instant::now();

        let mut file_touched = false;
        let mut file_gone = false;
        {
            let events = inotify
                .read_events(&mut buf)
                .map_err(|e| format!("read_events 失败: {e}"))?;
            for event in events {
                if event.mask.contains(EventMask::Q_OVERFLOW) {
                    // 事件队列溢出：保守起见做一次全量读取
                    file_touched = true;
                }
                if event.wd == watch.file_wd {
                    if event.mask.intersects(
                        EventMask::MODIFY | EventMask::ATTRIB | EventMask::MOVE_SELF,
                    ) {
                        file_touched = true;
                    }
                    if event.mask.intersects(EventMask::DELETE_SELF | EventMask::IGNORED) {
                        file_gone = true;
                    }
                } else if event.wd == watch.dir_wd {
                    let matches_target = event
                        .name
                        .map(|n| Path::new(n) == path)
                        .unwrap_or(false);
                    if matches_target
                        && event
                            .mask
                            .intersects(EventMask::CREATE | EventMask::MOVED_TO | EventMask::DELETE | EventMask::MOVED_FROM)
                    {
                        // 目标文件被轮转/重建/删除
                        file_touched = true;
                        if event.mask.intersects(EventMask::DELETE | EventMask::MOVED_FROM) {
                            file_gone = true;
                        }
                    }
                }
            }
        }

        if file_gone {
            if DEBUG.load(Ordering::Relaxed) {
                eprintln!("[fm:{}] file_gone -> 等待重建", path.display());
            }
            // 轮转/删除：按旧版语义发 NewFile，等待新文件后从头重读
            tx.blocking_send(FileMonitorEvent::NewFile)
                .map_err(|_| "事件接收端已关闭".to_string())?;
            wait_for_file(&mut inotify, path, running)?;
            let file = std::fs::File::open(path)
                .map_err(|e| format!("重开 {} 失败: {e}", path.display()))?;
            reader = BufReader::new(file);
            position = 0;
            watch = FileWatch::add(&mut inotify, path, &parent)?;
            file_touched = true;
        }

        if file_touched {
            read_available(&mut reader, &mut position, tx)?;
        }
    }
}

struct FileWatch {
    file_wd: inotify::WatchDescriptor,
    dir_wd: inotify::WatchDescriptor,
}

impl FileWatch {
    fn add(inotify: &mut Inotify, path: &Path, parent: &Path) -> Result<Self, String> {
        let file_wd = inotify
            .add_watch(path, WatchMask::MODIFY | WatchMask::ATTRIB | WatchMask::MOVE_SELF | WatchMask::DELETE_SELF)
            .map_err(|e| format!("watch {} 失败: {e}", path.display()))?;
        let dir_wd = inotify
            .add_watch(parent, WatchMask::CREATE | WatchMask::MOVED_TO | WatchMask::MOVED_FROM | WatchMask::DELETE)
            .unwrap_or_else(|_| file_wd.clone()); // 父目录不可 watch 时退化为仅文件 watch
        Ok(Self { file_wd, dir_wd })
    }
}

/// 等待文件出现。优先 watch 父目录的创建事件（零轮询）；父目录不存在时退化为慢轮询。
fn wait_for_file(inotify: &mut Inotify, path: &Path, running: &AtomicBool) -> MonitorResult {
    if path.exists() {
        return Ok(());
    }
    let parent = parent_dir(path);
    if !parent.exists() {
        eprintln!("file monitor: 父目录不存在，慢轮询等待 {}", path.display());
        while running.load(Ordering::Relaxed) && !path.exists() {
            thread::sleep(Duration::from_millis(500));
        }
        return Ok(());
    }

    let _wd = inotify
        .add_watch(&parent, WatchMask::CREATE | WatchMask::MOVED_TO)
        .map_err(|e| format!("watch 父目录失败: {e}"))?;
    let mut buf = [0u8; 1024];
    while running.load(Ordering::Relaxed) {
        if path.exists() {
            return Ok(());
        }
        if !wait_readable(inotify)? {
            continue;
        }
        let events = inotify
            .read_events(&mut buf)
            .map_err(|e| format!("read_events 失败: {e}"))?;
        if DEBUG.load(Ordering::Relaxed) {
            eprintln!("[fm:{} poll] got events", path.display());
        }
        for event in events {
            if event
                .name
                .map(|n| Path::new(n) == path)
                .unwrap_or(false)
                && event.mask.intersects(EventMask::CREATE | EventMask::MOVED_TO)
            {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// poll inotify fd（带超时）。返回 true = 有事件可读，false = 超时。
fn wait_readable(inotify: &Inotify) -> Result<bool, String> {
    let mut pfd = libc::pollfd {
        fd: inotify.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: pfd 是栈上合法的 pollfd 数组
    let n = unsafe { libc::poll(&mut pfd, 1, POLL_TIMEOUT_MS) };
    if n < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::Interrupted {
            return Ok(false);
        }
        return Err(format!("poll 失败: {err}"));
    }
    Ok(n > 0)
}

/// 读取当前所有可读内容（含轮转检测），语义与旧版轮询实现一致。
fn read_available(
    reader: &mut BufReader<std::fs::File>,
    position: &mut u64,
    tx: &mpsc::Sender<FileMonitorEvent>,
) -> MonitorResult {
    let metadata = reader
        .get_ref()
        .metadata()
        .map_err(|e| format!("metadata 失败: {e}"))?;
    if metadata.len() < *position {
        // 文件被截断/轮转：从头重读
        *position = 0;
        reader
            .seek(SeekFrom::Start(0))
            .map_err(|e| format!("seek 失败: {e}"))?;
        tx.blocking_send(FileMonitorEvent::NewFile)
            .map_err(|_| "事件接收端已关闭".to_string())?;
    } else if reader.stream_position().map_err(|e| e.to_string())? != *position {
        reader
            .seek(SeekFrom::Start(*position))
            .map_err(|e| format!("seek 失败: {e}"))?;
    }

    let mut line = String::new();
    loop {
        line.clear();
        let bytes_read = reader
            .read_line(&mut line)
            .map_err(|e| format!("read_line 失败: {e}"))?;
        if bytes_read == 0 {
            break;
        }
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            tx.blocking_send(FileMonitorEvent::NewLine(trimmed.to_string()))
                .map_err(|_| "事件接收端已关闭".to_string())?;
        }
        *position = reader
            .stream_position()
            .map_err(|e| format!("stream_position 失败: {e}"))?;
    }
    Ok(())
}

fn parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}
