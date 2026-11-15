//! 轻量日志器：支持写入文件并施加大小硬上限（滚动写入：超限后丢弃最旧的一半，
//! 保留最近的日志继续写），未配置文件时回退到标准输出（与旧版行为一致）。
//!
//! 用法：启动时调用 `logger::init`，之后使用 `log_info!` / `log_error!` 宏。

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

/// 默认日志上限：1MB（config.json 的 log_max_bytes 可覆盖）
const DEFAULT_MAX_BYTES: u64 = 1024 * 1024;

struct FileLog {
    file: File,
    path: PathBuf,
    written: u64,
    max_bytes: u64,
}

impl FileLog {
    fn open(path: &str, max_bytes: u64) -> Option<FileLog> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = OpenOptions::new().create(true).append(true).open(path).ok()?;
        let written = file.metadata().ok().map(|m| m.len()).unwrap_or(0);
        Some(FileLog {
            file,
            path: PathBuf::from(path),
            written,
            max_bytes,
        })
    }

    fn write_line(&mut self, line: &str) {
        // 大小硬上限：本次写入会超限则先滚动（丢弃最旧的一半）
        if self.written + line.len() as u64 > self.max_bytes {
            self.roll_oldest();
        }
        if self.file.write_all(line.as_bytes()).is_ok() {
            let _ = self.file.flush();
            self.written += line.len() as u64;
        } else {
            // 文件写入失败（如 /tmp 被清）时尝试重开一次
            if let Some(reopened) = FileLog::open(&self.path.to_string_lossy(), self.max_bytes) {
                *self = reopened;
            }
        }
    }

    /// 滚动：保留最近 max/2 字节（对齐到行首），丢弃更早的内容。
    /// append 模式下 set_len(0) 后 write 会从 0 开始，天然完成重写。
    fn roll_oldest(&mut self) {
        let keep = (self.max_bytes / 2) as usize;
        match std::fs::read(&self.path) {
            Ok(content) if content.len() > keep => {
                let mut start = content.len() - keep;
                // 对齐到下一行行首，避免留下半行开头
                if let Some(nl) = content[start..].iter().position(|&b| b == b'\n') {
                    start += nl + 1;
                }
                let tail = &content[start..];
                if self.file.set_len(0).is_ok() && self.file.write_all(tail).is_ok() {
                    let _ = self.file.flush();
                    self.written = tail.len() as u64;
                    return;
                }
                // 重写失败：退化为清空
                let _ = self.file.set_len(0);
                self.written = 0;
            }
            Ok(_) => {}
            Err(_) => {
                let _ = self.file.set_len(0);
                self.written = 0;
            }
        }
    }
}

static LOGGER: LazyLock<Mutex<Option<FileLog>>> = LazyLock::new(|| Mutex::new(None));

/// 初始化日志器：配置了 log_file 才写文件，否则保持标准输出。
/// 在 main 启动时、其他线程打日志前调用。
pub fn init(log_file: Option<&str>, max_bytes: u64) {
    if let Some(path) = log_file {
        let max = if max_bytes == 0 {
            DEFAULT_MAX_BYTES
        } else {
            max_bytes
        };
        match FileLog::open(path, max) {
            Some(file_log) => {
                println!("📝 日志输出到 {path}（上限 {max} 字节，超限滚动）");
                if let Ok(mut guard) = LOGGER.lock() {
                    *guard = Some(file_log);
                }
            }
            None => eprintln!("⚠️ 无法打开日志文件 {path}，回退到标准输出"),
        }
    }
}

/// UTC 时间戳（无外部依赖）
fn timestamp() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    format!("[UTC {:02}:{:02}:{:02}]", h, m, s)
}

pub fn write_log(level: &str, args: std::fmt::Arguments) {
    let line = format!("{} {} {}\n", timestamp(), level, args);
    if let Ok(mut guard) = LOGGER.lock() {
        if let Some(file_log) = guard.as_mut() {
            file_log.write_line(&line);
            return;
        }
    }
    // 未配置文件：回退标准输出
    if level == "ERROR" {
        eprint!("{line}");
    } else {
        print!("{line}");
    }
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::utils::logger::write_log("INFO", format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::utils::logger::write_log("ERROR", format_args!($($arg)*))
    };
}
