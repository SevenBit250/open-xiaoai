use serde::Deserialize;
use std::path::PathBuf;

/// Client 配置文件（默认 `/data/open-xiaoai/config.json`，可用环境变量 CONFIG_PATH 覆盖）。
///
/// ```json
/// {
///   "server": "ws://192.168.31.227:4399",
///   "log_file": "/tmp/client.log",
///   "log_max_bytes": 1048576
/// }
/// ```
///
/// 优先级：命令行参数 > config.json > 启动报错。
/// `log_file` 未配置时日志走 stdout/stderr（与旧版行为一致）。
#[derive(Debug, Default, Deserialize)]
pub struct AppConfig {
    /// WebSocket server 地址
    pub server: Option<String>,
    /// 日志文件路径；None = 输出到标准输出
    pub log_file: Option<String>,
    /// 日志文件大小硬上限（字节），超过后滚动写入（丢弃最旧的一半）；默认 1MB
    #[serde(default = "default_log_max_bytes")]
    pub log_max_bytes: u64,
}

fn default_log_max_bytes() -> u64 {
    1024 * 1024
}

pub const DEFAULT_CONFIG_PATH: &str = "/data/open-xiaoai/config.json";

impl AppConfig {
    /// 加载配置文件；文件不存在时返回默认值（不报错，保持无配置可用的旧行为）
    pub fn load() -> AppConfig {
        let path = std::env::var("CONFIG_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(DEFAULT_CONFIG_PATH));
        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_json::from_str::<AppConfig>(&content) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("⚠️ 配置文件解析失败（{}），使用默认配置: {}", path.display(), e);
                    AppConfig::default()
                }
            },
            Err(_) => AppConfig::default(),
        }
    }
}
