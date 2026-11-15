use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AudioConfig {
    pub pcm: String,
    pub channels: u16,
    pub bits_per_sample: u16,
    pub sample_rate: u32,
    pub period_size: u32,
    pub buffer_size: u32,
    /// 音频编解码器：None/"pcm" = 裸 PCM（默认，兼容旧版）；"opus" = Opus 压缩
    #[serde(default)]
    pub codec: Option<String>,
}

pub static AUDIO_CONFIG: LazyLock<AudioConfig> = LazyLock::new(|| AudioConfig {
    pcm: "noop".into(),
    channels: 1,
    bits_per_sample: 16,
    sample_rate: 16000,
    period_size: 160,
    buffer_size: 480,
    codec: None,
});

impl AudioConfig {
    pub fn is_opus(&self) -> bool {
        self.codec.as_deref().map(|c| c.eq_ignore_ascii_case("opus")).unwrap_or(false)
    }
}
