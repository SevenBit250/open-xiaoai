//! Opus 编解码封装（P1-2）。
//!
//! - 编码器内部缓冲 PCM，按 Opus 合法帧长（20ms）切片后编码，输入长度任意
//! - 解码器逐包解码，输出 PCM i16
//! - 语音场景（Voip profile），默认 24kbps 码率（16k 单声道约 3KB/s，对比 PCM 32KB/s）

use audiopus::coder::Decoder as OpusDecoderRaw;
use audiopus::coder::Encoder as OpusEncoderRaw;
use audiopus::{Application, Bitrate, Channels, SampleRate};

use crate::base::AppError;

/// Opus 单帧最大编码长度（RFC 6716: ~1275 字节）
const MAX_PACKET: usize = 1400;

fn to_sample_rate(rate: u32) -> Result<SampleRate, AppError> {
    match rate {
        8000 => Ok(SampleRate::Hz8000),
        12000 => Ok(SampleRate::Hz12000),
        16000 => Ok(SampleRate::Hz16000),
        24000 => Ok(SampleRate::Hz24000),
        48000 => Ok(SampleRate::Hz48000),
        _ => Err(format!("Opus 不支持的采样率: {rate}").into()),
    }
}

fn to_channels(channels: u16) -> Result<Channels, AppError> {
    match channels {
        1 => Ok(Channels::Mono),
        2 => Ok(Channels::Stereo),
        _ => Err(format!("Opus 不支持的声道数: {channels}").into()),
    }
}

pub struct OpusEncoder {
    inner: OpusEncoderRaw,
    frame_size: usize,
    pcm_buf: Vec<i16>,
}

impl OpusEncoder {
    pub fn new(sample_rate: u32, channels: u16) -> Result<Self, AppError> {
        let rate = to_sample_rate(sample_rate)?;
        let ch = to_channels(channels)?;
        let mut inner = OpusEncoderRaw::new(rate, ch, Application::Voip)
            .map_err(|e| format!("创建 Opus 编码器失败: {e:?}"))?;
        // 语音 24kbps：质量与带宽的平衡点（16k 单声道下 ~3KB/s）
        inner
            .set_bitrate(Bitrate::BitsPerSecond(24_000))
            .map_err(|e| format!("设置 Opus 码率失败: {e:?}"))?;
        // 20ms 帧（16k 单声道 = 320 样本），语音对话标准帧长
        let frame_size = sample_rate as usize / 50 * channels as usize;
        Ok(Self {
            inner,
            frame_size,
            pcm_buf: Vec::with_capacity(frame_size * 2),
        })
    }

    /// 输入任意长度 PCM（i16 交错），返回 0..n 个完整 Opus 帧
    pub fn encode(&mut self, pcm: &[i16]) -> Result<Vec<Vec<u8>>, AppError> {
        self.pcm_buf.extend_from_slice(pcm);
        let mut packets = Vec::new();
        while self.pcm_buf.len() >= self.frame_size {
            let (frame, rest) = self.pcm_buf.split_at(self.frame_size);
            let mut packet = vec![0u8; MAX_PACKET];
            let encoded = self
                .inner
                .encode(frame, &mut packet)
                .map_err(|e| format!("Opus 编码失败: {e:?}"))?;
            packets.push(packet[..encoded].to_vec());
            self.pcm_buf = rest.to_vec();
        }
        Ok(packets)
    }
}

pub struct OpusDecoder {
    inner: OpusDecoderRaw,
    frame_size: usize,
    channels: usize,
}

impl OpusDecoder {
    pub fn new(sample_rate: u32, channels: u16) -> Result<Self, AppError> {
        let inner = OpusDecoderRaw::new(to_sample_rate(sample_rate)?, to_channels(channels)?)
            .map_err(|e| format!("创建 Opus 解码器失败: {e:?}"))?;
        Ok(Self {
            inner,
            frame_size: sample_rate as usize / 50 * channels as usize,
            channels: channels as usize,
        })
    }

    /// 解码单个 Opus 帧为 PCM（i16 交错）；空包用于 PLC（丢包隐藏）
    pub fn decode(&mut self, packet: &[u8]) -> Result<Vec<i16>, AppError> {
        let mut pcm = vec![0i16; self.frame_size];
        // 返回值为每声道样本数
        let per_channel = if packet.is_empty() {
            self.inner
                .decode(None::<&[u8]>, &mut pcm, false)
                .map_err(|e| format!("Opus PLC 失败: {e:?}"))?
        } else {
            self.inner
                .decode(Some(packet), &mut pcm, false)
                .map_err(|e| format!("Opus 解码失败: {e:?}"))?
        };
        pcm.truncate(per_channel * self.channels);
        Ok(pcm)
    }
}

/// PCM 字节（LE, S16）<-> i16 样本互转
pub fn pcm_bytes_to_samples(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect()
}

pub fn samples_to_pcm_bytes(samples: &[i16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    for s in samples {
        bytes.extend_from_slice(&s.to_le_bytes());
    }
    bytes
}
