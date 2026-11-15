"""Open-XiaoAI 测试 Server

用途：验证 client 协议、观察事件流、测试音频流（P1-1 二进制帧 + P1-2 Opus 编解码）。

协议（与 packages/client-rust/src/services/connect/data.rs + message.rs 对齐）：
  - Text  帧: {"Event"|"Request"|"Response": {...}}
  - Binary帧（v1 二进制流帧，兼容旧 JSON 帧——首字节 '{' 即旧格式）:
      [0]=0x01 版本 [1]=flags(bit0=含json) [2]=tag_len [tag] [id_len] [id]
      ([2B LE json_len][json]) [rest]=payload

用法:
  python tools/test-server.py [端口]            # 仅观察
  python tools/test-server.py [端口] --auto     # 自动: PCM录音4s → Opus录音4s → PCM播放 → Opus播放(有声)
"""

import asyncio
import json
import math
import struct
import sys
import time

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 4399
AUTO = "--auto" in sys.argv

BASE_AUDIO_CONFIG = {
    "pcm": "noop",
    "channels": 1,
    "bits_per_sample": 16,
    "sample_rate": 16000,
    "period_size": 160,
    "buffer_size": 480,
}


def audio_config(codec=None):
    cfg = dict(BASE_AUDIO_CONFIG)
    if codec:
        cfg["codec"] = codec
    return cfg


def ts():
    return time.strftime("%H:%M:%S")


def decode_stream_frame(buf: bytes):
    """返回 (stream_dict, payload_len, wire_len)；兼容旧 JSON 帧。"""
    if buf[:1] == b"{":
        obj = json.loads(buf)
        return obj, len(obj.get("bytes", [])), len(buf)
    version, flags, tag_len = buf[0], buf[1], buf[2]
    pos = 3
    tag = buf[pos:pos + tag_len].decode()
    pos += tag_len
    id_len = buf[pos]
    pos += 1
    sid = buf[pos:pos + id_len].decode()
    pos += id_len
    data = None
    if flags & 0x01:
        (json_len,) = struct.unpack_from("<H", buf, pos)
        pos += 2
        data = json.loads(buf[pos:pos + json_len] or b"null")
        pos += json_len
    payload = buf[pos:]
    return {"id": sid, "tag": tag, "bytes": payload, "data": data}, len(payload), len(buf)


def encode_stream_frame(tag: str, payload: bytes, data=None) -> bytes:
    tag_b = tag.encode()
    json_b = json.dumps(data).encode() if data is not None else None
    buf = bytearray([0x01, 0x01 if json_b else 0x00, len(tag_b)])
    buf += tag_b
    id_b = f"srv-{time.monotonic_ns()}".encode()
    buf.append(len(id_b))
    buf += id_b
    if json_b:
        buf += struct.pack("<H", len(json_b)) + json_b
    buf += payload
    return bytes(buf)


def sine_pcm(seconds=1.5, freq=660.0, rate=16000, volume=0.3) -> bytes:
    """生成 16k/16bit/单声道正弦波（可听提示音）。"""
    n = int(seconds * rate)
    return b"".join(
        struct.pack("<h", int(volume * 32767 * math.sin(2 * math.pi * freq * i / rate)))
        for i in range(n)
    )


# ---------------- Opus 工具（基于 PyAV 的 libopus） ----------------

import av
import av.audio.frame
import numpy as np


class OpusFrameCodec:
    """libopus 帧级编解码（20ms 帧，与 client 端 codec.rs 对齐）。"""

    FRAME_SAMPLES = 320  # 16k / 50

    def __init__(self):
        self.enc = av.CodecContext.create("libopus", "w")
        self.enc.sample_rate = 16000
        self.enc.format = "s16"
        self.enc.layout = "mono"
        self.enc.open()
        self.dec = av.CodecContext.create("libopus", "r")
        self.dec.sample_rate = 16000
        self.dec.format = "s16"
        self.dec.layout = "mono"
        self.dec.open()

    def encode_pcm_bytes(self, pcm: bytes):
        """PCM(s16le mono 16k) -> opus 帧列表（每帧 20ms）。"""
        frames = []
        for i in range(0, len(pcm), self.FRAME_SAMPLES * 2):
            chunk = pcm[i:i + self.FRAME_SAMPLES * 2]
            samples = np.frombuffer(chunk.ljust(self.FRAME_SAMPLES * 2, b"\x00"), dtype="<i2")
            af = av.AudioFrame.from_ndarray(
                samples.reshape(1, -1), format="s16", layout="mono"
            )
            af.sample_rate = 16000
            for pkt in self.enc.encode(af):
                frames.append(bytes(pkt))
        return frames

    def decode_frame(self, opus_frame: bytes) -> int:
        """解码单帧，返回样本数（验证用）。"""
        pkt = av.Packet(opus_frame)
        out = self.dec.decode(pkt)
        frames = out if isinstance(out, list) else [out]
        return sum(f.samples for f in frames)


class Stats:
    def __init__(self):
        self.chunks = 0
        self.payload = 0
        self.wire = 0
        self.legacy_equiv = 0
        self.samples = 0

    def add(self, payload_len: int, wire_len: int, payload: bytes):
        self.chunks += 1
        self.payload += payload_len
        self.wire += wire_len
        if self.samples < 20 and payload_len:
            self.legacy_equiv += len(json.dumps(list(payload), separators=(",", ":"))) + 48
            self.samples += 1

    def report(self, name: str):
        if not self.chunks:
            print(f"📊 {name}: 无数据", flush=True)
            return
        ratio = self.wire / self.payload if self.payload else 0
        legacy_ratio = self.legacy_equiv / max(1, self.payload * self.samples // self.chunks)
        print(
            f"📊 {name}: {self.chunks} chunks | payload {self.payload/1024:.1f}KB "
            f"| 线上 {self.wire/1024:.1f}KB (x{ratio:.3f}) "
            f"| 旧JSON编码估算 x{legacy_ratio:.2f}",
            flush=True,
        )


class RecordSession:
    """一次录音会话的流收集（帧列表保留用于 opus 解码验证）。"""

    def __init__(self):
        self.stats = Stats()
        self.frames = []

    def add(self, payload_len, wire_len, payload):
        self.stats.add(payload_len, wire_len, payload)
        if len(self.frames) < 50000:
            self.frames.append(bytes(payload))


async def handle(ws):
    print(f"[{ts()}] ✅ client 已连接: {ws.remote_address}", flush=True)
    sessions = {}  # tag -> RecordSession

    async def send_request(cmd, payload=None):
        await ws.send(json.dumps({
            "Request": {"id": f"{cmd}-{time.monotonic_ns()}", "command": cmd, "payload": payload}
        }))

    async def record_for(name: str, codec_cfg, seconds: float):
        print(f"[{ts()}] 🎤 录音测试 [{name}] {seconds}s ...", flush=True)
        session = RecordSession()
        sessions["record"] = session
        await send_request("start_recording", codec_cfg)
        await asyncio.sleep(seconds)
        await send_request("stop_recording")
        await asyncio.sleep(0.5)
        session.stats.report(f"[{name}] 上行 record")
        return session

    async def play_pcm(pcm: bytes, codec_cfg, name: str):
        print(f"[{ts()}] 🔊 播放测试 [{name}]（音箱出声，突发推送）...", flush=True)
        await send_request("start_play", codec_cfg)
        chunks = []
        if codec_cfg.get("codec") == "opus":
            op = OpusFrameCodec()
            chunks = op.encode_pcm_bytes(pcm)
        else:
            for i in range(0, len(pcm), 960):
                chunks.append(pcm[i:i + 960])
        # 突发推送（快于实时）：模拟真实 TTS 服务端行为；
        # Windows 计时器粒度 ~15ms 无法做 1.0x 精细节拍，靠客户端缓冲消抖
        for c in chunks:
            await ws.send(encode_stream_frame("play", c))
            await asyncio.sleep(0)
        await asyncio.sleep(0.5)
        await send_request("stop_play")
        print(f"[{ts()}] 🔊 [{name}] 完成（{len(chunks)} 块）", flush=True)

    async def auto_test():
        try:
            await asyncio.sleep(2)
            # 1. PCM 录音基线
            await record_for("PCM", audio_config(), 4)
            # 2. Opus 录音
            opus_session = await record_for("Opus", audio_config("opus"), 4)
            # 3. 验证 opus 帧可解码（前 50 帧）
            try:
                op = OpusFrameCodec()
                n = 0
                for f in opus_session.frames[:50]:
                    if f:
                        op.decode_frame(f)
                        n += 1
                print(f"[{ts()}] 🧪 Opus 帧解码验证: {n}/{min(50, len(opus_session.frames))} 帧可解码 ✓", flush=True)
            except Exception as e:
                print(f"[{ts()}] ❌ Opus 帧解码验证失败: {e}", flush=True)
            # 4. 播放（1.0x 实时节拍，检验 P2-3 预缓冲）
            await play_pcm(sine_pcm(2.0), audio_config(), "PCM 660Hz")
            await asyncio.sleep(1)
            await play_pcm(sine_pcm(2.0), audio_config("opus"), "Opus 660Hz")
        except Exception as e:
            print(f"[{ts()}] auto_test 异常: {e}", flush=True)

    tasks = []
    try:
        if AUTO:
            tasks.append(asyncio.create_task(auto_test()))
        async for msg in ws:
            if isinstance(msg, bytes):
                stream, payload_len, wire_len = decode_stream_frame(msg)
                tag = stream.get("tag", "?")
                if tag == "record":
                    sessions.setdefault("record", RecordSession()).add(
                        payload_len, wire_len, stream.get("bytes", b"")
                    )
                continue

            obj = json.loads(msg)
            if "Event" in obj:
                ev = obj["Event"]
                name = ev.get("event")
                data = json.dumps(ev.get("data"), ensure_ascii=False)
                if len(data) > 160:
                    data = data[:160] + "..."
                print(f"[{ts()}] 📨 event {name}: {data}", flush=True)
            elif "Response" in obj:
                r = obj["Response"]
                print(f"[{ts()}] 📨 response {r.get('id')}: code={r.get('code')} "
                      f"msg={r.get('msg')}", flush=True)
            elif "Request" in obj:
                print(f"[{ts()}] 📨 client request {obj['Request'].get('command')}", flush=True)
    except websockets.ConnectionClosed as e:
        print(f"[{ts()}] ❌ 连接断开: {e}", flush=True)
    finally:
        for t in tasks:
            t.cancel()


async def main():
    async with websockets.serve(handle, "0.0.0.0", PORT, max_size=64 * 1024 * 1024):
        mode = " [AUTO: PCM/Opus 录音 + 播放]" if AUTO else ""
        print(f"🚀 测试 server: ws://0.0.0.0:{PORT}{mode}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
