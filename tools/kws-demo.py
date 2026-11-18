"""KWS 唤醒演示 Server：「豆包豆包」唤醒 → 音箱应答

流程：
  音箱 client 连接后 → server 下发 start_recording（PCM 16k 单声道持续推流）
  → sherpa-onnx KeywordSpotter 实时检测唤醒词
  → 命中「豆包豆包」→ RPC run_shell 让音箱 TTS 播报应答

用法: python tools/kws-demo.py [端口] [模型目录]
"""

import asyncio
import json
import struct
import sys
import time
from pathlib import Path

import numpy as np
import websockets

import sherpa_onnx

PORT = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 4399
MODEL_DIR = Path(
    sys.argv[2]
    if len(sys.argv) > 2
    else r"C:\Users\25293\Desktop\aiworkspace\kws-model\sherpa-onnx-kws-zipformer-wenetspeech-3.3M-2024-01-01"
)
# 关键词文件：沿用仓库 examples/kws/keywords.txt（含 豆包豆包），也可自行改写
KEYWORDS_FILE = Path(__file__).parent.parent / "examples" / "kws" / "keywords.txt"

REPLY = "哎，我在"  # 唤醒后音箱的 TTS 应答

SAMPLE_RATE = 16000
AUDIO_CONFIG = {
    "pcm": "noop",
    "channels": 1,
    "bits_per_sample": 16,
    "sample_rate": SAMPLE_RATE,
    "period_size": 160,
    "buffer_size": 480,
}


def ts():
    return time.strftime("%H:%M:%S")


def make_kws():
    kws = sherpa_onnx.KeywordSpotter(
        tokens=str(MODEL_DIR / "tokens.txt"),
        encoder=str(MODEL_DIR / "encoder-epoch-12-avg-2-chunk-16-left-64.onnx"),
        decoder=str(MODEL_DIR / "decoder-epoch-12-avg-2-chunk-16-left-64.onnx"),
        joiner=str(MODEL_DIR / "joiner-epoch-12-avg-2-chunk-16-left-64.onnx"),
        keywords_file=str(KEYWORDS_FILE),
        num_trailing_blanks=1,
        keywords_score=2.5,
        keywords_threshold=0.15,
    )
    return kws


async def handle(ws):
    print(f"[{ts()}] ✅ 音箱已连接: {ws.remote_address}", flush=True)
    kws = make_kws()
    stream = kws.create_stream()

    total_bytes = 0
    hits = 0
    last_feed = time.time()

    async def send_request(cmd, payload=None):
        await ws.send(json.dumps({
            "Request": {"id": f"{cmd}-{time.monotonic_ns()}", "command": cmd, "payload": payload}
        }))

    async def speak(text):
        """让音箱用官方 TTS 说话（走 run_shell）"""
        await send_request("run_shell", f"/usr/sbin/tts_play.sh '{text}'")

    async def stats_task():
        while True:
            await asyncio.sleep(10)
            print(
                f"[{ts()}] 📊 已接收 {total_bytes/1024:.0f}KB 音频，唤醒 {hits} 次",
                flush=True,
            )

    tasks = [asyncio.create_task(stats_task())]
    try:
        # 开启持续录音
        await send_request("start_recording", AUDIO_CONFIG)
        print(f"[{ts()}] 🎤 已开启持续录音，请对音箱说「豆包豆包」", flush=True)

        async for msg in ws:
            if not isinstance(msg, bytes):
                # Text 帧：事件/响应
                obj = json.loads(msg)
                if "Event" in obj:
                    ev = obj["Event"]
                    print(f"[{ts()}] 📨 event {ev.get('event')}", flush=True)
                continue

            # 二进制流帧解码（v1 格式）
            buf = msg
            if buf[:1] == b"{":
                continue  # 忽略旧格式
            tag_len = buf[2]
            pos = 3 + tag_len
            id_len = buf[pos]
            pos += 1 + id_len
            flags = buf[1]
            if flags & 0x01:
                (json_len,) = struct.unpack_from("<H", buf, pos)
                pos += 2 + json_len
            payload = buf[pos:]
            if payload:
                total_bytes += len(payload)
                samples = np.frombuffer(payload, dtype="<i2").astype(np.float32) / 32768.0
                stream.accept_waveform(SAMPLE_RATE, samples)
                while kws.is_ready(stream):
                    kws.decode_stream(stream)
                result = kws.get_result(stream)
                if result:
                    hits += 1
                    keyword = result.split("@")[0]
                    print(f"[{ts()}] 🎯 唤醒命中: {keyword}！（累计 {hits} 次）", flush=True)
                    await speak(REPLY)
                    stream = kws.create_stream()  # 重置检测流
                last_feed = time.time()
    except websockets.ConnectionClosed as e:
        print(f"[{ts()}] ❌ 连接断开: {e}", flush=True)
    finally:
        for t in tasks:
            t.cancel()


async def main():
    async with websockets.serve(handle, "0.0.0.0", PORT, max_size=16 * 1024 * 1024):
        print(f"🚀 KWS 演示 server: ws://0.0.0.0:{PORT}", flush=True)
        print(f"   模型: {MODEL_DIR}", flush=True)
        print(f"   关键词: {KEYWORDS_FILE}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
