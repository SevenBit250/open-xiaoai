"""流式 ASR 唤醒演示 Server：实时转写 + 文本匹配「豆包豆包」

对比 KWS 微型模型（拼音级匹配），此方案用完整流式 ASR 转写每个字再做文本匹配：
- 识别率数量级提升，任意唤醒词免调拼音
- 实时打印转写文本（可视化识别质量）
- 唤醒后音箱 TTS 应答

用法: python tools/asr-wake-demo.py [端口] [模型目录]
"""

import asyncio
import json
import re
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
    else r"C:\Users\25293\Desktop\aiworkspace\kws-model\sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20"
)

WAKE_WORDS = ["豆包豆包"]  # 文本匹配，可加任意词
REPLY = "哎，我在"

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


def normalize(text: str) -> str:
    return re.sub(r"[^a-z\u4e00-\u9fff]", "", text.lower())


def wake_regex(word: str) -> re.Pattern:
    """抗流式识别字符重复伪影：每个字允许连续出现 1-2 次。
    例：「豆包豆包」→ 豆{1,2}包{1,2}豆{1,2}包{1,2}"""
    pattern = "".join(
        f"{re.escape(ch)}{{1,2}}" if "\u4e00" <= ch <= "\u9fff" else re.escape(ch)
        for ch in normalize(word)
    )
    return re.compile(pattern)


WAKE_PATTERNS = [wake_regex(w) for w in WAKE_WORDS]
# 常见误读/伪影变体（实测「豆包豆包」常被转写为「豆豆包包」）
WAKE_PATTERNS += [re.compile("豆{2}包{2}")]

# 服务端攒够 ~100ms 再喂模型，减少 30ms 小分块的解码伪影
FEED_BYTES = 3200


def make_recognizer():
    return sherpa_onnx.OnlineRecognizer.from_transducer(
        tokens=str(MODEL_DIR / "tokens.txt"),
        encoder=str(MODEL_DIR / "encoder-epoch-99-avg-1.int8.onnx"),
        decoder=str(MODEL_DIR / "decoder-epoch-99-avg-1.int8.onnx"),
        joiner=str(MODEL_DIR / "joiner-epoch-99-avg-1.int8.onnx"),
        num_threads=4,
        sample_rate=SAMPLE_RATE,
        feature_dim=80,
        decoding_method="modified_beam_search",
        provider="cpu",
    )


async def handle(ws):
    print(f"[{ts()}] ✅ 音箱已连接: {ws.remote_address}", flush=True)
    recognizer = make_recognizer()
    stream = recognizer.create_stream()

    hits = 0
    last_text = ""
    total_bytes = 0
    start_time = time.time()
    dup_chunks = 0
    total_chunks = 0
    prev_payload = b""
    feed_buf = []
    feed_len = 0

    async def send_request(cmd, payload=None):
        await ws.send(json.dumps({
            "Request": {"id": f"{cmd}-{time.monotonic_ns()}", "command": cmd, "payload": payload}
        }))

    async def speak(text):
        await send_request("run_shell", f"/usr/sbin/tts_play.sh '{text}'")

    async def stats_task():
        while True:
            await asyncio.sleep(10)
            rate = total_bytes / 1024 / max(0.1, time.time() - start_time)
            print(
                f"[{ts()}] 📊 音频 {total_bytes/1024:.0f}KB ({rate:.1f}KB/s，正常≈31)"
                f" | 重复帧 {dup_chunks}/{total_chunks} | 唤醒 {hits}",
                flush=True,
            )

    tasks = [asyncio.create_task(stats_task())]
    try:
        await send_request("start_recording", AUDIO_CONFIG)
        print(f"[{ts()}] 🎤 持续录音+实时转写中，请说「豆包豆包」", flush=True)

        async for msg in ws:
            if not isinstance(msg, bytes):
                obj = json.loads(msg)
                if "Event" in obj:
                    print(f"[{ts()}] 📨 event {obj['Event'].get('event')}", flush=True)
                continue

            # 二进制流帧（v1）
            buf = msg
            if buf[:1] == b"{":
                continue
            tag_len = buf[2]
            pos = 3 + tag_len
            id_len = buf[pos]
            pos += 1 + id_len
            if buf[1] & 0x01:
                (json_len,) = struct.unpack_from("<H", buf, pos)
                pos += 2 + json_len
            payload = buf[pos:]
            if not payload:
                continue

            # 重复帧检测（字节级比对）
            total_chunks += 1
            if payload == prev_payload:
                dup_chunks += 1
            prev_payload = payload
            total_bytes += len(payload)

            samples = np.frombuffer(payload, dtype="<i2").astype(np.float32) / 32768.0
            feed_buf.append(samples)
            feed_len += len(samples)
            if feed_len >= FEED_BYTES // 2:  # 1600 样本 = 100ms
                merged = np.concatenate(feed_buf)
                stream.accept_waveform(SAMPLE_RATE, merged)
                feed_buf, feed_len = [], 0
                while recognizer.is_ready(stream):
                    recognizer.decode_stream(stream)

            text = recognizer.get_result(stream) or ""
            if text != last_text and text.strip():
                print(f"[{ts()}] 🗣️ {text}", flush=True)
                last_text = text

            norm = normalize(text)
            if any(p.search(norm) for p in WAKE_PATTERNS):
                hits += 1
                print(f"[{ts()}] 🎯 唤醒命中: {WAKE_WORDS[0]}！（累计 {hits} 次）", flush=True)
                await speak(REPLY)
                stream = recognizer.create_stream()
                last_text = ""
            elif recognizer.is_endpoint(stream) or len(norm) > 80:
                # 一句话说完或上下文过长，重置转写
                stream = recognizer.create_stream()
                last_text = ""
    except websockets.ConnectionClosed as e:
        print(f"[{ts()}] ❌ 连接断开: {e}", flush=True)
    finally:
        for t in tasks:
            t.cancel()


async def main():
    async with websockets.serve(handle, "0.0.0.0", PORT, max_size=16 * 1024 * 1024):
        print(f"🚀 流式 ASR 唤醒 server: ws://0.0.0.0:{PORT}", flush=True)
        print(f"   模型: {MODEL_DIR}", flush=True)
        print(f"   唤醒词: {WAKE_WORDS}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
