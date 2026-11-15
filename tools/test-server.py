"""Open-XiaoAI 测试 Server

用途：验证 client 协议、观察事件流、测试音频流（含 P1-1 二进制帧统计）。

协议（与 packages/client-rust/src/services/connect/data.rs + message.rs 对齐）：
  - Text  帧: {"Event"|"Request"|"Response": {...}}
  - Binary帧（v1 二进制流帧，兼容旧 JSON 帧——首字节 '{' 即旧格式）:
      [0]=0x01 版本 [1]=flags(bit0=含json) [2]=tag_len [tag] [id_len] [id]
      ([2B LE json_len][json]) [rest]=payload

用法:
  python tools/test-server.py [端口]            # 仅观察
  python tools/test-server.py [端口] --auto     # 连接后自动: 录音 8s -> 播放 1.5s 提示音(有声)
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

DEFAULT_AUDIO_CONFIG = {
    "pcm": "noop",
    "channels": 1,
    "bits_per_sample": 16,
    "sample_rate": 16000,
    "period_size": 160,
    "buffer_size": 480,
}


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


class Stats:
    def __init__(self):
        self.chunks = 0
        self.payload = 0
        self.wire = 0
        self.legacy_equiv = 0  # 同样 payload 若用旧 JSON 数字数组编码的估算体积
        self.samples = 0

    def add(self, payload_len: int, wire_len: int, payload: bytes):
        self.chunks += 1
        self.payload += payload_len
        self.wire += wire_len
        # 只对前 20 个 chunk 精确计算旧编码体积（够估算了）
        if self.samples < 20 and payload_len:
            self.legacy_equiv += len(json.dumps(list(payload), separators=(",", ":"))) + 48
            self.samples += 1

    def report(self, name: str):
        if not self.chunks:
            print(f"📊 {name}: 无数据", flush=True)
            return
        ratio = self.wire / self.payload if self.payload else 0
        legacy_ratio = self.legacy_equiv / max(
            1, self.payload * self.samples // self.chunks
        )
        print(
            f"📊 {name}: {self.chunks} chunks | payload {self.payload/1024:.1f}KB "
            f"| 线上 {self.wire/1024:.1f}KB (x{ratio:.3f}) "
            f"| 旧 JSON 编码估算 x{legacy_ratio:.2f}",
            flush=True,
        )


async def handle(ws):
    print(f"[{ts()}] ✅ client 已连接: {ws.remote_address}", flush=True)
    stats = {}
    rx_play_stats = Stats()

    async def send_request(cmd, payload=None):
        await ws.send(json.dumps({
            "Request": {"id": f"{cmd}-{time.monotonic_ns()}", "command": cmd, "payload": payload}
        }))

    async def auto_test():
        try:
            await asyncio.sleep(2)
            print(f"[{ts()}] 🎤 启动录音（默认配置，8s）...", flush=True)
            await send_request("start_recording", DEFAULT_AUDIO_CONFIG)
            await asyncio.sleep(8)
            await send_request("stop_recording")
            await asyncio.sleep(1)
            if "record" in stats:
                stats["record"].report("上行 record 流")
            await asyncio.sleep(1)
            print(f"[{ts()}] 🔊 播放 1.5s 提示音（660Hz 正弦波，音箱会出声）...", flush=True)
            await send_request("start_play", DEFAULT_AUDIO_CONFIG)
            pcm = sine_pcm()
            chunk = 960  # 30ms
            for i in range(0, len(pcm), chunk):
                await ws.send(encode_stream_frame("play", pcm[i:i + chunk]))
                await asyncio.sleep(0.03)
            await asyncio.sleep(0.3)
            await send_request("stop_play")
            print(f"[{ts()}] 🔊 播放完成", flush=True)
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
                if tag == "play":
                    rx_play_stats.add(payload_len, wire_len, stream.get("bytes", b""))
                else:
                    stats.setdefault(tag, Stats()).add(
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
                      f"data={str(r.get('data'))[:100]}", flush=True)
            elif "Request" in obj:
                print(f"[{ts()}] 📨 client request {obj['Request'].get('command')}", flush=True)
    except websockets.ConnectionClosed as e:
        print(f"[{ts()}] ❌ 连接断开: {e}", flush=True)
    finally:
        for t in tasks:
            t.cancel()
        for tag, s in stats.items():
            s.report(f"上行 {tag} 流")
        rx_play_stats.report("下行 play 流(回环)")


async def main():
    async with websockets.serve(handle, "0.0.0.0", PORT, max_size=64 * 1024 * 1024):
        mode = " [AUTO: 录音8s+播放提示音]" if AUTO else ""
        print(f"🚀 测试 server: ws://0.0.0.0:{PORT}{mode}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
