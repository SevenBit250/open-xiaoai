"""Open-XiaoAI 测试 Server

用途：验证 client 协议、观察事件流、压测时收音频流统计。
协议（与 packages/client-rust/src/services/connect/data.rs 对齐）：
  - Text  帧: {"Event"|"Request"|"Response": {...}}
  - Binary帧: JSON 编码的 {"id","tag","bytes","data"}（音频流，bytes 为数字数组）

用法: python tools/test-server.py [端口，默认 4399]
"""

import asyncio
import json
import sys
import time

import websockets

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 4399

stream_stats = {}  # tag -> [chunks, bytes]
event_log = []


async def handle(ws):
    peer = ws.remote_address
    print(f"[{time.strftime('%H:%M:%S')}] ✅ client 已连接: {peer}", flush=True)
    ping_task = None

    async def ping():
        # 每 30s 调一次 get_version 验证 RPC 通路
        i = 0
        while True:
            await asyncio.sleep(30)
            i += 1
            await ws.send(json.dumps({
                "Request": {"id": f"ping-{i}", "command": "get_version", "payload": None}
            }))

    try:
        ping_task = asyncio.create_task(ping())
        async for msg in ws:
            if isinstance(msg, bytes):
                # 音频流：只统计，不解析大数组
                s = stream_stats.setdefault("record", [0, 0])
                try:
                    obj = json.loads(msg)
                    tag = obj.get("tag", "record")
                    s = stream_stats.setdefault(tag, [0, 0])
                    s[0] += 1
                    s[1] += len(obj.get("bytes", []))
                except Exception:
                    s[0] += 1
                    s[1] += len(msg)
                continue

            obj = json.loads(msg)
            if "Event" in obj:
                ev = obj["Event"]
                name = ev.get("event")
                data = json.dumps(ev.get("data"), ensure_ascii=False)
                if len(data) > 200:
                    data = data[:200] + "..."
                line = f"[{time.strftime('%H:%M:%S')}] 📨 event {name}: {data}"
                print(line, flush=True)
                event_log.append((time.time(), name, ev.get("data")))
            elif "Response" in obj:
                r = obj["Response"]
                print(f"[{time.strftime('%H:%M:%S')}] 📨 response {r.get('id')}: "
                      f"code={r.get('code')} data={str(r.get('data'))[:120]}", flush=True)
            elif "Request" in obj:
                req = obj["Request"]
                print(f"[{time.strftime('%H:%M:%S')}] 📨 client request {req.get('command')}", flush=True)
            else:
                print(f"[{time.strftime('%H:%M:%S')}] ❓ unknown: {msg[:200]}", flush=True)
    except websockets.ConnectionClosed as e:
        print(f"[{time.strftime('%H:%M:%S')}] ❌ 连接断开: {e}", flush=True)
    finally:
        if ping_task:
            ping_task.cancel()
        if stream_stats:
            for tag, (n, b) in stream_stats.items():
                print(f"📊 stream[{tag}]: {n} chunks, {b} bytes", flush=True)


async def main():
    async with websockets.serve(handle, "0.0.0.0", PORT, max_size=64 * 1024 * 1024):
        print(f"🚀 测试 server 已启动: ws://0.0.0.0:{PORT}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
