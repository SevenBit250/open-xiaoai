"""模拟崩溃翻动的 server：完成 WS 握手后立即断开，触发 client 重连日志风暴。"""
import asyncio

import websockets


async def handle(ws):
    await ws.close()


async def main():
    async with websockets.serve(handle, "0.0.0.0", 4399):
        print("flapper server: ws://0.0.0.0:4399 (accept & close)", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
