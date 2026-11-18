"""Open-XiaoAI RK3588 语音服务 v3

架构：
  唤醒：sherpa-onnx BPE KWS（专用关键词检测，CJK 字符编码）
  声纹：WeSpeaker ResNet34-LM（唤醒后验证说话人身份）
  断句：Silero VAD（话语起止检测，按句定稿）
  看板：Web 实时展示转写/唤醒/声纹分数

模式：
- 常规（默认）：KWS 唤醒 → 声纹验证 → TTS 应答
- 注册（ENROLL=1）：采集 12s 语音 → 多段声纹入库
"""

import asyncio
import json
import os
import re
import struct
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import numpy as np
import websockets

import sherpa_onnx

# ---------------- 配置 ----------------
PORT = int(os.environ.get("PORT", "4399"))
DASH_PORT = int(os.environ.get("DASH_PORT", "8080"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
VOICEPRINT_FILE = DATA_DIR / "voiceprints.json"

MODEL_KWS = Path(os.environ.get("MODEL_KWS", "/models/kws"))
MODEL_SPK = Path(os.environ.get("MODEL_SPK", "/models/speaker/wespeaker_zh_cnceleb_resnet34_LM.onnx"))
MODEL_VAD = Path(os.environ.get("MODEL_VAD", "/models/vad/silero_vad.onnx"))

WAKE_WORDS = [w.strip() for w in os.environ.get("WAKE_WORDS", "豆包豆包").split(",") if w.strip()]
SPK_THRESHOLD = float(os.environ.get("SPK_THRESHOLD", "0.18"))
ENROLL = os.environ.get("ENROLL", "0") == "1"
ENROLL_NAME = os.environ.get("ENROLL_NAME", "owner")
ENROLL_SECONDS = float(os.environ.get("ENROLL_SECONDS", "12"))
REPLY_OK = os.environ.get("REPLY_OK", "哎，我在")

SAMPLE_RATE = 16000
AUDIO_CONFIG = {
    "pcm": "noop",
    "channels": 1,
    "bits_per_sample": 16,
    "sample_rate": SAMPLE_RATE,
    "period_size": 160,
    "buffer_size": 480,
}

RING_SECONDS = 6
VERIFY_SAMPLES = int(2.2 * SAMPLE_RATE)
VERIFY_TOPUP_SECONDS = 1.2
VAD_GAIN = 4.0
WAKE_COOLDOWN = 3.0
SPK_LOUDNESS_LIMIT = 0.45


def ts():
    return time.strftime("%H:%M:%S")


def normalize(text: str) -> str:
    return re.sub(r"[^a-z\u4e00-\u9fff]", "", text.lower())


def cosine(a, b) -> float:
    a, b = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b) + 1e-12))


# ---------------- 全局模型 ----------------
kws = None          # KeywordSpotter (BPE)
spk_extractor = None
spk_manager = None
vad = None          # Silero VAD
DASHBOARDS = set()
STATS = {"hits": 0, "rejected": 0, "started": time.time(), "last_score": None, "last_speaker": None}


def init_models():
    global kws, spk_extractor, spk_manager, vad

    # --- KWS：BPE 模型 ---
    print(f"[{ts()}] 🧠 加载 KWS 模型: {MODEL_KWS}", flush=True)
    t0 = time.time()
    enc = MODEL_KWS / "encoder.onnx"
    if not enc.exists():
        enc = MODEL_KWS / "encoder-epoch-99-avg-1.onnx"
    kws = sherpa_onnx.KeywordSpotter(
        provider="cpu", num_threads=1, max_active_paths=8,
        keywords_score=2.0, keywords_threshold=0.2, num_trailing_blanks=0,
        keywords_file=str(MODEL_KWS / "keywords.txt"),
        tokens=str(MODEL_KWS / "tokens.txt"),
        encoder=str(enc),
        decoder=str(next(MODEL_KWS.glob("decoder*.onnx"))),
        joiner=str(next(MODEL_KWS.glob("joiner*.onnx"))),
    )
    print(f"[{ts()}] ✅ KWS 就绪（{time.time()-t0:.1f}s）", flush=True)

    # --- 声纹 ---
    print(f"[{ts()}] 🧠 加载声纹模型: {MODEL_SPK}", flush=True)
    t0 = time.time()
    spk_extractor = sherpa_onnx.SpeakerEmbeddingExtractor(
        sherpa_onnx.SpeakerEmbeddingExtractorConfig(model=str(MODEL_SPK)))
    spk_manager = sherpa_onnx.SpeakerEmbeddingManager(spk_extractor.dim)
    if VOICEPRINT_FILE.exists():
        data = json.loads(VOICEPRINT_FILE.read_text(encoding="utf-8"))
        for name, embs in data.items():
            for i, emb in enumerate(embs):
                spk_manager.add(f"{name}#{i}", emb)
        print(f"[{ts()}] ✅ 已加载声纹: {list(data.keys())}", flush=True)
    else:
        print(f"[{ts()}] ⚠️ 无已注册声纹，请先以 ENROLL=1 运行注册", flush=True)
    print(f"[{ts()}] ✅ 声纹模型就绪（{time.time()-t0:.1f}s，dim={spk_extractor.dim}）", flush=True)

    # --- Silero VAD ---
    if MODEL_VAD.exists():
        vc = sherpa_onnx.VadModelConfig()
        vc.silero_vad.model = str(MODEL_VAD)
        vc.silero_vad.threshold = 0.5
        vc.silero_vad.min_speech_duration = 0.25
        vc.silero_vad.min_silence_duration = 0.6
        vc.sample_rate = SAMPLE_RATE
        vad = sherpa_onnx.VoiceActivityDetector(vc, buffer_size_in_seconds=30)
        print(f"[{ts()}] ✅ Silero VAD 就绪", flush=True)


def compute_embedding(samples: np.ndarray):
    s = spk_extractor.create_stream()
    s.accept_waveform(SAMPLE_RATE, samples.astype(np.float32))
    if not spk_extractor.is_ready(s):
        return None
    return spk_extractor.compute(s)


def verify_embedding(emb):
    if spk_manager.num_speakers == 0:
        return True, 1.0, "(未注册，跳过验证)"
    person_best = {}
    for entry in spk_manager.all_speakers:
        base = entry.split("#")[0]
        score = spk_manager.score(entry, emb)
        person_best[base] = max(person_best.get(base, -1.0), score)
    best_name = max(person_best, key=person_best.get)
    return person_best[best_name] >= SPK_THRESHOLD, person_best[best_name], best_name


def voiced_chunks(pcm: np.ndarray, rate: int = SAMPLE_RATE,
                  rms_threshold: float = 0.012, min_ms: int = 400):
    frame = rate // 10
    chunks, cur = [], []
    for i in range(0, len(pcm) - frame + 1, frame):
        if float(np.sqrt(np.mean(pcm[i:i + frame] ** 2))) >= rms_threshold:
            cur.extend(pcm[i:i + frame])
        else:
            if len(cur) >= rate * min_ms / 1000:
                chunks.append(np.array(cur))
            cur = []
    if len(cur) >= rate * min_ms / 1000:
        chunks.append(np.array(cur))
    return chunks


def save_voiceprint(name: str, embeddings: list):
    data = json.loads(VOICEPRINT_FILE.read_text(encoding="utf-8")) if VOICEPRINT_FILE.exists() else {}
    data[name] = embeddings
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    VOICEPRINT_FILE.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    print(f"[{ts()}] 💾 声纹已保存: {name}（{len(embeddings)} 条）", flush=True)


def decode_stream_frame(buf: bytes) -> bytes:
    if buf[:1] == b"{":
        return b""
    tag_len = buf[2]
    pos = 3 + tag_len
    id_len = buf[pos]
    pos += 1 + id_len
    if buf[1] & 0x01:
        (json_len,) = struct.unpack_from("<H", buf, pos)
        pos += 2 + json_len
    return buf[pos:]


def dump_wav(path: Path, samples: np.ndarray):
    try:
        import wave as wav_mod
        pcm16 = (np.clip(samples, -1.0, 1.0) * 32767).astype("<i2")
        with wav_mod.open(str(path), "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(SAMPLE_RATE)
            w.writeframes(pcm16.tobytes())
    except Exception as e:
        print(f"dump_wav: {e}", flush=True)


async def broadcast(event: dict):
    if not DASHBOARDS:
        return
    msg = json.dumps({**event, "time": ts()}, ensure_ascii=False)
    for ws in list(DASHBOARDS):
        try:
            await ws.send(msg)
        except Exception:
            DASHBOARDS.discard(ws)


# ---------------- 音箱连接处理 ----------------
async def handle(ws):
    path = getattr(getattr(ws, "request", None), "path", "/")
    if path.startswith("/dashboard"):
        DASHBOARDS.add(ws)
        try:
            await ws.send(json.dumps({
                "type": "hello", "wake_words": WAKE_WORDS,
                "threshold": SPK_THRESHOLD, "stats": STATS,
                "registered": list(spk_manager.all_speakers) if spk_manager else [],
                "time": ts(),
            }, ensure_ascii=False))
            async for _ in ws:
                pass
        except Exception:
            pass
        finally:
            DASHBOARDS.discard(ws)
        return

    print(f"[{ts()}] ✅ 音箱已连接: {ws.remote_address}", flush=True)

    async def send_request(cmd, payload=None):
        await ws.send(json.dumps({
            "Request": {"id": f"{cmd}-{time.monotonic_ns()}", "command": cmd, "payload": payload}
        }))

    async def speak(text):
        await send_request("run_shell", f"/usr/sbin/tts_play.sh '{text}'")

    # ---------- 注册模式 ----------
    if ENROLL:
        await speak(f"声纹注册开始，请在提示音结束后正常说几句话")
        await asyncio.sleep(6.0)
        await send_request("start_recording", AUDIO_CONFIG)
        print(f"[{ts()}] 🎙️ 注册模式 [{ENROLL_NAME}]：请说几句话（{ENROLL_SECONDS}s）...", flush=True)
        pcm = []
        target = int(ENROLL_SECONDS * SAMPLE_RATE)
        try:
            async for msg in ws:
                if not isinstance(msg, bytes):
                    continue
                payload = decode_stream_frame(msg)
                if not payload:
                    continue
                pcm.append(np.frombuffer(payload, dtype="<i2").astype(np.float32) / 32768.0)
                if sum(len(x) for x in pcm) >= target:
                    break
        except websockets.ConnectionClosed:
            pass
        audio = np.concatenate(pcm) if pcm else np.zeros(0, dtype=np.float32)
        audio = audio[int(1.5 * SAMPLE_RATE):]
        chunks = voiced_chunks(audio)
        seg = int(4 * SAMPLE_RATE)
        split = []
        for c in chunks:
            for i in range(0, len(c), seg):
                if len(c[i:i + seg]) >= SAMPLE_RATE:
                    split.append(c[i:i + seg])
        chunks = split
        total = sum(len(c) for c in chunks)
        print(f"[{ts()}] 📊 采集 {len(audio)/SAMPLE_RATE:.1f}s，语音段 {len(chunks)} 个", flush=True)
        if total < SAMPLE_RATE * 3:
            await speak("语音不足，注册失败，请重试")
            await asyncio.sleep(2)
            os._exit(1)
        embeddings = [e for e in (compute_embedding(c) for c in chunks) if e is not None]
        if not embeddings:
            await speak("注册失败")
            await asyncio.sleep(2)
            os._exit(1)
        if len(embeddings) >= 3:
            sims = [float(np.mean([cosine(embeddings[i], embeddings[j])
                                   for j in range(len(embeddings)) if j != i]))
                    for i in range(len(embeddings))]
            kept = [e for i, e in enumerate(embeddings) if sims[i] >= 0.35]
            print(f"[{ts()}] 🧹 互筛: {[round(s,2) for s in sims]}，保留 {len(kept)}/{len(embeddings)}", flush=True)
            if kept:
                embeddings = kept
        save_voiceprint(ENROLL_NAME, embeddings)
        dump_wav(DATA_DIR / "enroll_dump.wav", audio)
        for i, emb in enumerate(embeddings):
            spk_manager.add(f"{ENROLL_NAME}#{i}", emb)
        await speak("注册完成")
        await asyncio.sleep(3)
        os._exit(0)

    # ---------- 常规模式 ----------
    ring = deque(maxlen=SAMPLE_RATE * RING_SECONDS)
    kws_stream = kws.create_stream()
    feed_buf, feed_len = [], 0
    last_hit_time = 0.0
    utterance_active = False

    async def finish_verify(audio: np.ndarray, keyword: str):
        STATS["hits"] += 1
        await broadcast({"type": "wake", "matched": keyword})
        va = audio[-VERIFY_SAMPLES:]
        dump_wav(DATA_DIR / "verify_dump.wav", va)
        rms = float(np.sqrt(np.mean(va.astype(np.float64) ** 2)))
        if rms > SPK_LOUDNESS_LIMIT:
            STATS["rejected"] += 1
            await broadcast({"type": "verify", "pass": False, "score": 0,
                             "name": "-", "note": f"响度异常({rms:.2f})"})
            print(f"[{ts()}] 🚫 响度异常 RMS={rms:.3f}，拒绝", flush=True)
            return
        emb = compute_embedding(va)
        if emb is None:
            STATS["rejected"] += 1
            await broadcast({"type": "verify", "pass": False, "score": 0, "name": "-", "note": "音频不足"})
            return
        ok, score, name = verify_embedding(emb)
        STATS["last_score"], STATS["last_speaker"] = round(score, 3), name
        await broadcast({"type": "verify", "pass": bool(ok), "score": round(score, 3), "name": name})
        if ok:
            print(f"[{ts()}] 🎯 唤醒: {keyword} | 声纹: {name} score={score:.3f} ✅", flush=True)
            await speak(REPLY_OK)
        else:
            STATS["rejected"] += 1
            print(f"[{ts()}] 🚫 声纹不匹配（{name} score={score:.3f}）", flush=True)

    async def stats_task():
        while True:
            await asyncio.sleep(30)
            await broadcast({"type": "stats", "stats": STATS})

    tasks = [asyncio.create_task(stats_task())]
    try:
        await send_request("start_recording", AUDIO_CONFIG)
        print(f"[{ts()}] 🎤 KWS 监听中（唤醒词: {WAKE_WORDS}，阈值: {SPK_THRESHOLD}）", flush=True)

        async for msg in ws:
            if not isinstance(msg, bytes):
                obj = json.loads(msg)
                if "Event" in obj:
                    await broadcast({"type": "event", "name": obj["Event"].get("event")})
                continue
            payload = decode_stream_frame(msg)
            if not payload:
                continue
            samples = np.frombuffer(payload, dtype="<i2").astype(np.float32) / 32768.0
            ring.extend(samples)

            # --- KWS 唤醒检测（BPE 专用引擎） ---
            feed_buf.append(samples)
            feed_len += len(samples)
            if feed_len < FEED_SAMPLES:
                continue
            merged = np.concatenate(feed_buf)
            feed_buf, feed_len = [], 0

            kws_stream.accept_waveform(SAMPLE_RATE, merged)
            while kws.is_ready(kws_stream):
                kws.decode_stream(kws_stream)
            result = kws.get_result(kws_stream)
            if result:
                keyword = result.strip().lstrip("▁")
                if time.time() - last_hit_time < WAKE_COOLDOWN:
                    kws.reset_stream(kws_stream)
                    continue
                last_hit_time = time.time()
                try:
                    audio = np.asarray(ring, dtype=np.float32)[-VERIFY_SAMPLES:]
                    if len(audio) >= VERIFY_SAMPLES:
                        await finish_verify(audio, keyword)
                    else:
                        await asyncio.sleep(VERIFY_TOPUP_SECONDS)
                        audio = np.asarray(ring, dtype=np.float32)[-VERIFY_SAMPLES:]
                        await finish_verify(audio, keyword)
                except Exception:
                    import traceback
                    print(f"[{ts()}] ❗ 唤醒处理异常:\n{traceback.format_exc()}", flush=True)
                kws.reset_stream(kws_stream)
    except websockets.ConnectionClosed as e:
        print(f"[{ts()}] ❌ 连接断开: {e}", flush=True)
    finally:
        for t in tasks:
            t.cancel()
        await broadcast({"type": "speaker_offline"})


FEED_SAMPLES = 1600


# ---------------- 看板 ----------------
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>小爱语音看板</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,"Microsoft YaHei",sans-serif;background:#0f1117;color:#d7dae0;padding:16px}
h1{font-size:18px;margin-bottom:12px;color:#7aa2f7}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;max-width:980px;margin:0 auto}
.card{background:#1a1d29;border-radius:12px;padding:14px;border:1px solid #2a2e3d}
.card h2{font-size:13px;color:#8b90a0;margin-bottom:10px;font-weight:500}
#status{font-size:13px}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
.on{background:#4ade80}.off{background:#f87171}
.num{font-size:28px;font-weight:700;color:#fff}
.bar{height:10px;background:#2a2e3d;border-radius:5px;overflow:hidden;margin:6px 0}
.bar>div{height:100%;border-radius:5px}
.good{background:linear-gradient(90deg,#22c55e,#4ade80)}
.bad{background:linear-gradient(90deg,#ef4444,#f87171)}
#events{max-height:300px;overflow-y:auto;font-size:13px}
.ev{padding:8px;border-radius:8px;margin-bottom:8px}
.ev.pass{background:#12291c;border:1px solid #1f5133}
.ev.reject{background:#2d1618;border:1px solid #5c2027}
.ev.wake{background:#1e2a3a;border:1px solid #2d4a6b}
.ev.err{background:#2d2416;border:1px solid #5c4a20}
.muted{color:#565f89;font-size:12px}
.thr::after{content:"";position:absolute;left:var(--thr);top:-12px;width:2px;height:14px;background:#e0af68}
</style>
</head>
<body>
<h1>🎙️ KWS+声纹看板 <span class="muted" id="meta"></span></h1>
<div class="grid">
<div class="card"><h2>状态</h2>
<div id="status"><span class="dot off" id="dot"></span><span id="st">连接中…</span></div>
<div style="display:flex;gap:24px;margin-top:12px">
<div><div class="num" id="hits">0</div><div class="muted">唤醒</div></div>
<div><div class="num" id="rej">0</div><div class="muted">拒绝</div></div>
<div><div class="num" id="spk">-</div><div class="muted">说话人</div></div>
</div>
<h2 style="margin-top:16px">声纹分数</h2>
<div class="bar thr" id="thrbar" style="position:relative"><div id="scorebar" class="bad" style="width:0%"></div></div>
<div class="muted" id="scoretext">等待唤醒（阈值 <span id="thr"></span>）</div>
</div>
<div class="card"><h2>事件流</h2><div id="events"></div></div>
</div>
<script>
const $=id=>document.getElementById(id);let hits=0,rej=0,thr=.18;
function connect(){const ws=new WebSocket(`ws://${location.hostname}:4399/dashboard`);
ws.onopen=()=>{$("dot").className="dot on";$("st").textContent="已连接";};
ws.onclose=()=>{$("dot").className="dot off";$("st").textContent="重连中…";setTimeout(connect,1500);};
ws.onmessage=e=>{const m=JSON.parse(e.data);
if(m.type==="hello"){thr=m.threshold;$("thr").textContent=thr;$("thrbar").style.setProperty("--thr",thr*100+"%");$("meta").textContent=`唤醒词: ${m.wake_words.join("/")}`;}
else if(m.type==="wake"){addEv(`<div class="ev wake">🎯 KWS命中: ${m.matched}<span class="t">${m.time}</span></div>`);}
else if(m.type==="verify"){const p=Math.max(0,Math.min(100,m.score*100));$("scorebar").style.width=p+"%";$("scorebar").className=m.pass?"good":"bad";$("scoretext").textContent=`${m.name}: ${m.score.toFixed(3)} ${m.pass?"✅":"❌"} ${m.note||""}`;$("spk").textContent=m.pass?m.name:"陌生人";if(!m.pass){rej++;$("rej").textContent=rej;}addEv(`<div class="ev ${m.pass?"pass":"reject"}">${m.pass?"✅":"🚫"} ${m.name}=${m.score.toFixed(3)} ${m.note||""}<span class="t">${m.time}</span></div>`);}
else if(m.type==="error"){addEv(`<div class="ev err">❗ ${m.detail}<span class="t">${m.time}</span></div>`);}
};}
function addEv(h){const d=document.createElement("div");d.innerHTML=h;const e=$("events");e.prepend(d);while(e.children.length>50)e.lastChild.remove();}
connect();
</script>
</body>
</html>"""


class DashHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(DASHBOARD_HTML.encode("utf-8"))

    def log_message(self, *args):
        pass


def start_dash_http():
    HTTPServer(("0.0.0.0", DASH_PORT), DashHandler).serve_forever()


async def main():
    init_models()
    threading.Thread(target=start_dash_http, daemon=True).start()
    mode = " [注册模式]" if ENROLL else ""
    async with websockets.serve(handle, "0.0.0.0", PORT, max_size=16 * 1024 * 1024):
        print(f"🚀 RK 语音服务 v3: ws://0.0.0.0:{PORT}{mode} | 看板: http://0.0.0.0:{DASH_PORT}", flush=True)
        print(f"   唤醒: BPE KWS | 声纹: WeSpeaker | 阈值: {SPK_THRESHOLD} | 应答: {REPLY_OK}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
