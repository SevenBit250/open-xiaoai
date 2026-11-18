# Open-XiaoAI RK3588 开发日志

> 2026-11-15 ~ 2026-11-19 | 从零到生产可用的唤醒+声纹系统

---

## 总览

在 RK3588 上构建了完整的语音唤醒服务（Docker 化），从音箱固件刷机、Rust client 五轮性能优化、到最终的服务端 v3 架构（BPE KWS 唤醒 + WeSpeaker 声纹验证）。

## 硬件与基础设施

### 音箱端（LX06 小爱音箱 Pro）
- **刷机**：绕过小米账号风控（passToken 被拒 70016），通过 miwifi 灰度 API 签名直链下载固件，Docker 制作补丁固件（SSH 固化 + 禁 OTA + `/data/init.sh` 钩子）
- **SSH 免密**：Dropbear v2017.75 不支持 ed25519，用 RSA + `/data/dropbear/authorized_keys` bind mount 持久化
- **开机自启**：`/data/init.sh` 拉起 client，断电重启零人工干预

### 服务端（RK3588, Armbian aarch64, 4GB RAM）
- Docker 镜像在 WSL 交叉构建（arm64 + qemu binfmt），`docker save | scp | docker load` 部署
- 镜像加速：daocloud/1panel/1ms.run 三镜像源
- 模型下载：GitHub 直连（2MB/s 突发）、hf-mirror（ hf-mirror.com 秒级）、ModelScope 国内镜像（16s/百MB）、局域网 Clash 代理

---

## Rust Client 五轮优化（5 commits, 全部真机实测）

| Commit | 优化项 | 实测收益 |
|---|---|---|
| `a944f92` | P0: ubus 原生轮询替代 mphelper fork 风暴 | 31ms→0.95ms/次，零进程 spawn，load 2.03→1.35 |
| `9dc1662` | P1-1: 音频流 v1 二进制帧替代 JSON 数字数组 | 线上开销 x2.58→x1.048，带宽省 60% |
| `be2426b` | P1-2: Opus 压缩（Voip/24kbps/20ms 帧）+ P2-3 播放预缓冲 | 10.9x 压缩，underrun 4~6次→0 |
| `f7b4c14` | P2-1: inotify 事件驱动文件监控 + 安全兜底 | 空闲零系统调用；**发现 LX06 4.9 内核 fsnotify 间歇性失效** |
| `1ea064b` | config.json 配置 + 内置滚动日志 | 日志恒 ≤ 上限，断连风暴免疫 |

**综合效果**：4 秒麦克风上行从 637KB → 20KB（32 倍）；音箱 CPU 持续负载降 ~25%。

### 关键发现
- LX06 4.9 内核 fsnotify 间歇性整体失效（busybox inotifyd 也收不到事件，dmesq 无线索），需兜底轮询
- 微服务代理场景下 Windows `asyncio.sleep` 计时粒度 ~15ms，无法做 1.0x 精确推流
- Dropbear v2017.75 不支持 ed25519 公钥

---

## 服务端架构演进

### v1 → v2：paraformer + WeSpeaker（已过时但经验宝贵）

| 组件 | 方案 | 关键决策/发现 |
|---|---|---|
| ASR | sherpa paraformer（int8） | 替换 zipformer int8 后叠字消失 |
| 声纹 | WeSpeaker ResNet34-LM (dim=256) | 替换 3D-Speaker ERes2NetV2，分离度 +0.275 vs +0.123 |
| VAD | Silero（4x 增益） | 麦克风低电平（峰值 ~2^26），Silero 默认灵敏度不够 |
| 反作弊 | RMS 响度检测（阈值 0.45） | 防音箱自播攻击；真人喊唤醒 RMS 可达 0.3+ |

### v3（当前）：BPE KWS 专用唤醒引擎

**核心突破**：发现 xiaozhi 例程用的是 **sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20**（标准 ASR 模型）的 **FP32 版 + BPE 子词 token 体系**作为 KWS 引擎——而非官方标注的"KWS 专用"拼音小模型。

#### 为什么 BPE >> 拼音（对唤醒词检测）

| 维度 | 拼音模型 (wenetspeech-3.3M) | BPE 模型 (bilingual-zh-en-2023) |
|---|---|---|
| Token 粒度 | 音素级 (`d òu b āo`) | 字符/子词级 (`豆 包`) |
| 容错性 | 一个音素偏 → 整链断 | 口音/语速/声调偏差 → 同一 token |
| 精度 | int8 量化 | FP32 全精度 |
| 唤醒延迟 | chunk-16 (320ms) | chunk-8 (160ms) |
| 真实麦克风检测 | ❌ 0/4 命中 | ✅ 4/4 命中 |

**关键参数**：`num_trailing_blanks=0, keywords_score=2.0, keywords_threshold=0.2, max_active_paths=8`

#### 架构（用户设计）

```
音箱 client ──WS(Opus/二进制帧)──> RK3588 Docker
  BPE KWS 唤醒（只认词，不认人）→ 任何人都能唤醒 ✓
    └─ 后处理：声纹验证（WeSpeaker）
       ├─ 普通对话：放行
       └─ 敏感操作：仅 owner
```

---

## 音频链路探索

### 8 通道 TDM 麦克风阵列
- **6 个 MEMS 麦克风**（六边形排列），ch6-7 静音
- TDM 数据峰值 ~2^26（非 2^31），客户端 `>>8` 转 S16
- 通道间延迟 0-2 样本 @48kHz（小阵列远场近似零延迟）
- 波束成形（delay-and-sum）对 KWS 检测率无提升——问题不在音频质量，在模型 token 体系

### 踩坑记录
| 坑 | 原因 | 解法 |
|---|---|---|
| KWS 拼音模型真麦全灭 | token 体系不适配（音素级 vs 字符级） | 换 BPE 模型 |
| Silero VAD 静音不检出 | 麦克风电平低（峰值~2^26） | 4x 增益预处理 |
| 声纹"一个人识别出 5 种" | 注册时 TTS 提示音混入注册音频 | 提示音播完再录 + 注册段互筛 |
| Colab openWakeWord 训练崩 | Python 3.13 无 piper-phonemize/torchvision wheel | 放弃训练，直接用 BPE KWS |
| 音箱"死机"（小爱无反应） | 多次服务重启后 arecord 子进程死锁 | 重启音箱恢复 |

---

## 部署清单

### RK3588 目录结构
```
/home/awei/open-xiaoai/
├── docker-compose.yml
├── app/
│   ├── Dockerfile
│   └── server.py          # v3 服务
├── models/
│   ├── kws/               # BPE KWS 模型（encoder/decoder/joiner/tokens/keywords/bpe.model）
│   ├── speaker/            # WeSpeaker ResNet34-LM
│   └── vad/                # Silero VAD
└── data/
    └── voiceprints.json   # 注册声纹
```

### 常用命令
```bash
# 常规启动
cd ~/open-xiaoai && docker compose up -d

# 声纹注册
docker compose down && \
docker compose run --rm --service-ports -e ENROLL=1 -e ENROLL_NAME=owner xiaoai-voice && \
docker compose up -d

# 看板
http://<RK_IP>:8080

# 音箱配置
/data/open-xiaoai/config.json → {"server": "ws://<RK_IP>:4399", ...}
```

---

## 后续方向

- [ ] 6 麦波束成形（需标定阵列几何参数）
- [ ] 目标说话人提取 TSE（只听主人的声音）
- [ ] 对话 ASR（云端火山流式识别）+ LLM（豆包）
- [ ] 多房间音箱组网
