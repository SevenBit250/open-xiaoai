# Rust Client 问题清单与优化记录

> 基于真机实测（小爱音箱 Pro LX06，ROM 1.94.13，192.168.31.93）。
> 测量时间：2026-11-15。修改任何一项后请在「状态」栏更新并补充实测对比数据。

## 设备基线（修前对照用）

| 项目 | 值 |
|---|---|
| CPU | 4× Cortex-A53（32 位 armv7 用户态） |
| 内存 | 128MB，可用 ~54MB，无 swap |
| 存储 | `/data`（ubifs）剩 8.1MB；rootfs 只读 squashfs；`/tmp` 58MB tmpfs |
| 系统 | OpenWrt 定制 + procd，glibc 2.25，无 bash / opkg，Dropbear v2017.75（仅 RSA） |
| 空闲负载 | load ~1.0，每核 ~20%（官方服务即吃掉） |
| 关键路径 | `instruction.log` 实时写入正常；采音 `noop→Capture→dsnoop hw:0,3`（S32_LE 8ch 48k，ALSA plug 转 16k） |

实测基准（真机，各 200 次）：

| 调用方式 | 单次耗时 |
|---|---|
| `/bin/sh -c "mphelper mute_stat"`（现状路径） | ~31ms |
| `mphelper mute_stat` 直接调用 | ~26ms |
| `ubus call mediaplayer player_get_play_status` | ~6ms |

---

## 问题列表

### 🔴 P0-1 PlayingMonitor 进程风暴

- **组件**：`packages/client-rust/src/services/monitor/playing.rs`（播放状态监控器）
- **作用**：监测音箱播放/暂停/空闲状态变化，上报 `playing` 事件给 Server，用于「用户说话打断 TTS 播报」的检测
- **问题**：每 10ms 调 `run_shell("mphelper mute_stat")` 起一个 `/bin/sh -c` 子进程。轮询间隔(10ms) < 单次耗时(31ms)，实际是永不间断的 fork/exec 风暴：~64 进程/秒，吃掉约 1 个 CPU 核（全机 25%），加剧 128MB 小内存的碎片化
- **修法**：
  1. 便宜版：改用 `ubus call mediaplayer player_get_play_status`（实测 5 倍快，还能拿音量）+ 自适应间隔（空闲 500ms / 活动期 50ms）
  2. 正解：Rust 直连 ubus unix socket `/var/run/ubus.sock`（协议简单，零进程 spawn）
  3. 事件版：`ubus listen` 订阅 mediaplayer 事件，免轮询
- **状态**：✅ 已修复（2026-11-15，采用正解 + 自适应轮询）

**实测数据（LX06 1.94.13 真机）**：

| 指标 | 修复前（mphelper） | 修复后（ubus 直连） |
|---|---|---|
| 单次状态查询延迟 | ~31ms（sh + mphelper 双 spawn） | **~0.95ms**（33 倍提升） |
| 60s loadavg 变化 | 1.57 → 2.03（+0.46 持续爬升） | 2.10 → **1.35**（回落到系统基线以下） |
| 进程 spawn（20 次采样） | 持续捕捉到 mphelper/sh | **0 次** |
| 轮询间隔 | 固定 10ms | 自适应：播放 100ms / 空闲 200ms |
| client 常驻内存 | — | RSS 1.2MB / 8 线程 |

实现要点（`playing.rs`）：
- 引入 `ubus-zero` crate（lib 名 `ubus`，features `unix`+`json`），长连接 + `lookup("mediaplayer")` + `invoke(player_get_play_status)`，blobmsg→JSON 两层解析出 `info.status`
- 监控跑在专用 std 线程（阻塞式 ubus 调用），经 `blocking_send` 桥接到 tokio 异步回调
- 生命周期：`AtomicBool` 停止标志 + 发送失败自动退出，杜绝线程泄漏
- 环境变量 `PLAYING_MONITOR=shell` 可回退旧 mphelper 路径（防未来固件 ubus 布局变化）
- 状态语义（实测）：`info.status` 0=空闲 1=播放 2=暂停 3=暂停后驻留；映射 1→Playing、2→Paused、其余→Idle，与旧版完全兼容且能捕捉到暂停瞬态

**事件驱动（方案 B）实证结论**：`ubus listen`（广播事件）与 `ubus subscribe mediaplayer`（对象通知）在完整音乐播放会话中均 **0 消息**——此固件的播放状态没有任何事件面，纯事件驱动监控不可行，自适应轮询是当前最优解。

### 🔴 P0-2 PlayingMonitor 静默死亡 bug

- **组件**：`packages/client-rust/src/services/monitor/playing.rs`（旧实现 :59）+ `client.rs` 调用处
- **作用**：同 P0-1
- **问题**：旧轮询循环里 `run_shell(...).await?` 用 `?` 传播错误——`mphelper` 偶发失败一次，整个监控任务就退出；外层 `let _ = start_monitor(...)` 吞掉错误，无任何日志。之后 `playing` 事件永久断流，打断检测失效，直到 WS 重连才会重启监控
- **修法**：去掉 `?`，单次失败沿用上次状态继续轮询；连续失败 N 次再重建任务并打日志
- **状态**：✅ 已修复（2026-11-15，随 P0-1 重写一并解决）：新实现任何错误 → 打日志 → 1s 后重建 ubus 连接；`shell` 回退路径单次失败沿用上次状态，连续 10 次才重建

### 🟠 P1-1 音频流 JSON 序列化膨胀 ~3.9 倍

- **组件**：`packages/client-rust/src/services/connect/message.rs:107-117`（`send_stream`）
- **作用**：Stream 消息承载双向二进制音频分块（上行 `record` 麦克风 PCM / 下行 `play` 播放 PCM）
- **问题**：`serde_json::to_vec(&Stream{bytes: Vec<u8>})` 把字节序列化成 `[104,101,...]` JSON 数字数组：16kHz/16bit 单声道 32KB/s → 线上 ~125KB/s，两端还要付 JSON 数字解析 CPU（服务端每秒解析 12 万+ 个数字）
- **修法**：Binary 帧自定义紧凑头：`type(1B) + tag_len(1B) + tag + payload_len(4B) + payload`。协议库两端共用（Python/Node 走同一 Rust binding），单一改动点；过渡方案 base64（1.33 倍）
- **注意**：改协议需同步验证 examples/xiaozhi、migpt、gemini 的 binding 调用路径
- **状态**：待修

### 🟠 P1-2 无音频压缩

- **组件**：`packages/client-rust/src/services/audio/`（record.rs / play.rs / config.rs）
- **作用**：`arecord` 采音上行 + `aplay` 流式播放，均裸 PCM（默认 16k/16bit/单声道）
- **问题**：双向裸 PCM ~100KB/s，WiFi 高峰期可能卡顿；Server 端也省不了带宽
- **修法**：可选 Opus（16k 单声道压到 4-6KB/s，A53 编码开销 <2% CPU）；`packages/runtime` 交叉编译镜像已自带 ARMv7 Opus 库。通过 `AudioConfig` 协商开关，默认关保持兼容
- **状态**：待修

### 🟡 P2-1 FileMonitor 10ms 轮询 + 空转等待

- **组件**：`packages/client-rust/src/services/monitor/file.rs`（文件尾部监控器，instruction/kws 事件的数据源）
- **作用**：tail `instruction.log`（官方 AIVS 指令流：ASR/TTS/Play 等）和 `kws.log`（自定义唤醒词命中）
- **问题**：
  1. 每 10ms 两次 `metadata()` stat 轮询，常态空转，阻止 CPU 深度休眠
  2. kws 未安装时 `kws.log` 不存在，`while !Path::exists()` 每 10ms 无限空转（当前机器即此状态）
  3. `metadata().unwrap()` 在 tmpfs 异常时可能 panic 掉监控任务
- **修法**：inotify 事件驱动（内核 4.9 支持，`inotify` crate），文件不存在时懒启动（kws monitor 改为首次事件触发再挂载）；`unwrap()` 换容错
- **状态**：待修

### 🟡 P2-2 二进制体积偏大

- **组件**：`packages/client-rust/Cargo.toml`（构建配置）
- **作用**：client 二进制部署在 `/data/open-xiaoai/client`，`/data` 仅剩 8.1MB
- **问题**：（初判有误）实测原版构建已含 `lto + opt-level="s" + codegen-units=1 + panic="abort" + strip`，二进制仅 **859KB**；引入 ubus 依赖后 920KB，完全无需处理
- **状态**：✅ 无需修复（原配置已优化，实测数据为准）

### 🟢 P3-1 重连策略原始

- **组件**：`packages/client-rust/src/bin/client.rs:39-55`（主循环）
- **作用**：WS 断线后自动重连 Server
- **问题**：固定 1s 间隔无限重试，无退避无抖动；Server 长时间宕机时产生无意义连接风暴（局域网影响小，公网场景差）
- **修法**：指数退避（1s 起，上限 30s）+ 随机抖动；重连日志节流
- **状态**：待修

### 🟢 P3-2 零鉴权零加密

- **组件**：`packages/client-rust/src/services/connect/`（连接层）
- **作用**：WS 通信本身
- **问题**：无认证无加密，局域网内任何主机都能伪装 Server 连上后调 `run_shell`（= 远程任意命令执行）。README 已声明仅限局域网，但缺最低限度防线
- **修法**：共享密钥握手（连接后 Server 发 challenge，Client 回 HMAC，失败即断开）；可选 wss
- **状态**：待修（局域网自用可暂缓）

### ⚪ 观察项（暂不建议改）

- `arecord`/`aplay` 子进程接法：音箱上最省事的 ALSA 方案，开销可接受，不动
- `SpeakerManager` 低频控制指令走 shell：频率低，优化无收益
- `aplay` 16k 单声道经 ALSA plug 重采样到 48k 双声道 dmixer：有轻微 CPU 开销，但改 48k 直出需要服务端配合重采样，收益小，暂不动
- `rpc.rs` 10s 默认超时 / 32 并发信号量：现网无瓶颈

---

## 已完成的真机部署准备

- [x] WSL(Ubuntu-24.04) → 音箱免密 SSH（RSA 密钥，Dropbear v2017.75 不支持 ed25519）
- [x] 公钥持久化：`/data/dropbear/authorized_keys` + bind mount 到 `/etc/dropbear/`，已写入 `/data/init.sh` 开机生效
- [x] scp 不可用（无 sftp-server），传文件用管道：`cat file | ssh xiaoai "cat > /path"`
- [x] 部署目录：`/data/open-xiaoai/`（client + server.txt + ubus_probe），P0 修复版已在真机运行验证
- [x] 测试 server：`tools/test-server.py`（Windows Python + websockets，端口 4399，收事件/流统计/RPC ping）
- [x] WSL 编译副本 `~/open-xiaoai/client-rust`（rsync 自 /mnt/c 后 docker 构建，避免 9p I/O 慢）

## 音箱端实测备忘

- `mphelper` 是 `/usr/bin/mphelper` **shell 脚本**：`mute_stat` = `ubus call mediaplayer player_get_play_status` + jshn 两层解析，透传 `info.status`
- TTS（`tts_play.sh`）播放**不进入** mediaplayer 播放状态（status 始终 0）；云端音乐才是 status=1
- `mibrain ai_service`（`ask_xiaoai`）可脚本化触发原生对话/播放，是自动化测试入口
- busybox `timeout`/`date +%N` 行为异常：计时用 `/usr/bin/time -f "%e"`，脚本传文件执行避免多层引号转义
- `ps | grep xxx` 会自匹配：用 `grep -c "xxx[r]"` 防污染

## 交叉编译命令（改码后重新构建）

```bash
# 在 WSL 内，用 packages/runtime 镜像交叉编译（ARMv7 + glibc 2.25）
cd /path/to/open-xiaoai/packages/client-rust
docker run --rm -v "$PWD:/app" idootop/open-xiaoai-runtime:oh2p \
    cargo build --target armv7-unknown-linux-gnueabihf --release
# 产物 target/armv7-unknown-linux-gnueabihf/release/client
# 部署：cat client | ssh xiaoai "cat > /data/open-xiaoai/client && chmod +x /data/open-xiaoai/client"
```
