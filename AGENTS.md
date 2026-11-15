# AGENTS.md

Open-XiaoAI：接管小爱音箱 Pro（LX06）/ Xiaomi 智能音箱 Pro（OH2P）的「耳朵」和「嘴巴」，通过 WebSocket 将麦克风音频流和事件转发到自定义 Server 端，由多模态大模型 / Agent 接管对话。**项目已停止维护**（见 README 顶部公告）。仅支持 LX06 和 OH2P 两个机型，其他型号不可用。

仓库语言：文档、注释、commit message 均为中文，请保持一致。

## 目录结构

- `packages/client-patch/` — 补丁固件制作工具（TypeScript + Bash + Python）。用小米账号登录获取 OTA 固件 → unsquashfs 解包 → 应用 `patches/` 下的补丁 → 重新打包 squashfs。`.env`（从 `.env.example` 复制）配置小米账号和 SSH 密码。
- `packages/client-rust/` — 运行在小爱音箱上的 Client 端补丁程序（Rust，crate 名 `open-xiaoai`）。只做转发和被动响应，不含业务逻辑。
- `packages/flash-tool/` — 刷机工具（把 patched squashfs 刷入音箱）。
- `packages/runtime/` — Docker 交叉编译（armv7-unknown-linux-gnueabihf）+ QEMU 用户态模拟环境。音箱 glibc 仅 2.25，需 Linaro GCC 7.5.0 工具链。
- `examples/` — Server 端演示：`xiaozhi`（Python/uv）、`gemini`（Python + pyo3）、`migpt`（Node/pnpm + napi）、`kws`（自定义唤醒词）、`stereo`（组立体声，纯 Rust）。
- `docs/flash.md` — 刷机教程，改动固件/刷机相关代码前必读。

## 常用命令

```bash
# Client 端（需 cross + Docker）
cd packages/client-rust
cross build --release --target armv7-unknown-linux-gnueabihf

# 补丁固件（Windows 须在 Git Bash 运行）
cd packages/client-patch
npm install && npm run build        # 或用 Docker：见 package README

# 示例（以 migpt 为例）
cd examples/migpt && pnpm install && pnpm dev

# 交叉编译/模拟环境
cd packages/runtime && make build / make run-x86 / make run-armv7
```

CI（`.github/workflows/`）：`compile-client-rust.yml` 编译 Client；`LX06.yaml`/`OH2P.yaml` 按机型打包固件。

## 核心架构与协议

**Client-Server 分工**：音箱端算力/内存极有限，识别和业务逻辑全部放 Server 端；Client（Rust）只转发音频流、上报事件、响应 RPC。Rust 协议库通过 binding 复用到 Server 端：Python 用 pyo3（cdylib `open_xiaoai_server`），Node 用 napi（产物 `open-xiaoai.node`），examples 里 Cargo.toml 以 path 依赖 `../../packages/client-rust`。

**通信协议**（`packages/client-rust/src/services/connect/data.rs`）：单个 WebSocket 连接（默认端口 4399）：
- Text 帧 JSON：`Request`/`Response`（Server → Client 的 RPC，id 关联；Client 命令：`get_version`、`run_shell`、`start_play`、`stop_play`、`start_recording`、`stop_recording`）、`Event`（Client → Server 上报 `instruction` / `playing` / `kws`）。
- Binary 帧 = Stream 音频分块（tag `record` 上行麦克风 PCM / `play` 下行播放 PCM）：**v1 二进制帧格式**（首字节 0x01，见 `message.rs` 注释；接收端兼容识别旧 JSON 帧——首字节 `{`），勿再用 serde_json 序列化 `Stream.bytes`。`AudioConfig.codec = "opus"` 可启用 Opus 压缩（Voip/24kbps/20ms 帧，`src/services/audio/codec.rs`，默认 None=PCM 兼容旧版）。

**Client 端监控原理**（`src/services/monitor/`）：
- `instruction`：tail 音箱自带 AIVS 日志 `/tmp/mico_aivs_lab/instruction.log`（每行一条 JSON，含 ASR 识别结果、Speak/Play 指令等）。
- `playing`：原生 ubus socket（`ubus-zero` crate）长连接轮询 `mediaplayer.player_get_play_status`，自适应间隔（播放 100ms/空闲 200ms），零进程 spawn；环境变量 `PLAYING_MONITOR=shell` 回退旧 mphelper 路径。详见 `docs/client-optimization.md`（P0-1 实测数据与状态语义 0/1/2/3）。
- `kws`：tail `/tmp/open-xiaoai/kws.log`，格式 `timestamp@keyword`，`__STARTED__` 表示唤醒引擎就绪。
- 文件监控（file.rs）为 inotify + 2s 安全兜底轮询双模：**LX06 4.9 内核的 fsnotify 会间歇性整体失效**（inotifyd 也收不到事件），勿改成纯 inotify 依赖。
- 录音：spawn 系统 `arecord` 子进程读 stdout；播放：收到的流写入 `aplay` 子进程 stdin。

**固件补丁**（`packages/client-patch/patches/`）：`01-ssh`（固化 SSH）、`02-login`（root 密码，`{SSH_PASSWORD}` 占位符在 patch.sh 中替换，默认 open-xiaoai）、`03-ota`（禁自动更新）、`04-start`（rc.local 挂载 `/data/init.sh` 开机启动钩子）、`LX06/`（机型专属音频补丁）。补丁按文件名顺序应用，`.patch` 走 `patch -p1`，`.sh` 直接执行。

## 注意事项

- 音箱端二进制必须为 ARMv7 (armhf) + glibc 2.25 兼容，用 `packages/runtime` 的 Docker 环境编译，不要用本机工具链。
- 系统更新后补丁失效需重新刷机；`/data` 分区在刷机后保留，业务程序放 `/data/open-xiaoai/`。
- Client 启动参数为 server 地址（`client ws://ip:4399`），断线后每秒自动重连；`run_shell` RPC 可执行任意脚本，切勿连接不可信 server。
- 无加密/鉴权/音频压缩，仅限局域网测试。
- examples 各自独立（Python 用 uv、Node 用 pnpm），修改协议时需同步检查各 example 的 binding 是否受影响。
