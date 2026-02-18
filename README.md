# zstdproxyMC

基于 Zstd 的 Minecraft TCP 代理项目集合，目标是尽量以低侵入方式给客户端与服务端链路增加可选压缩能力。

## 项目组成

- `projects/zstdproxy-forge`
  Forge 1.20.1 模组（客户端 + 服务端）。
  客户端读取 `servers.zstd.json` 并自动生成 `[zstd]` 服务器入口；服务端可作为内置 zstd 代理入口。

- `projects/zstd-server-jar`
  独立 Java 服务端代理（可执行 jar），不依赖 Forge 运行。

- `projects/gozstdserver`
  独立 Go 服务端代理实现它有完整的 zstd 压缩解压实现，可独立运行。

## 工作原理

### 1. 客户端侧（Forge 模组）

- 启动后读取 `servers.zstd.json`（或配置 URL）
- 为每条线路在本地起一个 loopback 代理端口（`127.0.0.1:随机端口`）
- 将游戏多人列表中的条目替换/追加为本地地址
- 游戏连本地端口后：
  - 客户端 -> 远端：使用 zstd 压缩后发送
  - 远端 -> 客户端：对 zstd 流解压后回灌给 Minecraft

### 2. 服务端侧（Forge 内置代理 / 独立代理）

- 监听公网入口端口 `listen`
- 将流量转发到后端实际 MC/Velocity 端口 `target`
- 转发方向：
  - 客户端 -> 后端：zstd 解压
  - 后端 -> 客户端：zstd 压缩
- 提供基础防护：并发限制、请求速率窗口、封禁时长
- 输出实时统计：`Raw / Zstd / Ratio / Conns`

### 3. 数据流示意

`Minecraft Client -> (zstdproxy client) -> Zstd Tunnel -> (zstdproxy server) -> Backend MC/Velocity`

## 关键配置

- 客户端：`servers.zstd.json`、`config/zstdproxy-client.toml`
- 服务端：`config/zstdproxy-server.properties`

建议：

- `listen` 与 `target` 必须不同端口
- zstd 入口端口仅给安装 zstd 客户端的玩家使用
- 非 zstd 探针/普通客户端打到 zstd 端口会出现 `Unknown frame descriptor`

## 已知限制

- 在线模式（正版验证）下，登录后链路被加密，压缩收益通常会很低
- 在该场景中 `Ratio` 接近 `100%` 甚至略高是常见现象，不代表代理未工作

## 构建

### Forge 模组

```powershell
cd projects\zstdproxy-forge
.\gradlew.bat build
```

### Java 独立服务端

```powershell
cd projects\zstd-server-jar
gradle build
```

### Go 独立服务端

```powershell
cd projects\gozstdserver
go build .
```

## 开源协议

### 本仓库协议（建议）

建议为本仓库采用 **MIT License**（宽松、易分发、适合整合包二次发布场景）。

### 第三方依赖说明

本项目依赖多个第三方组件（如 Forge、zstd-jni 等），使用与分发时需要遵守其各自许可证条款。

> 注意：本仓库协议只覆盖本仓库自有代码，不替代第三方依赖许可证。
