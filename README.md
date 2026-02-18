# zstdproxyMC

基于 Zstd 的 Minecraft TCP 代理项目集合，目标是在不修改MC本体的前提下显著降低服务器带宽占用。
本仓库是基于noforge1.21.1的zstdproxy https://github.com/MeguminKato/ZstdProxy 修改而来，目前本仓库仅有forge1.20.1版本，如果需要，可以考虑移植到其他版本。

## 压缩效果

根据实测，在齿轮盛宴整合包（`www.齿轮盛宴.com`）这类机械动力为主的科技整合包场景中，压缩率通常可达到 70%~90%。
在没有大量实体的环境下带宽也可以降低约20%，综合压缩效果大约40%-90%

- 视频演示：<https://www.bilibili.com/video/BV1D4Z9B2EL3/?vd_source=9ea20aff4b6c678542669bfb7d5bb4e5>

## 第一次使用

### 版本选用

如果你的客户端/服务器有[DH]Distant Horizons，那么请使用DHFIX版本，如果没有，请使用All版本，由于本模组和[DH]Distant Horizons使用了相同的依赖库，在加载时会导致崩溃。
本模组经过测试还支持Velocity端，使用Velocity端请搭配独立服务端代理程序，客户端使用jar mod，独立服务端代理程序可参考下面的-独立服务端/客户端（跨平台）。

### 客户端（Forge 模组，本模组为客户端服务端通用模组）

首次启动游戏后，会在整合包根目录自动生成 `servers.zstd.json`。

你需要手动填写 zstd 代理地址（不是 MC 服务端直连地址），保存后等待 `2~5` 秒，在游戏多人列表点击刷新即可看到配置的 zstd 线路。

### 服务端（Forge 模组，本模组为客户端服务端通用模组）

首次启动后，会在 `config` 目录生成 `zstdproxy-server.properties`。

常用配置项如下：

| 配置项 | 含义 |
| --- | --- |
| `enabled=false` | 是否启用内置 zstd 服务端代理 |
| `listen=0.0.0.0:9000` | zstd 客户端连接的公共监听地址 |
| `target=127.0.0.1:25565` | 后端 Minecraft目标地址 |
| `level=7` | 后端到客户端方向的 zstd 压缩级别 |
| `max_conn_per_ip=20` | 每个源 IP 最大并发连接数 |
| `max_req_per_window=30` | 每个源 IP 在窗口期内最大请求次数 |
| `ban_duration=30m` | 超限后封禁时长 |
| `stats_interval=1s` | 统计日志输出间隔 |

注:如果窗口期和连接数设置不当,在客户端大量刷新MOTD的情况下有可能会被封，如使用FRP,haproxy等转发软件的,请打开proxyprotocolv2协议，否则IP获取不正确可能导致封禁整条线路
## 项目组成

- `projects/zstdproxy-forge`  
  Forge 1.20.1 模组（客户端 + 服务端）。客户端读取 `servers.zstd.json` 并生成 `[zstd]` 服务器入口；服务端可作为内置 zstd 代理入口。
- `projects/zstd-server-jar`  
  独立 Java 服务端代理（可执行 jar），不依赖 Forge 运行。
- `projects/gozstdserver`  
  独立 Go 服务端代理实现，包含完整 zstd 压缩/解压流程，可单独运行。

## 工作原理

### 1. 客户端侧（Forge 模组）

- 启动后读取 `servers.zstd.json`（或配置 URL）
- 为每条线路启动本地 loopback 代理端口（`127.0.0.1:随机端口`）
- 将多人列表中的线路替换/追加为本地地址
- 游戏连接本地端口后：
  - 客户端 -> 远端：zstd 压缩
  - 远端 -> 客户端：zstd 解压并回灌给 Minecraft

### 2. 服务端侧（Forge 内置代理 / 独立代理）

- 监听公网入口端口 `listen`
- 将流量转发到后端 MC/Velocity 端口 `target`
- 转发方向：
  - 客户端 -> 后端：zstd 解压
  - 后端 -> 客户端：zstd 压缩
- 提供基础防护：并发限制、请求速率窗口、封禁时长
- 输出实时统计：`Raw / Zstd / Ratio / Conns`

### 3. 数据流示意

`MC客户端(zstdproxy) <-> FRP/HaProxy Server(可能存在) <-> FRP Client(可能存在) <-> GoZstdServer <-> MC服务端`

## 关键配置

- 客户端：`servers.zstd.json`、`config/zstdproxy-client.toml`
- 服务端：`config/zstdproxy-server.properties`

建议：

- `listen` 与 `target` 必须是不同端口/端点
- zstd 入口端口仅开放给安装 zstd 客户端模组的玩家
- 非 zstd 客户端探测该端口时，日志出现 `Unknown frame descriptor` 属正常

## 注意事项

- 在线模式（正版验证）下，登录后链路会加密，压缩收益通常偏低
- 此时 `Ratio` 接近 `100%` 甚至略高属于常见现象，不代表代理未工作

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

## 独立服务端/客户端（跨平台）

以下命令适用于独立程序（`GoZstdServer*`）。
独立服务端程序往往是和客户端的jar一起使用，服务器使用独立程序，客户端使用jar mod，这样可以自动生成服务器地址。
由于这个工具的特性，它不仅可以代理MC，也可以代理使用tcp协议的其他游戏服务器。

#### Windows

```powershell
cd C:\Users\Administrator\Desktop\zstd
.\GoZstdServer.exe -mode server -l 0.0.0.0:9000 -r 127.0.0.1:25565 -L 3
```

#### Linux (amd64)

```bash
cd /path/to/zstd
chmod +x ./GoZstdServer-linux-amd64
./GoZstdServer-linux-amd64 -mode server -l 0.0.0.0:9000 -r 127.0.0.1:25565 -L 3
```

#### Linux (arm64)

```bash
cd /path/to/zstd
chmod +x ./GoZstdServer-linux-arm64
./GoZstdServer-linux-arm64 -mode server -l 0.0.0.0:9000 -r 127.0.0.1:25565 -L 3
```

#### macOS (Apple Silicon)

```bash
cd /path/to/zstd
chmod +x ./GoZstdServer-macos-arm64
./GoZstdServer-macos-arm64 -mode server -l 0.0.0.0:9000 -r 127.0.0.1:25565 -level 3
```

### 第三方依赖说明

本项目依赖多个第三方组件（如 Forge、`zstd-jni` 等），使用与分发时需遵守其各自许可证条款。

> 本仓库协议仅覆盖本仓库自有代码，不替代第三方依赖许可证。


## 许可证

该项目采用MIT许可证授权。详情请参见LICENSE。



