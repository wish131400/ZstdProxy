# zstdproxy-forge-src

Source-maintainable Forge 1.20.1 project for zstdproxy.

## What this project does
- Loads server definitions from `servers.zstd.json` in gameDir.
- Starts local loopback zstd proxies using `github.luben:zstd-jni:1.5.5-10`.
- Publishes generated local addresses into multiplayer server list with suffix `[zstd]`.
- On dedicated server, can run an embedded zstd forwarder from `config/zstdproxy-server.properties`.

## Build
- Java 17+
- Gradle (ForgeGradle)

```
./gradlew build
```

Output jar will be in `build/libs/`.

## Config
Forge client config file `config/zstdproxy-client.toml`:
- `url` - optional HTTP(S) json endpoint, empty uses local file.
- `level` - zstd compression level (1-22).

Forge dedicated-server config file `config/zstdproxy-server.properties`:
- Auto-generated on first dedicated-server start.
- You must set `enabled=true` to activate.
- Important fields:
  - `listen` - proxy public bind address.
  - `target` - backend MC/Velocity server address.
  - `level` - zstd level for MC -> client.
  - `max_conn_per_ip`, `max_req_per_window`, `request_window`, `ban_duration`.

## JSON format
```
{
  "servers": [
    {
      "name": "Line 1",
      "addr": "example.com:25570",
      "mask": "server1",
      "icon": "base64_png_optional"
    }
  ]
}
```
