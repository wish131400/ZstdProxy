# zstd-server-jar

Standalone Java server for ZstdProxy client side.

## Build

```powershell
..\gradle-8.8\bin\gradle.bat clean build
```

Output jar:

- `build/libs/zstd-server-jar-1.0.0-all.jar` (recommended, includes dependencies)

## Run

First run (auto-generate config and exit):

```powershell
java -jar build/libs/zstd-server-jar-1.0.0-all.jar
```

Then edit generated `zstd-server.properties`, especially:

- `listen`
- `target`

Run again:

```powershell
java -jar build/libs/zstd-server-jar-1.0.0-all.jar --config zstd-server.properties
```
