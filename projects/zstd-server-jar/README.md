# zstd-server-jar

Standalone Java server for ZstdProxy client side.

## Platform Support

- Windows: supported (x64/arm64, requires Java 17+)
- Linux: supported (x64/arm64, requires Java 17+)
- macOS: supported (Intel/Apple Silicon, requires Java 17+)

## Build

### Windows (PowerShell)

```powershell
..\gradle-8.8\bin\gradle.bat clean build
```

### Linux / macOS (bash)

```bash
../gradle-8.8/bin/gradle clean build
```

Output jar:

- `build/libs/zstd-server-jar-1.3.0-all.jar` (recommended, includes dependencies)

## Run

### Step 1: First run (generate config and exit)

#### Windows

```powershell
java -jar build/libs/zstd-server-jar-1.3.0-all.jar
```

#### Linux / macOS

```bash
java -jar build/libs/zstd-server-jar-1.3.0-all.jar
```

This creates `zstd-server.properties`.

### Step 2: Edit config

At minimum, set:

- `listen`
- `target`

### Step 3: Start with config

#### Windows

```powershell
java -jar build/libs/zstd-server-jar-1.3.0-all.jar --config zstd-server.properties
```

#### Linux / macOS

```bash
java -jar build/libs/zstd-server-jar-1.3.0-all.jar --config zstd-server.properties
```

## Notes

- `listen` and `target` must not be the same endpoint.
- Do not use trailing dot host such as `127.0.0.1.`.
- If a non-zstd client connects to zstd port, `Unknown frame descriptor` may appear in logs.
