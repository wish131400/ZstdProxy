package cn.tohsaka.factory.zstdproxy.server;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.mojang.logging.LogUtils;
import net.minecraftforge.fml.loading.FMLPaths;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

final class ServerProxyRuntime {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final byte[] PROXY_V2_SIGNATURE = new byte[]{
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
    };

    private final Object lifecycleLock = new Object();

    private volatile boolean running;
    private ServerSocket listener;
    private Thread acceptThread;
    private ExecutorService workers;
    private ScheduledExecutorService statsTicker;
    private FloodGuard guard;
    private TrafficStats stats;
    private ProxyConfig cfg;

    void start(int mcServerPort) {
        synchronized (lifecycleLock) {
            if (running) {
                return;
            }

            Path configPath = FMLPaths.GAMEDIR.get().resolve("config").resolve("zstdproxy-server.properties");
            ProxyConfig loaded = loadOrCreateConfig(configPath, mcServerPort);
            if (loaded == null) {
                return;
            }
            if (!loaded.enabled) {
                LOGGER.warn("[zstdproxy-server] config exists but enabled=false, skip start. File: {}", configPath);
                LOGGER.warn("[zstdproxy-server] set enabled=true after checking listen/target.");
                return;
            }

            try {
                listener = new ServerSocket();
                listener.bind(loaded.listen.toAddress());
            } catch (IOException e) {
                LOGGER.error("[zstdproxy-server] bind failed on {}: {}", loaded.listen, e.toString());
                closeQuietly(listener);
                listener = null;
                return;
            }

            this.cfg = loaded;
            this.stats = new TrafficStats();
            this.guard = new FloodGuard(loaded);
            this.workers = Executors.newCachedThreadPool(new NamedFactory("zstdsrv-worker"));
            this.statsTicker = Executors.newSingleThreadScheduledExecutor(new NamedFactory("zstdsrv-stats"));
            this.running = true;

            startStatsPrinter();
            acceptThread = new Thread(this::acceptLoop, "zstdsrv-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();

            LOGGER.info("GoZstdServer started: listen={} target={} mode=server", loaded.listen, loaded.target);
            LOGGER.info("guard: max_conn={} max_req={} window={} ban={}",
                loaded.maxConnPerIp, loaded.maxReqPerWindow, loaded.window, loaded.banDuration);
        }
    }

    void stop() {
        synchronized (lifecycleLock) {
            if (!running) {
                return;
            }
            running = false;
            closeQuietly(listener);
            listener = null;
            if (acceptThread != null) {
                try {
                    acceptThread.join(1000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            acceptThread = null;
            shutdownQuietly(statsTicker);
            shutdownQuietly(workers);
            statsTicker = null;
            workers = null;
            guard = null;
            stats = null;
            cfg = null;
            LOGGER.info("[zstdproxy-server] stopped");
        }
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = listener.accept();
                workers.execute(() -> handleClient(client));
            } catch (IOException e) {
                if (running) {
                    LOGGER.warn("[zstdproxy-server] accept error: {}", e.toString());
                }
            } catch (Exception e) {
                if (running) {
                    LOGGER.warn("[zstdproxy-server] accept unexpected error: {}", e.toString());
                }
            }
        }
    }

    private void handleClient(Socket client) {
        stats.addConn(1);
        String remoteIp = sourceIp(client.getRemoteSocketAddress());
        String sourceIp = remoteIp;

        try (Socket clientSocket = client) {
            PushbackInputStream pushIn = new PushbackInputStream(clientSocket.getInputStream(), 16);
            ProxyInfo proxyInfo = parseProxyProtocolV2(pushIn);
            if (proxyInfo.valid && proxyInfo.sourceIp != null && !proxyInfo.sourceIp.isBlank()) {
                sourceIp = proxyInfo.sourceIp;
            }

            if (!guard.begin(sourceIp)) {
                LOGGER.warn("[server] blocked {} by flood guard", sourceIp);
                return;
            }

            try (Socket upstream = new Socket()) {
                try {
                    upstream.connect(cfg.target.toAddress(), 5000);
                } catch (IOException dialErr) {
                    LOGGER.warn("[server] Remote {} Connect Error: {}", clientSocket.getRemoteSocketAddress(), dialErr.getMessage());
                    return;
                }

                upstream.setTcpNoDelay(true);
                clientSocket.setTcpNoDelay(true);

                Future<Exception> c2s = workers.submit(() -> {
                    try {
                        forwardDecompress(upstream, pushIn, stats);
                        return null;
                    } catch (Exception ex) {
                        return ex;
                    } finally {
                        closeWrite(upstream);
                    }
                });

                Future<Exception> s2c = workers.submit(() -> {
                    try {
                        forwardCompress(clientSocket.getOutputStream(), upstream, cfg.level, stats);
                        return null;
                    } catch (Exception ex) {
                        return ex;
                    } finally {
                        closeWrite(clientSocket);
                    }
                });

                Exception err1 = c2s.get();
                Exception err2 = s2c.get();
                if (isRealPipeErr(err1)) {
                    LOGGER.warn("pipe error source={} dir=client->mc: {}", sourceIp, err1.toString());
                }
                if (isRealPipeErr(err2)) {
                    LOGGER.warn("pipe error source={} dir=mc->client: {}", sourceIp, err2.toString());
                }
            } finally {
                guard.end(sourceIp);
            }
        } catch (Exception ex) {
            if (isRealPipeErr(ex)) {
                LOGGER.warn("[server] connection error source={} remote={}: {}", sourceIp, remoteIp, ex.toString());
            }
        } finally {
            stats.addConn(-1);
        }
    }

    private void forwardDecompress(Socket dst, InputStream src, TrafficStats stats) throws IOException {
        try (ZstdInputStream zstdIn = new ZstdInputStream(new CountingInputStream(src, stats::addZstd))) {
            OutputStream dstOut = dst.getOutputStream();
            byte[] buf = new byte[16 * 1024];
            int n;
            while ((n = zstdIn.read(buf)) >= 0) {
                if (n > 0) {
                    dstOut.write(buf, 0, n);
                    stats.addRaw(n);
                }
            }
        }
    }

    private void forwardCompress(OutputStream dst, Socket src, int level, TrafficStats stats) throws IOException {
        try (ZstdOutputStream zstdOut = new ZstdOutputStream(new CountingOutputStream(dst, stats::addZstd), level)) {
            zstdOut.setCloseFrameOnFlush(false);
            InputStream srcIn = src.getInputStream();
            byte[] buf = new byte[16 * 1024];
            int n;
            while ((n = srcIn.read(buf)) >= 0) {
                if (n > 0) {
                    stats.addRaw(n);
                    zstdOut.write(buf, 0, n);
                    zstdOut.flush();
                }
            }
        }
    }

    private ProxyInfo parseProxyProtocolV2(PushbackInputStream in) throws IOException {
        byte[] first = new byte[PROXY_V2_SIGNATURE.length];
        int n = readSome(in, first);
        if (n < 0) {
            return ProxyInfo.invalid();
        }
        if (n < PROXY_V2_SIGNATURE.length) {
            in.unread(first, 0, n);
            return ProxyInfo.invalid();
        }
        if (!Arrays.equals(first, PROXY_V2_SIGNATURE)) {
            in.unread(first);
            return ProxyInfo.invalid();
        }

        byte[] fixed = readFully(in, 4);
        int verCmd = fixed[0] & 0xFF;
        int famProto = fixed[1] & 0xFF;
        int payloadLen = ((fixed[2] & 0xFF) << 8) | (fixed[3] & 0xFF);
        byte[] payload = readFully(in, payloadLen);

        int version = (verCmd & 0xF0) >> 4;
        int command = verCmd & 0x0F;
        int family = (famProto & 0xF0) >> 4;
        int protocol = famProto & 0x0F;

        if (version != 0x2 || command != 0x1 || protocol != 0x1) {
            return ProxyInfo.invalid();
        }

        if (family == 0x1 && payload.length >= 12) {
            String srcIp = ipString(payload, 0, 4);
            String dstIp = ipString(payload, 4, 4);
            int srcPort = u16(payload, 8);
            int dstPort = u16(payload, 10);
            return new ProxyInfo(true, srcIp, srcPort, dstIp, dstPort);
        }
        if (family == 0x2 && payload.length >= 36) {
            String srcIp = ipString(payload, 0, 16);
            String dstIp = ipString(payload, 16, 16);
            int srcPort = u16(payload, 32);
            int dstPort = u16(payload, 34);
            return new ProxyInfo(true, srcIp, srcPort, dstIp, dstPort);
        }
        return ProxyInfo.invalid();
    }

    private int readSome(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n < 0) {
                return off == 0 ? -1 : off;
            }
            off += n;
            if (n == 0) {
                break;
            }
        }
        return off;
    }

    private byte[] readFully(InputStream in, int len) throws IOException {
        byte[] out = new byte[len];
        int off = 0;
        while (off < len) {
            int n = in.read(out, off, len - off);
            if (n < 0) {
                throw new EOFException("unexpected EOF");
            }
            off += n;
        }
        return out;
    }

    private String ipString(byte[] data, int offset, int len) throws IOException {
        byte[] raw = Arrays.copyOfRange(data, offset, offset + len);
        return InetAddress.getByAddress(raw).getHostAddress();
    }

    private int u16(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private void startStatsPrinter() {
        long periodMs = Math.max(250L, cfg.statsInterval.toMillis());
        AtomicLong prevRaw = new AtomicLong();
        AtomicLong prevZstd = new AtomicLong();

        statsTicker.scheduleAtFixedRate(() -> {
            long raw = stats.rawBytes.get();
            long zstd = stats.zstdBytes.get();
            int conns = stats.activeConn.get();

            long dr = raw - prevRaw.getAndSet(raw);
            long dz = zstd - prevZstd.getAndSet(zstd);
            long rawPerSec = (long) (dr * (1000.0 / periodMs));
            long zstdPerSec = (long) (dz * (1000.0 / periodMs));
            double ratio = raw <= 0 ? 0.0 : ((double) zstd * 100.0 / (double) raw);

            String now = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            LOGGER.info("[{}] Raw: {} ({}) | Zstd: {} ({}) | Ratio: {}% | Conns: {}",
                now,
                formatSize(raw),
                formatRate(rawPerSec),
                formatSize(zstd),
                formatRate(zstdPerSec),
                String.format(Locale.ROOT, "%.2f", ratio),
                conns);
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        String[] units = {"KB", "MB", "GB", "TB"};
        double v = bytes / 1024.0;
        int idx = 0;
        while (v >= 1024.0 && idx < units.length - 1) {
            v /= 1024.0;
            idx++;
        }
        return String.format(Locale.ROOT, "%.2f %s", v, units[idx]);
    }

    private String formatRate(long bytesPerSec) {
        if (bytesPerSec < 1024) {
            return bytesPerSec + "B/s";
        }
        String[] units = {"KB/s", "MB/s", "GB/s", "TB/s"};
        double v = bytesPerSec / 1024.0;
        int idx = 0;
        while (v >= 1024.0 && idx < units.length - 1) {
            v /= 1024.0;
            idx++;
        }
        return String.format(Locale.ROOT, "%.1f%s", v, units[idx]);
    }

    private ProxyConfig loadOrCreateConfig(Path path, int mcServerPort) {
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path.getParent());
                String body = """
                    # ------------------------------------------------------------
                    # zstdproxy server config (Forge mod, auto-generated)
                    # ------------------------------------------------------------
                    # IMPORTANT:
                    # 1) Set enabled=true after checking listen/target.
                    # 2) listen and target must NOT be the same endpoint.
                    # 3) Do not use host with trailing dot, e.g. 127.0.0.1.
                    # ------------------------------------------------------------

                    # Enable built-in zstd proxy on dedicated server.
                    # false = disabled (safe default), true = start proxy.
                    enabled=false

                    # Public bind address for zstd clients to connect.
                    # Usually 0.0.0.0:<public_port>.
                    listen=0.0.0.0:35566

                    # Backend Minecraft/Velocity target.
                    # Usually local backend, e.g. 127.0.0.1:25565.
                    target=127.0.0.1:${PORT}

                    # Zstd compression level for backend -> client stream.
                    # Range: 1..22, recommended: 3..9.
                    level=7

                    # Flood guard: max concurrent connections per source IP.
                    # Set <=0 to disable this limit.
                    max_conn_per_ip=20

                    # Flood guard: max connection attempts per source IP in request_window.
                    # Set <=0 to disable this limit.
                    max_req_per_window=30

                    # Flood guard request window.
                    # Supported suffix: ms, s, m, h, d.
                    request_window=10s

                    # Ban duration after exceeding rate limit.
                    # Supported suffix: ms, s, m, h, d.
                    ban_duration=30m

                    # Stats print interval.
                    # Supported suffix: ms, s, m, h, d.
                    stats_interval=1s
                    """.replace("${PORT}", String.valueOf(mcServerPort > 0 ? mcServerPort : 25565));
                Files.writeString(path, body, StandardCharsets.UTF_8);
                LOGGER.warn("[zstdproxy-server] generated config: {}", path);
                LOGGER.warn("[zstdproxy-server] please edit config and set enabled=true, then restart server.");
            } catch (IOException e) {
                LOGGER.error("[zstdproxy-server] failed to create config {}: {}", path, e.toString());
            }
            return null;
        }

        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            props.load(in);
        } catch (IOException e) {
            LOGGER.error("[zstdproxy-server] failed reading config {}: {}", path, e.toString());
            return null;
        }

        boolean enabled = Boolean.parseBoolean(props.getProperty("enabled", "false").trim());
        HostPort listen = HostPort.parse(props.getProperty("listen", "0.0.0.0:35566"));
        HostPort target = HostPort.parse(props.getProperty("target", "127.0.0.1:" + (mcServerPort > 0 ? mcServerPort : 25565)));

        int level = clamp(parseInt(props.getProperty("level"), 7), 1, 22);
        int maxConn = parseInt(props.getProperty("max_conn_per_ip"), 20);
        int maxReq = parseInt(props.getProperty("max_req_per_window"), 30);
        Duration window = parseDuration(props.getProperty("request_window"), Duration.ofSeconds(10));
        Duration ban = parseDuration(props.getProperty("ban_duration"), Duration.ofMinutes(30));
        Duration statsInterval = parseDuration(props.getProperty("stats_interval"), Duration.ofSeconds(1));
        if (statsInterval.isZero() || statsInterval.isNegative()) {
            statsInterval = Duration.ofSeconds(1);
        }

        return new ProxyConfig(enabled, listen, target, level, maxConn, maxReq, window, ban, statsInterval);
    }

    private int parseInt(String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private Duration parseDuration(String raw, Duration fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String text = raw.trim().toLowerCase(Locale.ROOT);
        try {
            if (text.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2)));
            }
            if (text.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            if (text.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            if (text.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            if (text.endsWith("d")) {
                return Duration.ofDays(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            return Duration.ofSeconds(Long.parseLong(text));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private String sourceIp(SocketAddress address) {
        if (address instanceof InetSocketAddress inet) {
            InetAddress ip = inet.getAddress();
            return ip != null ? ip.getHostAddress() : inet.getHostString();
        }
        return String.valueOf(address);
    }

    private boolean isRealPipeErr(Exception err) {
        if (err == null || err instanceof EOFException) {
            return false;
        }
        String msg = err.toString().toLowerCase(Locale.ROOT);
        return !(msg.contains("broken pipe") || msg.contains("connection reset") || msg.contains("socket closed"));
    }

    private void closeWrite(Socket socket) {
        try {
            socket.shutdownOutput();
        } catch (Exception ignored) {
        }
    }

    private void closeQuietly(ServerSocket socket) {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception ignored) {
        }
    }

    private void shutdownQuietly(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdownNow();
    }

    private record ProxyInfo(boolean valid, String sourceIp, int sourcePort, String targetIp, int targetPort) {
        static ProxyInfo invalid() {
            return new ProxyInfo(false, null, 0, null, 0);
        }
    }

    private record ProxyConfig(
        boolean enabled,
        HostPort listen,
        HostPort target,
        int level,
        int maxConnPerIp,
        int maxReqPerWindow,
        Duration window,
        Duration banDuration,
        Duration statsInterval
    ) {
    }

    private record HostPort(String host, int port) {
        static HostPort parse(String raw) {
            if (raw == null || raw.isBlank()) {
                throw new IllegalArgumentException("empty host:port");
            }
            String value = raw.trim();

            if (value.startsWith("[") && value.contains("]")) {
                int end = value.indexOf(']');
                String host = value.substring(1, end);
                int port = 25565;
                if (end + 1 < value.length() && value.charAt(end + 1) == ':') {
                    port = Integer.parseInt(value.substring(end + 2).trim());
                }
                return new HostPort(normalizeHost(host), port);
            }

            int lastColon = value.lastIndexOf(':');
            int firstColon = value.indexOf(':');
            if (lastColon > 0 && firstColon == lastColon) {
                String host = value.substring(0, lastColon).trim();
                int port = Integer.parseInt(value.substring(lastColon + 1).trim());
                return new HostPort(normalizeHost(host), port);
            }
            return new HostPort(normalizeHost(value), 25565);
        }

        InetSocketAddress toAddress() {
            return new InetSocketAddress(host, port);
        }

        private static String normalizeHost(String host) {
            String h = host.trim();
            if (h.endsWith(".") && h.length() > 1) {
                h = h.substring(0, h.length() - 1);
            }
            return h;
        }
    }

    private static final class TrafficStats {
        private final AtomicLong rawBytes = new AtomicLong();
        private final AtomicLong zstdBytes = new AtomicLong();
        private final AtomicInteger activeConn = new AtomicInteger();

        private void addRaw(long n) {
            if (n > 0) {
                rawBytes.addAndGet(n);
            }
        }

        private void addZstd(long n) {
            if (n > 0) {
                zstdBytes.addAndGet(n);
            }
        }

        private void addConn(int delta) {
            activeConn.addAndGet(delta);
        }
    }

    private static final class FloodGuard {
        private final Map<String, GuardEntry> state = new ConcurrentHashMap<>();
        private final ProxyConfig cfg;

        private FloodGuard(ProxyConfig cfg) {
            this.cfg = cfg;
        }

        private synchronized boolean begin(String ip) {
            long now = System.currentTimeMillis();
            GuardEntry entry = state.computeIfAbsent(ip, k -> new GuardEntry());

            if (entry.bannedUntilMs > now) {
                return false;
            }

            if (cfg.maxReqPerWindow > 0 && !cfg.window.isZero() && !cfg.window.isNegative()) {
                long cutoff = now - cfg.window.toMillis();
                while (!entry.requestsMs.isEmpty() && entry.requestsMs.peekFirst() < cutoff) {
                    entry.requestsMs.removeFirst();
                }
                entry.requestsMs.addLast(now);
                if (entry.requestsMs.size() > cfg.maxReqPerWindow) {
                    entry.bannedUntilMs = now + cfg.banDuration.toMillis();
                    return false;
                }
            }

            if (cfg.maxConnPerIp > 0 && entry.activeConn >= cfg.maxConnPerIp) {
                return false;
            }

            entry.activeConn++;
            return true;
        }

        private synchronized void end(String ip) {
            GuardEntry entry = state.get(ip);
            if (entry == null) {
                return;
            }
            if (entry.activeConn > 0) {
                entry.activeConn--;
            }
            long now = System.currentTimeMillis();
            if (entry.activeConn == 0 && entry.requestsMs.isEmpty() && entry.bannedUntilMs <= now) {
                state.remove(ip);
            }
        }

        private static final class GuardEntry {
            private int activeConn;
            private long bannedUntilMs;
            private final Deque<Long> requestsMs = new ArrayDeque<>();
        }
    }

    private static final class CountingInputStream extends InputStream {
        private final InputStream delegate;
        private final Counter counter;

        private CountingInputStream(InputStream delegate, Counter counter) {
            this.delegate = Objects.requireNonNull(delegate);
            this.counter = Objects.requireNonNull(counter);
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            if (value >= 0) {
                counter.add(1);
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = delegate.read(b, off, len);
            if (n > 0) {
                counter.add(n);
            }
            return n;
        }
    }

    private static final class CountingOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final Counter counter;

        private CountingOutputStream(OutputStream delegate, Counter counter) {
            this.delegate = Objects.requireNonNull(delegate);
            this.counter = Objects.requireNonNull(counter);
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            counter.add(1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            if (len > 0) {
                counter.add(len);
            }
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }
    }

    @FunctionalInterface
    private interface Counter {
        void add(long n);
    }

    private static final class NamedFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger index = new AtomicInteger(1);

        private NamedFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + index.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
