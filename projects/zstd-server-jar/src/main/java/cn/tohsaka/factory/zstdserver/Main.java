package cn.tohsaka.factory.zstdserver;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;

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

public final class Main {
    private static final byte[] PROXY_V2_SIGNATURE = new byte[]{
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51, 0x55, 0x49, 0x54, 0x0a
    };

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String configArg = parseConfigArg(args);
        Path configPath = Path.of(configArg);

        if (!Files.exists(configPath)) {
            writeDefaultConfig(configPath);
            System.out.println("[INIT] Generated config: " + configPath.toAbsolutePath());
            System.out.println("[INIT] Please edit listen/target before starting again.");
            System.out.println("[INIT] Example: target=127.0.0.1:25565");
            return;
        }

        AppConfig cfg = AppConfig.load(configPath);
        runServer(cfg);
    }

    private static void runServer(AppConfig cfg) throws Exception {
        final HostPort listen = HostPort.parse(cfg.listen);
        final HostPort target = HostPort.parse(cfg.target);

        final TrafficStats stats = new TrafficStats();
        final FloodGuard guard = new FloodGuard(cfg);
        final ExecutorService workers = Executors.newCachedThreadPool(new NamedFactory("zstdsrv-worker"));
        final ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(new NamedFactory("zstdsrv-stats"));

        try (ServerSocket listener = new ServerSocket()) {
            listener.bind(listen.toAddress());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                shutdownQuietly(ticker);
                shutdownQuietly(workers);
                closeQuietly(listener);
            }, "zstdsrv-shutdown"));

            System.out.printf("GoZstdServer started: listen=%s target=%s mode=server%n", listen, target);
            System.out.printf("guard: max_conn=%d max_req=%d window=%s ban=%s%n",
                cfg.maxConnPerIp, cfg.maxReqPerWindow, cfg.window, cfg.banDuration);

            startStatsPrinter(stats, ticker, cfg.statsInterval);

            while (true) {
                Socket client;
                try {
                    client = listener.accept();
                } catch (IOException acceptErr) {
                    if (listener.isClosed()) {
                        return;
                    }
                    System.err.println("accept error: " + acceptErr);
                    continue;
                }

                workers.execute(() -> handleClient(client, target, cfg, guard, stats, workers));
            }
        } finally {
            shutdownQuietly(ticker);
            shutdownQuietly(workers);
        }
    }

    private static void handleClient(
        Socket client,
        HostPort target,
        AppConfig cfg,
        FloodGuard guard,
        TrafficStats stats,
        ExecutorService workers
    ) {
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
                System.err.printf("blocked %s: rate or connection limit exceeded%n", sourceIp);
                return;
            }

            try (Socket upstream = new Socket()) {
                upstream.connect(target.toAddress(), 5000);
                upstream.setTcpNoDelay(true);
                clientSocket.setTcpNoDelay(true);

                final String finalSourceIp = sourceIp;
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
                    System.err.printf("pipe error source=%s dir=client->mc: %s%n", finalSourceIp, err1);
                }
                if (isRealPipeErr(err2)) {
                    System.err.printf("pipe error source=%s dir=mc->client: %s%n", finalSourceIp, err2);
                }
            } finally {
                guard.end(sourceIp);
            }
        } catch (Exception e) {
            if (isRealPipeErr(e)) {
                System.err.printf("connection error remote=%s source=%s: %s%n", remoteIp, sourceIp, e);
            }
        } finally {
            stats.addConn(-1);
        }
    }

    private static void forwardDecompress(Socket dst, InputStream src, TrafficStats stats) throws IOException {
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

    private static void forwardCompress(OutputStream dst, Socket src, int level, TrafficStats stats) throws IOException {
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

    private static ProxyInfo parseProxyProtocolV2(PushbackInputStream in) throws IOException {
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

    private static int readSome(InputStream in, byte[] buf) throws IOException {
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

    private static byte[] readFully(InputStream in, int len) throws IOException {
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

    private static String ipString(byte[] data, int offset, int len) throws IOException {
        byte[] raw = Arrays.copyOfRange(data, offset, offset + len);
        return InetAddress.getByAddress(raw).getHostAddress();
    }

    private static int u16(byte[] data, int offset) {
        return ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF);
    }

    private static void startStatsPrinter(TrafficStats stats, ScheduledExecutorService ticker, Duration interval) {
        long periodMs = Math.max(250L, interval.toMillis());
        AtomicLong prevRaw = new AtomicLong();
        AtomicLong prevZstd = new AtomicLong();

        ticker.scheduleAtFixedRate(() -> {
            long raw = stats.rawBytes.get();
            long zstd = stats.zstdBytes.get();
            int conns = stats.activeConn.get();

            long dr = raw - prevRaw.getAndSet(raw);
            long dz = zstd - prevZstd.getAndSet(zstd);

            double ratio = raw <= 0 ? 0.0 : ((double) zstd * 100.0 / (double) raw);
            long rawPerSec = (long) (dr * (1000.0 / periodMs));
            long zstdPerSec = (long) (dz * (1000.0 / periodMs));

            String now = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            System.out.printf("[%s] Raw: %s (%s) | Zstd: %s (%s) | Ratio: %.2f%% | Conns: %d%n",
                now,
                formatSize(raw),
                formatRate(rawPerSec),
                formatSize(zstd),
                formatRate(zstdPerSec),
                ratio,
                conns
            );
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        final String[] units = {"KB", "MB", "GB", "TB"};
        double v = bytes / 1024.0;
        int idx = 0;
        while (v >= 1024.0 && idx < units.length - 1) {
            v /= 1024.0;
            idx++;
        }
        return String.format(Locale.ROOT, "%.2f %s", v, units[idx]);
    }

    private static String formatRate(long bytesPerSec) {
        if (bytesPerSec < 1024) {
            return bytesPerSec + "B/s";
        }
        final String[] units = {"KB/s", "MB/s", "GB/s", "TB/s"};
        double v = bytesPerSec / 1024.0;
        int idx = 0;
        while (v >= 1024.0 && idx < units.length - 1) {
            v /= 1024.0;
            idx++;
        }
        return String.format(Locale.ROOT, "%.1f%s", v, units[idx]);
    }

    private static String parseConfigArg(String[] args) {
        String path = "zstd-server.properties";
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if ("-h".equals(a) || "--help".equals(a)) {
                printUsageAndExit();
            }
            if ("-config".equals(a) || "--config".equals(a)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("missing value for " + a);
                }
                path = args[++i];
                continue;
            }
            if (a.startsWith("--config=")) {
                path = a.substring("--config=".length());
            }
        }
        return path;
    }

    private static void printUsageAndExit() {
        System.out.println("Usage: java -jar zstd-server-jar-<ver>-all.jar [--config <path>]");
        System.out.println("First run auto-generates config if missing.");
        System.exit(0);
    }

    private static void writeDefaultConfig(Path configPath) throws IOException {
        Path parent = configPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String body = """
            # zstd server config
            # edit target/listen for your environment, then restart
            listen=0.0.0.0:25570
            target=127.0.0.1:25565
            level=7
            max_conn_per_ip=20
            max_req_per_window=30
            request_window=10s
            ban_duration=30m
            stats_interval=1s
            """;
        Files.writeString(configPath, body, StandardCharsets.UTF_8);
    }

    private static void closeWrite(Socket socket) {
        try {
            socket.shutdownOutput();
        } catch (Exception ignored) {
        }
    }

    private static void closeQuietly(ServerSocket socket) {
        try {
            socket.close();
        } catch (Exception ignored) {
        }
    }

    private static void shutdownQuietly(ExecutorService executor) {
        executor.shutdownNow();
    }

    private static String sourceIp(SocketAddress addr) {
        if (addr instanceof InetSocketAddress isa) {
            InetAddress ip = isa.getAddress();
            if (ip != null) {
                return ip.getHostAddress();
            }
            return isa.getHostString();
        }
        return String.valueOf(addr);
    }

    private static boolean isRealPipeErr(Exception err) {
        if (err == null) {
            return false;
        }
        String msg = err.toString().toLowerCase(Locale.ROOT);
        if (msg.contains("broken pipe") || msg.contains("connection reset") || msg.contains("socket closed")) {
            return false;
        }
        return !(err instanceof EOFException);
    }

    private static Duration parseDuration(String raw, Duration fallback) {
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
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private static int parseInt(String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }

    private record ProxyInfo(boolean valid, String sourceIp, int sourcePort, String targetIp, int targetPort) {
        static ProxyInfo invalid() {
            return new ProxyInfo(false, null, 0, null, 0);
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
            int v = delegate.read();
            if (v >= 0) {
                counter.add(1);
            }
            return v;
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
        private final AppConfig cfg;

        private FloodGuard(AppConfig cfg) {
            this.cfg = cfg;
        }

        private synchronized boolean begin(String ip) {
            long now = System.currentTimeMillis();
            GuardEntry e = state.computeIfAbsent(ip, k -> new GuardEntry());

            if (e.bannedUntilMs > now) {
                return false;
            }

            if (cfg.maxReqPerWindow > 0 && !cfg.window.isZero() && !cfg.window.isNegative()) {
                long cutoff = now - cfg.window.toMillis();
                while (!e.requestsMs.isEmpty() && e.requestsMs.peekFirst() < cutoff) {
                    e.requestsMs.removeFirst();
                }
                e.requestsMs.addLast(now);
                if (e.requestsMs.size() > cfg.maxReqPerWindow) {
                    e.bannedUntilMs = now + cfg.banDuration.toMillis();
                    return false;
                }
            }

            if (cfg.maxConnPerIp > 0 && e.activeConn >= cfg.maxConnPerIp) {
                return false;
            }

            e.activeConn++;
            return true;
        }

        private synchronized void end(String ip) {
            GuardEntry e = state.get(ip);
            if (e == null) {
                return;
            }
            if (e.activeConn > 0) {
                e.activeConn--;
            }

            long now = System.currentTimeMillis();
            if (e.activeConn == 0 && e.requestsMs.isEmpty() && e.bannedUntilMs <= now) {
                state.remove(ip);
            }
        }

        private static final class GuardEntry {
            private int activeConn;
            private long bannedUntilMs;
            private final Deque<Long> requestsMs = new ArrayDeque<>();
        }
    }

    private record AppConfig(
        String listen,
        String target,
        int level,
        int maxConnPerIp,
        int maxReqPerWindow,
        Duration window,
        Duration banDuration,
        Duration statsInterval
    ) {
        static AppConfig load(Path path) throws IOException {
            Properties p = new Properties();
            try (InputStream in = Files.newInputStream(path)) {
                p.load(in);
            }

            String listen = p.getProperty("listen", "0.0.0.0:25570");
            String target = p.getProperty("target", "127.0.0.1:25565");
            int level = parseInt(p.getProperty("level"), 7);
            int maxConn = parseInt(p.getProperty("max_conn_per_ip"), 20);
            int maxReq = parseInt(p.getProperty("max_req_per_window"), 30);
            Duration window = parseDuration(p.getProperty("request_window"), Duration.ofSeconds(10));
            Duration ban = parseDuration(p.getProperty("ban_duration"), Duration.ofMinutes(30));
            Duration stats = parseDuration(p.getProperty("stats_interval"), Duration.ofSeconds(1));

            level = Math.max(1, Math.min(22, level));
            if (stats.isNegative() || stats.isZero()) {
                stats = Duration.ofSeconds(1);
            }
            return new AppConfig(listen, target, level, maxConn, maxReq, window, ban, stats);
        }
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
                // Users often input "127.0.0.1." by mistake; trim to avoid DNS lookup failure.
                h = h.substring(0, h.length() - 1);
            }
            return h;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    private static final class NamedFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger seq = new AtomicInteger(1);

        private NamedFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + seq.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }
}
