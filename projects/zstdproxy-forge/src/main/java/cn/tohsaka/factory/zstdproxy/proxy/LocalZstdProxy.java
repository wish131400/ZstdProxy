package cn.tohsaka.factory.zstdproxy.proxy;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.mojang.logging.LogUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class LocalZstdProxy {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final ExecutorService WORKERS = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "zstdproxy-worker");
        t.setDaemon(true);
        return t;
    });

    private LocalZstdProxy() {
    }

    public static ProxyHandle start(String remoteHost, int remotePort, int level) throws IOException {
        ServerSocket listener = new ServerSocket();
        listener.bind(new InetSocketAddress("127.0.0.1", 0));

        AtomicBoolean running = new AtomicBoolean(true);
        Thread acceptThread = new Thread(() -> acceptLoop(listener, running, remoteHost, remotePort, level),
                "zstdproxy-accept-" + remoteHost + ":" + remotePort);
        acceptThread.setDaemon(true);
        acceptThread.start();

        return new ProxyHandle(listener, running, acceptThread);
    }

    private static void acceptLoop(ServerSocket listener, AtomicBoolean running,
                                   String remoteHost, int remotePort, int level) {
        while (running.get()) {
            try {
                Socket localClient = listener.accept();
                WORKERS.execute(() -> handleConnection(localClient, remoteHost, remotePort, level));
            } catch (SocketException socketException) {
                if (running.get()) {
                    LOGGER.warn("accept failed: {}", socketException.toString());
                }
                return;
            } catch (Exception e) {
                LOGGER.warn("accept failed: {}", e.toString());
            }
        }
    }

    private static void handleConnection(Socket localClient, String remoteHost, int remotePort, int level) {
        try (Socket client = localClient; Socket upstream = new Socket()) {
            upstream.connect(new InetSocketAddress(remoteHost, remotePort), 5000);
            upstream.setTcpNoDelay(true);
            client.setTcpNoDelay(true);

            var upstreamWriter = WORKERS.submit(() -> {
                try {
                    streamCompress(client.getInputStream(), upstream.getOutputStream(), level);
                } catch (Exception ignored) {
                } finally {
                    try {
                        upstream.shutdownOutput();
                    } catch (Exception ignored) {
                    }
                }
            });

            var downstreamWriter = WORKERS.submit(() -> {
                try {
                    streamDecompress(upstream.getInputStream(), client.getOutputStream());
                } catch (Exception ignored) {
                } finally {
                    try {
                        client.shutdownOutput();
                    } catch (Exception ignored) {
                    }
                }
            });

            upstreamWriter.get();
            downstreamWriter.get();
        } catch (Exception e) {
            LOGGER.debug("proxy pipe closed: {}", e.toString());
        }
    }

    private static void streamCompress(InputStream in, OutputStream out, int level) throws IOException {
        try (ZstdOutputStream zstdOut = new ZstdOutputStream(out, level)) {
            zstdOut.setCloseFrameOnFlush(false);
            byte[] buffer = new byte[16 * 1024];
            int read;
            while ((read = in.read(buffer)) >= 0) {
                if (read > 0) {
                    zstdOut.write(buffer, 0, read);
                    zstdOut.flush();
                }
            }
        }
    }

    private static void streamDecompress(InputStream in, OutputStream out) throws IOException {
        try (ZstdInputStream zstdIn = new ZstdInputStream(in)) {
            byte[] buffer = new byte[16 * 1024];
            int read;
            while ((read = zstdIn.read(buffer)) >= 0) {
                if (read > 0) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            }
        }
    }

    public record HostPort(String host, int port) {
        public static HostPort parse(String raw) {
            if (raw == null || raw.isBlank()) {
                throw new IllegalArgumentException("empty addr");
            }

            String value = raw.trim();
            if (value.startsWith("[") && value.contains("]")) {
                int end = value.indexOf(']');
                String host = value.substring(1, end);
                if (end + 1 < value.length() && value.charAt(end + 1) == ':') {
                    int port = Integer.parseInt(value.substring(end + 2));
                    return new HostPort(host, port);
                }
                return new HostPort(host, 25565);
            }

            int lastColon = value.lastIndexOf(':');
            int firstColon = value.indexOf(':');
            if (lastColon > 0 && firstColon == lastColon) {
                String host = value.substring(0, lastColon).trim();
                int port = Integer.parseInt(value.substring(lastColon + 1).trim());
                return new HostPort(host, port);
            }

            return new HostPort(value, 25565);
        }
    }

    public static final class ProxyHandle implements AutoCloseable {
        private final ServerSocket listener;
        private final AtomicBoolean running;
        private final Thread acceptThread;

        private ProxyHandle(ServerSocket listener, AtomicBoolean running, Thread acceptThread) {
            this.listener = listener;
            this.running = running;
            this.acceptThread = acceptThread;
        }

        public int localPort() {
            return listener.getLocalPort();
        }

        @Override
        public void close() {
            running.set(false);
            try {
                listener.close();
            } catch (IOException ignored) {
            }
        }
    }
}
