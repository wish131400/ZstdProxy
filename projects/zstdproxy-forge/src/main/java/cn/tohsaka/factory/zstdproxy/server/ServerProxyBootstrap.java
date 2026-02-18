package cn.tohsaka.factory.zstdproxy.server;

import com.mojang.logging.LogUtils;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.server.ServerStartedEvent;
import net.minecraftforge.event.server.ServerStoppingEvent;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public final class ServerProxyBootstrap {
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static final ServerProxyRuntime RUNTIME = new ServerProxyRuntime();

    private ServerProxyBootstrap() {
    }

    public static void init() {
        if (!INITIALIZED.compareAndSet(false, true)) {
            return;
        }
        MinecraftForge.EVENT_BUS.addListener(ServerProxyBootstrap::onServerStarted);
        MinecraftForge.EVENT_BUS.addListener(ServerProxyBootstrap::onServerStopping);
        LOGGER.info("zstdproxy server bootstrap initialized");
    }

    private static void onServerStarted(ServerStartedEvent event) {
        if (!event.getServer().isDedicatedServer()) {
            return;
        }
        RUNTIME.start(event.getServer().getPort());
    }

    private static void onServerStopping(ServerStoppingEvent event) {
        if (!event.getServer().isDedicatedServer()) {
            return;
        }
        RUNTIME.stop();
    }
}
