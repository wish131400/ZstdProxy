package cn.tohsaka.factory.zstdproxy;

import cn.tohsaka.factory.zstdproxy.client.ClientProxyPublisher;
import cn.tohsaka.factory.zstdproxy.server.ServerProxyBootstrap;
import com.mojang.logging.LogUtils;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.DistExecutor;
import net.minecraftforge.api.distmarker.Dist;
import org.slf4j.Logger;

@Mod(Zstdproxy.MODID)
public class Zstdproxy {
    public static final String MODID = "zstdproxy";

    private static final Logger LOGGER = LogUtils.getLogger();

    public Zstdproxy() {
        DistExecutor.safeRunWhenOn(Dist.CLIENT, () -> ClientProxyPublisher::init);
        ServerProxyBootstrap.init();
        LOGGER.info("zstdproxy loaded");
    }
}
