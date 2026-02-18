package cn.tohsaka.factory.zstdproxy;

import net.minecraftforge.common.ForgeConfigSpec;

public final class ClientConfig {
    public static final ForgeConfigSpec SPEC;

    private static final ForgeConfigSpec.ConfigValue<String> URL;
    private static final ForgeConfigSpec.IntValue LEVEL;

    static {
        ForgeConfigSpec.Builder builder = new ForgeConfigSpec.Builder();

        URL = builder
                .comment("Remote JSON endpoint. Leave empty to use local servers.zstd.json")
                .define("url", "");

        LEVEL = builder
                .comment("zstd compression level for client->server stream")
                .defineInRange("level", 3, 1, 22);

        SPEC = builder.build();
    }

    private ClientConfig() {
    }

    public static String getUrl() {
        String value = URL.get();
        return value == null ? "" : value.trim();
    }

    public static int getLevel() {
        return LEVEL.get();
    }
}
