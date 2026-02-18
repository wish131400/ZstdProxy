package cn.tohsaka.factory.zstdproxy;

import java.util.List;

public record ZstdServerList(List<ZstdServer> servers) {
    public record ZstdServer(String name, String addr, String mask, String icon) {
    }
}
