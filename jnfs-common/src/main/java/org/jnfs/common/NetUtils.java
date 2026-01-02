package org.jnfs.common;

import cn.hutool.core.net.NetUtil;
import java.util.LinkedHashSet;

/**
 * 网络工具类
 */
public class NetUtils {

    /**
     * 获取本机 IP 地址
     * 优先返回非回环的 IPv4 地址
     */
    public static String getLocalIp() {
        try {
            // 获取所有本机 IPv4 地址
            LinkedHashSet<String> ips = NetUtil.localIpv4s();
            if (ips != null && !ips.isEmpty()) {
                for (String ip : ips) {
                    // 排除 127.0.0.1 和 localhost
                    if (!"127.0.0.1".equals(ip) && !"localhost".equals(ip)) {
                        return ip;
                    }
                }
            }
            // 兜底方案
            return NetUtil.getLocalhostStr();
        } catch (Exception e) {
            e.printStackTrace();
            return "localhost";
        }
    }
}
