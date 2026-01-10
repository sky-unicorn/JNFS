package org.jnfs.common;

import org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 配置加载工具类
 */
public class ConfigUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

    /**
     * 加载 YAML 配置文件
     * 优先从运行目录查找，如果没有则从 classpath 加载
     */
    public static Map<String, Object> loadConfig(String fileName) {
        Yaml yaml = new Yaml();
        
        // 1. 尝试从外部运行目录加载 (便于生产环境修改配置)
        try {
            if (Files.exists(Paths.get(fileName))) {
                LOG.info("加载外部配置文件: {}", fileName);
                try (InputStream in = Files.newInputStream(Paths.get(fileName))) {
                    return yaml.load(in);
                }
            }
        } catch (Exception e) {
            LOG.error("加载外部配置文件失败", e);
        }

        // 2. 尝试从 classpath 加载
        LOG.info("加载 Classpath 配置文件: {}", fileName);
        try (InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream(fileName)) {
            if (in == null) {
                throw new RuntimeException("找不到配置文件: " + fileName);
            }
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败: " + fileName, e);
        }
    }

    /**
     * 解析注册中心地址配置
     * 支持 List<String> 或逗号分隔的 String，兼容旧版 host/port 配置
     */
    @SuppressWarnings("unchecked")
    public static List<InetSocketAddress> parseRegistryAddresses(Map<String, Object> config) {
        List<InetSocketAddress> registryAddresses = new ArrayList<>();
        String defaultHost = "localhost";
        int defaultPort = 5367;

        if (config != null && config.containsKey("registry")) {
            Map<String, Object> regConfig = (Map<String, Object>) config.get("registry");
            // 优先检查 'addresses' 或 'address'
            Object addressesObj = regConfig.get("addresses");
            if (addressesObj == null) {
                 addressesObj = regConfig.get("address");
            }
            
            if (addressesObj instanceof List) {
                List<String> addrList = (List<String>) addressesObj;
                for (String addr : addrList) {
                    parseAndAddAddress(addr, registryAddresses);
                }
            } else if (addressesObj instanceof String) {
                String[] addrs = ((String) addressesObj).split(",");
                for (String addr : addrs) {
                    parseAndAddAddress(addr, registryAddresses);
                }
            } else {
                // 兼容旧配置
                String host = (String) regConfig.getOrDefault("host", defaultHost);
                int port = (int) regConfig.getOrDefault("port", defaultPort);
                registryAddresses.add(new InetSocketAddress(host, port));
            }
        } else {
            registryAddresses.add(new InetSocketAddress(defaultHost, defaultPort));
        }
        
        return registryAddresses;
    }

    private static void parseAndAddAddress(String addr, List<InetSocketAddress> list) {
        try {
            String[] parts = addr.trim().split(":");
            if (parts.length == 2) {
                list.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            }
        } catch (Exception e) {
            LOG.warn("解析注册中心地址失败: {}", addr);
        }
    }
}
