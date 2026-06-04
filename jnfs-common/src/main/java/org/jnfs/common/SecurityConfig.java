package org.jnfs.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * 安全配置中心
 * 集中管理 Token 和 AES 加密密钥，从 YAML 配置文件读取
 *
 * 使用方式 (服务端):
 *   SecurityConfig.init("namenode.yml");
 *   String token = SecurityConfig.getToken();
 *   byte[] key = SecurityConfig.getAesKey();
 *
 * 使用方式 (SDK/客户端):
 *   String token = SecurityConfig.getToken(); // 使用默认值
 */
public class SecurityConfig {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityConfig.class);

    public static final String DEFAULT_TOKEN = "jnfs-secure-token-2025";
    static final byte[] DEFAULT_AES_KEY = "jnfs-aes-key-256bit-secure-key!!".getBytes(StandardCharsets.UTF_8);

    private static volatile String cachedToken;
    private static volatile byte[] cachedAesKey;
    private static volatile boolean initialized;

    private SecurityConfig() {
    }

    /**
     * 从指定 YAML 配置文件加载安全配置。
     * 配置格式：security.token / security.aes-key
     * 配置缺失时使用硬编码默认值（带 WARN 日志）。
     * 多次调用不会重复加载（首次生效）。
     */
    @SuppressWarnings("unchecked")
    public static synchronized void init(String configFileName) {
        if (initialized) {
            return;
        }

        Map<String, Object> config = ConfigUtil.loadConfig(configFileName);
        Map<String, Object> sec = (Map<String, Object>) config.get("security");

        // 读取 token
        if (sec != null && sec.containsKey("token")) {
            String customToken = (String) sec.get("token");
            if (customToken != null && !customToken.isEmpty()) {
                cachedToken = customToken;
            }
        }
        if (cachedToken == null) {
            LOG.warn("配置文件中未找到 security.token，使用默认值 (长度={})", DEFAULT_TOKEN.length());
            cachedToken = DEFAULT_TOKEN;
        }

        // 读取 AES key
        if (sec != null && sec.containsKey("aes-key")) {
            String keyStr = (String) sec.get("aes-key");
            if (keyStr != null && !keyStr.isEmpty()) {
                byte[] parsed = keyStr.getBytes(StandardCharsets.UTF_8);
                if (parsed.length >= 32) {
                    cachedAesKey = Arrays.copyOf(parsed, 32);
                } else {
                    throw new IllegalArgumentException(
                            "security.aes-key 长度不足: 需要至少 32 字节，实际 " + parsed.length
                                    + " 字节。请使用 32 字符的强随机密钥。");
                }
            }
        }
        if (cachedAesKey == null) {
            LOG.warn("配置文件中未找到 security.aes-key，使用默认值");
            cachedAesKey = DEFAULT_AES_KEY;
        }

        initialized = true;
        LOG.info("SecurityConfig 初始化完成 (token 已加载, key 长度={})", cachedAesKey.length);
    }

    /**
     * 获取安全令牌。
     * 首次调用时若未 init，自动使用 ConfigUtil 加载默认配置（带 WARN 日志）。
     */
    public static String getToken() {
        if (!initialized) {
            synchronized (SecurityConfig.class) {
                if (!initialized) {
                    // 自动初始化：尝试从 jnfs 配置读取
                    LOG.warn("SecurityConfig 尚未初始化，将使用默认 token。"
                            + "请在生产环境中调用 SecurityConfig.init(\"your-config.yml\")");
                    forceInitDefaults();
                }
            }
        }
        return cachedToken;
    }

    /**
     * 获取 AES 加密密钥 (256 位)。
     * 首次调用时若未 init，自动使用默认值（带 WARN 日志）。
     */
    public static byte[] getAesKey() {
        if (!initialized) {
            synchronized (SecurityConfig.class) {
                if (!initialized) {
                    LOG.warn("SecurityConfig 尚未初始化，将使用默认 AES 密钥。"
                            + "请在生产环境中调用 SecurityConfig.init(\"your-config.yml\")");
                    forceInitDefaults();
                }
            }
        }
        return Arrays.copyOf(cachedAesKey, cachedAesKey.length);
    }

    private static void forceInitDefaults() {
        cachedToken = DEFAULT_TOKEN;
        cachedAesKey = DEFAULT_AES_KEY;
        initialized = true;
    }
}
