package org.jnfs.common;

/**
 * 全局常量定义
 */
public class Constants {

    // 默认端口定义
    public static final int DEFAULT_REGISTRY_PORT = 5367;
    public static final int DEFAULT_NAMENODE_PORT = 5368;
    public static final int DEFAULT_DATANODE_PORT = 5369;
    public static final int DEFAULT_DASHBOARD_PORT = 15367;

    /**
     * 获取安全令牌，委托给 SecurityConfig
     */
    public static String getValidToken() {
        return SecurityConfig.getToken();
    }
}
