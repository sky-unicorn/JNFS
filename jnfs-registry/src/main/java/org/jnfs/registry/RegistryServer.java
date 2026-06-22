package org.jnfs.registry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jnfs.common.AppHomeInitializer;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.NettyServerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 注册中心服务 (Standalone)
 * 负责 DataNode 的注册、心跳维护，以及向 NameNode 提供服务发现
 * 
 * 升级：集成 Dashboard HTTP 服务
 */
public class RegistryServer {

    static {
        AppHomeInitializer.init();
    }

    private static final Logger LOG = LoggerFactory.getLogger(RegistryServer.class);

    private final int port;
    private final int dashboardPort;

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(true);
    // Dashboard 实例引用，用于优雅关闭
    private DashboardServer dashboardServer;

    public RegistryServer(int port, int dashboardPort) {
        this.port = port;
        this.dashboardPort = dashboardPort;
    }

    public void run() throws Exception {
        // 启动 Dashboard
        dashboardServer = new DashboardServer(dashboardPort);
        new Thread(() -> dashboardServer.start()).start();

        // 注册 Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered...");
            shutdown();
        }));

        try {
            // 使用 NettyServerUtils 启动 Registry Server
            // Registry 的业务逻辑比较轻量，可以直接在 IO 线程处理 (businessGroup = null)
            NettyServerUtils.start("Registry Center", port, new RegistryHandler(), null);
        } finally {
            shutdown();
        }
    }

    /**
     * 统一的资源释放方法，支持幂等调用
     */
    private void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return; // 已关闭，幂等返回
        }
        LOG.info("正在停止 RegistryServer 资源...");

        // 1. 关闭 Dashboard HTTP 服务
        if (dashboardServer != null) {
            dashboardServer.stop();
        }

        // 2. 关闭 RegistryHandler 的定时清理任务
        RegistryHandler.shutdown();

        LOG.info("RegistryServer 资源释放完成");
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("registry.yml");

        // 初始化安全配置
        org.jnfs.common.SecurityConfig.init("registry.yml");

        // 读取服务器端口配置
        Map<String, Object> serverConfig = (Map<String, Object>) config.getOrDefault("server", Map.of());
        int port = (int) serverConfig.getOrDefault("port", 5367);

        // 读取 Dashboard 端口配置
        Map<String, Object> dashboardConfig = (Map<String, Object>) config.getOrDefault("dashboard", Map.of());
        int dashboardPort = (int) dashboardConfig.getOrDefault("port", 15367);

        // 读取心跳超时配置
        Map<String, Object> heartbeatConfig = (Map<String, Object>) config.getOrDefault("heartbeat", Map.of());
        int heartbeatTimeout = (int) heartbeatConfig.getOrDefault("timeout_ms", 30000);
        
        // 更新 Handler 中的超时设置
        RegistryHandler.heartbeatTimeout = heartbeatTimeout;

        LOG.info("启动注册中心 -> RPC Port: {}, Dashboard Port: {}, Heartbeat Timeout: {}ms", port, dashboardPort, heartbeatTimeout);

        new RegistryServer(port, dashboardPort).run();
    }
}
