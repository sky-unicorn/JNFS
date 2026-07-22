package org.jnfs.registry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import cn.hutool.crypto.digest.BCrypt;
import org.jnfs.common.AppHomeInitializer;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.NettyServerUtils;
import org.jnfs.registry.auth.AuthManager;
import org.jnfs.registry.auth.FileUserStore;
import org.jnfs.registry.auth.MysqlUserStore;
import org.jnfs.registry.auth.UserStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 注册中心服务 (Standalone)
 * 负责 DataNode 的注册、心跳维护，以及向 NameNode 提供服务发现
 *
 * 升级：集成 Dashboard HTTP 服务 + 可选登录鉴权
 */
public class RegistryServer {

    static {
        AppHomeInitializer.init();
    }

    private static final Logger LOG = LoggerFactory.getLogger(RegistryServer.class);

    private final int port;
    private final int dashboardPort;
    private final AuthManager authManager;

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(true);
    // Dashboard 实例引用，用于优雅关闭
    private DashboardServer dashboardServer;

    public RegistryServer(int port, int dashboardPort) {
        this(port, dashboardPort, null);
    }

    public RegistryServer(int port, int dashboardPort, AuthManager authManager) {
        this.port = port;
        this.dashboardPort = dashboardPort;
        this.authManager = authManager;
    }

    public void run() throws Exception {
        // 启动 Dashboard
        dashboardServer = new DashboardServer(dashboardPort, authManager);
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

        // 2. 关闭 Dashboard 鉴权资源（session 清理线程、连接池等）
        if (authManager != null) {
            authManager.shutdown();
        }

        // 3. 关闭 RegistryHandler 的定时清理任务
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

        // 初始化 Dashboard 登录鉴权
        AuthManager authManager = initAuth(dashboardConfig);

        LOG.info("启动注册中心 -> RPC Port: {}, Dashboard Port: {}, Heartbeat Timeout: {}ms{}",
                port, dashboardPort, heartbeatTimeout,
                authManager != null ? ", 鉴权: 已启用" : ", 鉴权: 已禁用");

        new RegistryServer(port, dashboardPort, authManager).run();
    }

    /**
     * 根据配置初始化 Dashboard 鉴权
     * <p>
     * 配置路径：dashboard.auth.*
     * - enabled: 是否启用（默认 false，保留旧的无登录行为）
     * - storage.mode: file | mysql（默认 file）
     * - storage.mysql.*: mysql 连接信息
     * - initial-admin.*: 首次启动且无用户时创建初始管理员
     * - session.timeout-seconds: session 有效期（默认 7200）
     *
     * @return AuthManager 实例，未启用鉴权时返回 null
     */
    @SuppressWarnings("unchecked")
    private static AuthManager initAuth(Map<String, Object> dashboardConfig) {
        Map<String, Object> authConfig = (Map<String, Object>) dashboardConfig.getOrDefault("auth", Map.of());
        boolean authEnabled = Boolean.TRUE.equals(authConfig.get("enabled"));
        if (!authEnabled) {
            LOG.info("Dashboard 鉴权未启用（dashboard.auth.enabled=false）");
            return null;
        }

        // 1. 创建 UserStore
        Map<String, Object> storageConfig = (Map<String, Object>) authConfig.getOrDefault("storage", Map.of());
        String storageMode = (String) storageConfig.getOrDefault("mode", "file");
        UserStore userStore;
        if ("mysql".equalsIgnoreCase(storageMode)) {
            Map<String, Object> mysqlConfig = (Map<String, Object>) storageConfig.getOrDefault("mysql", Map.of());
            String dbHost = (String) mysqlConfig.getOrDefault("host", "localhost");
            int dbPort = ((Number) mysqlConfig.getOrDefault("port", 3306)).intValue();
            String dbName = (String) mysqlConfig.getOrDefault("database", "jnfs_registry");
            String dbUser = (String) mysqlConfig.getOrDefault("user", "root");
            String dbPassword = (String) mysqlConfig.getOrDefault("password", "");
            userStore = new MysqlUserStore(dbHost, dbPort, dbName, dbUser, dbPassword);
            LOG.info("Dashboard 用户存储: MySQL ({}:{}/{})", dbHost, dbPort, dbName);
        } else {
            userStore = new FileUserStore();
            LOG.info("Dashboard 用户存储: 本地文件");
        }

        // 2. 初始化初始管理员（仅在用户数为 0 时执行）
        if (userStore.userCount() == 0) {
            initInitialAdmin(authConfig, userStore);
        }

        // 3. 创建 AuthManager
        Map<String, Object> sessionConfig = (Map<String, Object>) authConfig.getOrDefault("session", Map.of());
        long sessionTimeout = ((Number) sessionConfig.getOrDefault("timeout-seconds", 7200)).longValue();
        return new AuthManager(userStore, sessionTimeout);
    }

    /**
     * 初始化初始管理员账号（仅首次启动、存储中无用户时调用）
     * <p>
     * 密码来源优先级：配置 initial-admin.password → 环境变量 JNFS_DASHBOARD_ADMIN_PASSWORD。
     * 两者均为空则拒绝启动（System.exit(2)），符合迁移框架 INV-4「失败拒绝启动」精神。
     */
    @SuppressWarnings("unchecked")
    private static void initInitialAdmin(Map<String, Object> authConfig, UserStore userStore) {
        Map<String, Object> initialAdminConfig = (Map<String, Object>) authConfig.getOrDefault("initial-admin", Map.of());
        String adminUsername = (String) initialAdminConfig.getOrDefault("username", "admin");
        String adminPassword = (String) initialAdminConfig.getOrDefault("password", "");

        // 配置为空时回退环境变量
        if (adminPassword == null || adminPassword.isEmpty()) {
            adminPassword = System.getenv("JNFS_DASHBOARD_ADMIN_PASSWORD");
        }
        if (adminPassword == null || adminPassword.isEmpty()) {
            LOG.error("Dashboard 鉴权已启用但初始管理员密码为空。请在 registry.yml 配置 "
                    + "dashboard.auth.initial-admin.password，或设置环境变量 JNFS_DASHBOARD_ADMIN_PASSWORD");
            System.exit(2);
        }

        String bcryptHash = BCrypt.hashpw(adminPassword);
        userStore.saveUser(adminUsername, bcryptHash);
        LOG.warn("初始管理员 '{}' 已创建。为安全起见，建议立即从 registry.yml 删除 initial-admin.password 明文配置",
                adminUsername);
    }
}
