package org.jnfs.registry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import cn.hutool.crypto.digest.BCrypt;
import org.jnfs.common.AppHomeInitializer;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.NettyServerUtils;
import org.jnfs.common.SecurityUtil;
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
    /**
     * 共享存储 DataSource（storage.mode=mysql 时创建）。
     * 同时供 Dashboard 鉴权用户库（dashboard_user 表）与冗余存储管理 API 使用，两者同库。
     * file 模式为 null。
     */
    private final com.zaxxer.hikari.HikariDataSource metadataDataSource;

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(true);
    // Dashboard 实例引用，用于优雅关闭
    private DashboardServer dashboardServer;

    public RegistryServer(int port, int dashboardPort) {
        this(port, dashboardPort, null, null);
    }

    public RegistryServer(int port, int dashboardPort, AuthManager authManager) {
        this(port, dashboardPort, authManager, null);
    }

    public RegistryServer(int port, int dashboardPort, AuthManager authManager,
                          com.zaxxer.hikari.HikariDataSource metadataDataSource) {
        this.port = port;
        this.dashboardPort = dashboardPort;
        this.authManager = authManager;
        this.metadataDataSource = metadataDataSource;
    }

    public void run() throws Exception {
        // 启动 Dashboard（传入元数据库 DataSource 供冗余存储 API 使用）
        dashboardServer = new DashboardServer(dashboardPort, authManager, metadataDataSource);
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

        // 4. 关闭元数据库 DataSource（决策 9，冗余存储 API）
        if (metadataDataSource != null && !metadataDataSource.isClosed()) {
            try {
                metadataDataSource.close();
            } catch (Exception e) {
                LOG.warn("关闭元数据库 DataSource 失败", e);
            }
        }

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

        // 统一存储配置：一个 mode 同时决定鉴权用户后端与冗余 API 是否启用，避免重复耦合
        Map<String, Object> storageConfig = (Map<String, Object>) config.getOrDefault("storage", Map.of());
        String storageMode = (String) storageConfig.getOrDefault("mode", "file");

        // 把 storage 配置序列化并 AES 加密后注入 RegistryHandler，供 NameNode 启动时拉取（决策：Registry 为唯一配置源）
        try {
            byte[] plainPayload = serializeStorageConfig(storageConfig).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] cipherPayload = new SecurityUtil(org.jnfs.common.SecurityConfig.getAesKey()).encryptBytes(plainPayload);
            RegistryHandler.setStorageConfigPayload(cipherPayload);
            LOG.info("存储配置已加密发布到 RegistryHandler（mode={}）", storageMode);
        } catch (Exception e) {
            LOG.error("序列化/加密 storage 配置失败，拒绝启动", e);
            System.exit(2);
        }

        // mysql 模式：创建单一共享 DataSource（dashboard_user 表与冗余元数据表同库）
        // file 模式：不创建 DataSource（FileUserStore + 无冗余 API）
        com.zaxxer.hikari.HikariDataSource storageDataSource =
                "mysql".equalsIgnoreCase(storageMode) ? createMysqlDataSource(storageConfig) : null;

        // 初始化 Dashboard 鉴权（mysql 模式复用共享 DataSource）
        AuthManager authManager = initAuth(dashboardConfig, storageMode, storageDataSource);

        LOG.info("启动注册中心 -> RPC Port: {}, Dashboard Port: {}, Heartbeat Timeout: {}ms, 存储模式: {}{}{}",
                port, dashboardPort, heartbeatTimeout, storageMode,
                authManager != null ? ", 鉴权: 已启用" : ", 鉴权: 已禁用",
                storageDataSource != null ? ", 冗余API: 已启用" : ", 冗余API: 未启用");

        new RegistryServer(port, dashboardPort, authManager, storageDataSource).run();
    }

    /**
     * 将顶层 storage 配置序列化为管道分隔明文（供 NameNode 拉取）。
     * <p>
     * 格式：{@code mode|mysqlHost|mysqlPort|mysqlDatabase|mysqlUser|mysqlPassword}
     * - file 模式：{@code file|||||}（后 5 字段空）
     * - mysql 模式：{@code mysql|host|port|database|user|password}
     * <p>
     * 本方法只产出明文，由调用方负责 AES 加密后再发布；不输出密码日志。
     *
     * @param storageConfig 顶层 storage 配置段
     * @return 明文 payload
     */
    @SuppressWarnings("unchecked")
    private static String serializeStorageConfig(Map<String, Object> storageConfig) {
        String mode = (String) storageConfig.getOrDefault("mode", "file");
        if (!"mysql".equalsIgnoreCase(mode)) {
            return "file|||||"; // file 模式：5 个空字段
        }
        Map<String, Object> mysql = (Map<String, Object>) storageConfig.getOrDefault("mysql", Map.of());
        return String.join("|",
                "mysql",
                (String) mysql.getOrDefault("host", "localhost"),
                String.valueOf(mysql.getOrDefault("port", 3306)),
                (String) mysql.getOrDefault("database", "jnfs"),
                (String) mysql.getOrDefault("user", "root"),
                (String) mysql.getOrDefault("password", ""));
    }

    /**
     * 根据顶层 storage 配置创建共享 MySQL DataSource。
     * <p>
     * 配置路径：storage.mysql.{host, port, database, user, password}
     * dashboard_user 表与冗余组/策略/任务表共用此连接与同一数据库。
     *
     * @param storageConfig 顶层 storage 配置段
     * @return HikariDataSource
     */
    @SuppressWarnings("unchecked")
    private static com.zaxxer.hikari.HikariDataSource createMysqlDataSource(Map<String, Object> storageConfig) {
        Map<String, Object> mysqlConfig = (Map<String, Object>) storageConfig.getOrDefault("mysql", Map.of());
        String dbHost = (String) mysqlConfig.getOrDefault("host", "localhost");
        int dbPort = ((Number) mysqlConfig.getOrDefault("port", 3306)).intValue();
        String dbName = (String) mysqlConfig.getOrDefault("database", "jnfs");
        String dbUser = (String) mysqlConfig.getOrDefault("user", "root");
        String dbPassword = (String) mysqlConfig.getOrDefault("password", "");

        com.zaxxer.hikari.HikariConfig hikariConfig = new com.zaxxer.hikari.HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        hikariConfig.setUsername(dbUser);
        hikariConfig.setPassword(dbPassword);
        hikariConfig.setMaximumPoolSize(5); // Dashboard 鉴权 + 冗余 API 共享，略大于原 3
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        LOG.info("共享存储 DataSource 已创建: {}:{}/{}", dbHost, dbPort, dbName);
        return new com.zaxxer.hikari.HikariDataSource(hikariConfig);
    }

    /**
     * 根据配置初始化 Dashboard 鉴权。
     * <p>
     * 用户存储后端由顶层 storage.mode 决定：
     * - mysql：复用共享 DataSource（MysqlUserStore，close() 不关闭共享池）
     * - file：本地文件（FileUserStore）
     * <p>
     * 配置路径：dashboard.auth.*
     * - enabled: 是否启用（默认 false，保留旧的无登录行为）
     * - initial-admin.*: 首次启动且无用户时创建初始管理员
     * - session.timeout-seconds: session 有效期（默认 7200）
     *
     * @param dashboardConfig  dashboard 配置段
     * @param storageMode      顶层 storage.mode（file | mysql）
     * @param sharedDataSource 共享 DataSource（mysql 模式时非 null）
     * @return AuthManager 实例，未启用鉴权时返回 null
     */
    @SuppressWarnings("unchecked")
    private static AuthManager initAuth(Map<String, Object> dashboardConfig,
                                        String storageMode,
                                        com.zaxxer.hikari.HikariDataSource sharedDataSource) {
        Map<String, Object> authConfig = (Map<String, Object>) dashboardConfig.getOrDefault("auth", Map.of());
        boolean authEnabled = Boolean.TRUE.equals(authConfig.get("enabled"));
        if (!authEnabled) {
            LOG.info("Dashboard 鉴权未启用（dashboard.auth.enabled=false）");
            return null;
        }

        // 1. 创建 UserStore（后端由顶层 storage.mode 决定）
        UserStore userStore;
        if ("mysql".equalsIgnoreCase(storageMode) && sharedDataSource != null) {
            userStore = new MysqlUserStore(sharedDataSource);
            LOG.info("Dashboard 用户存储: MySQL（共享存储 DataSource）");
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
