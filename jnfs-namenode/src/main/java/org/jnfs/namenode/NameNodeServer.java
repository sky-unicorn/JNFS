package org.jnfs.namenode;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jnfs.common.AppHomeInitializer;
import org.jnfs.common.ChannelPoolUtils;
import org.jnfs.common.CommandType;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.Constants;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.DataDirResolver;
import org.jnfs.common.HeartbeatSender;
import org.jnfs.common.NetUtils;
import org.jnfs.common.NettyClientBootstrap;
import org.jnfs.common.NettyServerUtils;
import org.jnfs.common.NodeAddressResolver;
import org.jnfs.common.NodeIdManager;
import org.jnfs.common.Packet;
import org.jnfs.common.ServerShutdownHelper;
import org.jnfs.common.SecurityConfig;
import org.jnfs.common.SecurityUtil;
import org.jnfs.common.migration.MigrationResult;
import org.jnfs.common.migration.MigrationRunner;
import org.jnfs.common.migration.StorageMode;
import org.jnfs.namenode.migration.FileToH2Importer;
import org.jnfs.namenode.migration.FileToMysqlImporter;
import org.jnfs.namenode.replication.ReplicationGroupStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;

/**
 * NameNode 服务启动类
 * 负责管理元数据和调度 DataNode
 *
 * 升级：集成注册中心发现机制，并根据配置初始化元数据管理器
 * 优化：使用连接池复用 Registry 连接 (方案 B)
 */
public class NameNodeServer {

    static {
        AppHomeInitializer.init();
    }

    private static final Logger LOG = LoggerFactory.getLogger(NameNodeServer.class);

    /** 启动期拉取 storage 配置的总体重试预算（Registry 可能仍在初始化，如 H2 首次建库） */
    private static final long STORAGE_CONFIG_FETCH_MAX_WAIT_MS = 60_000L;
    /** 启动期拉取 storage 配置失败后的重试间隔 */
    private static final long STORAGE_CONFIG_FETCH_RETRY_INTERVAL_MS = 2_000L;

    private final int port;
    private final String advertisedHost;
    private final String nodeId;
    // 支持多个注册中心地址
    private final List<InetSocketAddress> registryAddresses;

    // 复用 EventLoopGroup
    private final EventLoopGroup workerGroup;
    // 连接池映射
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> registryPoolMap;

    // 调度器 (使用 Daemon 线程)
    private final ScheduledExecutorService heartbeatScheduler;
    private final ScheduledExecutorService discoveryScheduler;

    // 冗余组配置缓存（JDBC 模式 mysql/h2 构造；未配置时 null → 单副本降级）
    private final ReplicationGroupStore replicationGroupStore;

    // 夜间对账同步调度器（JDBC 模式 mysql/h2 构造；未配置时 null → 对账短路）
    private final org.jnfs.namenode.replication.ReplicaSyncScheduler replicaSyncScheduler;

    // 后台文件类型嗅探/大小回填调度器（JDBC 模式 mysql/h2 构造；与上传下载主链路无关）
    private final FileTypeDetectScheduler fileTypeDetectScheduler;

    // 元数据管理器（用于 shutdown 时关闭 DataSource）
    private final MetadataManager metadataManager;

    public NameNodeServer(int port, String advertisedHost, String nodeId, List<InetSocketAddress> registryAddresses,
                          ReplicationGroupStore replicationGroupStore,
                          org.jnfs.namenode.replication.ReplicaSyncScheduler replicaSyncScheduler,
                          FileTypeDetectScheduler fileTypeDetectScheduler,
                          MetadataManager metadataManager) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.nodeId = nodeId;
        this.registryAddresses = registryAddresses;
        this.replicationGroupStore = replicationGroupStore;
        this.replicaSyncScheduler = replicaSyncScheduler;
        this.fileTypeDetectScheduler = fileTypeDetectScheduler;
        this.metadataManager = metadataManager;

        // 初始化共享的 Worker Group
        this.workerGroup = new NioEventLoopGroup();

        // 初始化连接池 (使用通用工具类)
        this.registryPoolMap = ChannelPoolUtils.createDefaultPoolMap(workerGroup);

        // 初始化调度器 (使用统一的 Daemon 线程工厂)
        this.heartbeatScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("NameNode-Heartbeat"));
        this.discoveryScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("NameNode-Discovery"));
    }

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(true);

    /** I1 修复：首次 discovery 是否已完成（volatile 跨线程可见） */
    private volatile boolean firstDiscoveryDone = false;

    public void run() throws Exception {
        // 启动后台线程定期从注册中心拉取 DataNode 列表
        startDiscoveryThread();
        // 启动注册与心跳线程
        startRegistrationHeartbeatThread();

        // 共享的 Handler 实例
        NameNodeHandler sharedHandler = new NameNodeHandler();

        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(16);

        // 注册 Shutdown Hook (必须在 start 阻塞前注册)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered...");
            shutdown();
        }));

        try {
            // 使用 NettyServerUtils 启动服务，传入共享 Handler
            // 这会阻塞直到 Channel 关闭
            NettyServerUtils.start("NameNode", port, sharedHandler, businessGroup);
        } finally {
            // 如果 NettyServerUtils.start 返回 (例如 Channel 关闭)，主动停止所有资源
            shutdown();
            // 确保业务线程组也关闭 (虽然 NettyServerUtils 可能会尝试关闭它，但这里作为兜底)
            businessGroup.shutdownGracefully();
        }
    }

    /**
     * 统一的资源释放方法，支持幂等调用
     */
    private void shutdown() {
        // 关闭对账同步调度器（mysql 模式）
        if (replicaSyncScheduler != null) {
            try {
                replicaSyncScheduler.shutdown();
            } catch (Exception e) {
                LOG.warn("关闭 ReplicaSyncScheduler 失败", e);
            }
        }
        // 关闭冗余组缓存刷新调度器（mysql 模式）
        if (replicationGroupStore != null) {
            try {
                replicationGroupStore.shutdown();
            } catch (Exception e) {
                LOG.warn("关闭 ReplicationGroupStore 失败", e);
            }
        }
        // 关闭后台文件类型嗅探调度器（先于元数据 DataSource 关闭，其内部 workerGroup 一并释放）
        if (fileTypeDetectScheduler != null) {
            try {
                fileTypeDetectScheduler.shutdown();
            } catch (Exception e) {
                LOG.warn("关闭 FileTypeDetectScheduler 失败", e);
            }
        }
        // 关闭 JDBC 元数据 DataSource（mysql/H2 共享池；H2 AUTO_SERVER 混合模式下本进程连接须在
        // 退出前释放，最后一个退出的进程兜底关闭共享文件库）
        if (metadataManager != null && metadataManager.isJdbcBacked()) {
            try {
                javax.sql.DataSource ds = metadataManager.getDataSource();
                if (ds instanceof com.zaxxer.hikari.HikariDataSource hds && !hds.isClosed()) {
                    hds.close();
                }
            } catch (Exception e) {
                LOG.warn("关闭元数据 DataSource 失败", e);
            }
        }
        ServerShutdownHelper.shutdownAll(LOG, "NameNodeServer", running,
                new ScheduledExecutorService[]{heartbeatScheduler, discoveryScheduler},
                registryPoolMap, workerGroup);
    }

    private void startRegistrationHeartbeatThread() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                // 新格式: node_id|host:port
                String payload = nodeId + "|" + advertisedHost + ":" + port;
                HeartbeatSender.broadcastString(LOG, registryPoolMap, registryAddresses,
                        CommandType.REGISTRY_HEARTBEAT_NAMENODE, addr -> payload);
            } catch (Exception e) {
                LOG.error("发送心跳失败: {}", e.getMessage(), e);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    private void startDiscoveryThread() {
        discoveryScheduler.scheduleAtFixedRate(() -> {
            try {
                fetchDataNodesFromRegistry();
            } catch (Exception e) {
                LOG.error("从注册中心获取节点失败: {}", e.getMessage(), e);
            }
        }, 0, 10, TimeUnit.SECONDS); // 每10秒刷新一次
    }

    private void fetchDataNodesFromRegistry() {
        // 尝试从任一 Registry 获取 DataNode 列表 (Failover 模式)
        for (InetSocketAddress addr : registryAddresses) {
            SimpleChannelPool pool = registryPoolMap.get(addr);
            Channel ch = null;
            String handlerName = "discovery_" + System.nanoTime();
            try {
                // 同步获取连接 (带有超时)
                Future<Channel> future = pool.acquire();
                if (future.await(3000, TimeUnit.MILLISECONDS)) {
                    if (future.isSuccess()) {
                        ch = future.getNow();
                    }
                } else {
                    // 超时取消，避免连接泄漏
                    future.cancel(true);
                }

                if (ch == null) {
                    continue; // 尝试下一个
                }

                CompletableFuture<Boolean> promise = new CompletableFuture<>();
                DiscoveryHandler handler = new DiscoveryHandler(promise);

                // 动态添加 Handler
                ch.pipeline().addLast(handlerName, handler);

                Packet request = new Packet();
                request.setCommandType(CommandType.REGISTRY_GET_DATANODES);
                request.setToken(Constants.getValidToken());
                ch.writeAndFlush(request);

                // 等待结果
                try {
                    promise.get(5, TimeUnit.SECONDS);
                    // I1 修复：首次 discovery 成功且映射已建立后，触发对账 startup-recovery。
                    // 此刻 NodeAddressResolver 映射就绪，resolveNodeAddress 可正常解析。
                    // 用独立 daemon 线程跑，避免阻塞 discovery 调度线程（dispatchTasks 内含 semaphore.acquire）。
                    if (!firstDiscoveryDone) {
                        firstDiscoveryDone = true;
                        if (replicaSyncScheduler != null) {
                            Thread t = new Thread(() -> {
                                try {
                                    replicaSyncScheduler.runStartupRecovery();
                                } catch (Exception e) {
                                    LOG.warn("runStartupRecovery 异常", e);
                                }
                            }, "ReplicaSync-StartupRecovery");
                            t.setDaemon(true);
                            t.start();
                        }
                    }
                    // 成功获取，退出循环
                    return;
                } catch (TimeoutException e) {
                    LOG.warn("获取 DataNode 列表超时 ({})", addr);
                }
            } catch (Exception e) {
                // LOG.warn("获取 DataNode 列表失败 ({}) : {}", addr, e.getMessage());
            } finally {
                if (ch != null) {
                    // 清理 Handler
                    if (ch.pipeline().get(handlerName) != null) {
                        ch.pipeline().remove(handlerName);
                    }
                    pool.release(ch);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("namenode.yml");

        // 初始化安全配置
        SecurityConfig.init("namenode.yml");

        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 5368);
        // 如果没有配置 advertised_host，则自动获取本机 IP (统一使用项目自带的 NetUtils)
        String advertisedHost = (String) serverConfig.getOrDefault("advertised_host", NetUtils.getLocalIp());

        // 读取注册中心配置 (支持逗号分隔的多个地址)
        List<InetSocketAddress> registryAddresses = ConfigUtil.parseRegistryAddresses(config);

        LOG.info("使用注册中心集群: {}", registryAddresses);
        LOG.info("对外广播地址: {}", advertisedHost);

        // 初始化 node_id (配置指定 > 本地文件 > 自动生成)
        String nodeId = NodeIdManager.initialize(serverConfig);
        LOG.info("节点ID: {}", nodeId);

        // 加载安全配置

        // --- 数据迁移（必须在初始化业务组件之前执行） ---
        File dataDir = DataDirResolver.dataDir();

        // --- 初始化 MetadataManager ---
        MetadataManager metadataManager = null;

        // 冗余组配置缓存（mysql/h2 分支构造；未配置时 null → 单副本降级）
        ReplicationGroupStore replicationGroupStore = null;

        // 对账同步调度器（mysql/h2 分支构造；未配置时 null → 对账短路）
        org.jnfs.namenode.replication.ReplicaSyncScheduler replicaSyncScheduler = null;

        // 后台文件类型嗅探/大小回填调度器（mysql/h2 分支构造）
        FileTypeDetectScheduler fileTypeDetectScheduler = null;

        // 缓存配置默认值
        boolean cacheEnabled = true;
        long cacheMaxSize = 100000L;

        // --- 从 Registry 拉取 storage 配置（决策：Registry 为唯一配置源，避免 namenode.yml 重复配置） ---
        // 必须在迁移/MetadataManager 初始化之前完成；拉取失败则拒绝启动
        LOG.info("从 Registry 拉取 storage 配置...");
        StorageConfig storageCfg;
        try {
            storageCfg = fetchStorageConfigFromRegistry(registryAddresses);
        } catch (Exception e) {
            LOG.error("拉取 storage 配置失败，拒绝启动。原因: {}", e.getMessage());
            System.exit(2);
            return; // unreachable，仅为编译器满足
        }
        String mode = storageCfg.mode;
        StorageMode storageMode = StorageMode.fromConfig(mode);
        LOG.info("storage 模式: {}, mysql={}, h2Path={}", mode,
                "mysql".equalsIgnoreCase(mode)
                        ? storageCfg.mysqlHost + ":" + storageCfg.mysqlPort + "/" + storageCfg.mysqlDatabase
                        : "n/a",
                storageCfg.h2Path == null || storageCfg.h2Path.isEmpty() ? "(DataDirResolver默认)" : storageCfg.h2Path);

        // 读取缓存配置（本地 JVM 调优，从顶层 cache 段读取，与存储模式无关）
        if (config.containsKey("cache")) {
            Map<String, Object> cacheConfig = (Map<String, Object>) config.get("cache");
            cacheEnabled = (boolean) cacheConfig.getOrDefault("enabled", true);
            Object maxSizeObj = cacheConfig.getOrDefault("max-size", 100000);
            if (maxSizeObj instanceof Integer) {
                cacheMaxSize = ((Integer) maxSizeObj).longValue();
            } else if (maxSizeObj instanceof Long) {
                cacheMaxSize = (Long) maxSizeObj;
            }
        }

        if ("mysql".equalsIgnoreCase(mode)) {
            LOG.info("使用 MySQL 元数据存储");
            String dbHost = storageCfg.mysqlHost;
            int dbPort = storageCfg.mysqlPort;
            String dbName = storageCfg.mysqlDatabase;
            String user = storageCfg.mysqlUser;
            String password = storageCfg.mysqlPassword;

            // 先运行迁移（必须在初始化 MetadataManager 之前）
            com.zaxxer.hikari.HikariDataSource migrationDs = null;
            try {
                com.zaxxer.hikari.HikariConfig hikariConfig = new com.zaxxer.hikari.HikariConfig();
                hikariConfig.setJdbcUrl("jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                        + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
                hikariConfig.setUsername(user);
                hikariConfig.setPassword(password);
                hikariConfig.setMaximumPoolSize(2);
                migrationDs = new com.zaxxer.hikari.HikariDataSource(hikariConfig);

                MigrationResult migrationResult = MigrationRunner.run(storageMode, null, migrationDs);
                if (migrationResult.isFailed()) {
                    LOG.error("数据迁移失败，拒绝启动。原因: {}", migrationResult.getMessage());
                    System.exit(2);
                }
                LOG.info("数据迁移: {}", migrationResult.getMessage());
            } finally {
                if (migrationDs != null) {
                    migrationDs.close();
                }
            }

            metadataManager = new MySQLMetadataManager(dbHost, dbPort, dbName, user, password);

            // --- file → mysql 自动导入（单机转分布式场景） ---
            // 若存在 file 模式历史日志, 先规整为 V1(稳定 storageId), 再导入 mysql
            File logFile = new File(dataDir, MigrationRunner.METADATA_LOG_FILE);
            if (logFile.exists()) {
                MigrationResult fileMigration = MigrationRunner.run(StorageMode.FILE, dataDir, null);
                if (fileMigration.isFailed()) {
                    LOG.error("file 日志迁移失败，拒绝启动。原因: {}", fileMigration.getMessage());
                    System.exit(2);
                }
                try {
                    FileToMysqlImporter.importIfApplicable(
                            dataDir, metadataManager.getDataSource());
                } catch (Exception e) {
                    LOG.error("file→mysql 自动导入失败，拒绝启动。原因: {}", e.getMessage(), e);
                    System.exit(2);
                }
            }

            // 冗余组配置缓存（mysql 模式专用，决策 9：NameNode 定期从 mysql 加载冗余组定义）
            replicationGroupStore = new ReplicationGroupStore(
                    metadataManager.getDataSource());
            replicationGroupStore.start();
            LOG.info("冗余组配置缓存已启动（mysql 模式，多副本启用）");

            // 对账同步调度器（mysql 模式专用，决策 10：任务持久化 + 启动恢复）
            // 复用 workerGroup 需要在此处创建，但 NameNodeServer 构造函数中才初始化 workerGroup。
            // 此处先传 null，在 NameNodeServer 构造后由 run() 启动。
            // 替代方案：直接在构造前创建 EventLoopGroup 传给 scheduler。
            io.netty.channel.EventLoopGroup syncWorkerGroup = new io.netty.channel.nio.NioEventLoopGroup();
            replicaSyncScheduler = new org.jnfs.namenode.replication.ReplicaSyncScheduler(
                    metadataManager.getDataSource(),
                    replicationGroupStore,
                    advertisedHost, port,
                    syncWorkerGroup,
                    null);   // 默认策略
            replicaSyncScheduler.start();
            LOG.info("对账同步调度器已启动（mysql 模式）");

            // 后台文件类型嗅探调度器（独立 workerGroup，兜底 file_type/file_size，不碰上传下载链路）
            io.netty.channel.EventLoopGroup fileTypeGroup = new io.netty.channel.nio.NioEventLoopGroup();
            fileTypeDetectScheduler = new FileTypeDetectScheduler(metadataManager.getDataSource(), fileTypeGroup);
            fileTypeDetectScheduler.start();
            LOG.info("文件类型嗅探调度器已启动（mysql 模式）");
        } else {
            // --- H2 嵌入式文件库分支（file 模式退役，旧 file 配置经 StorageMode.fromConfig 落到 H2） ---
            LOG.info("使用 H2 嵌入式文件库元数据存储（file 模式已退役）");

            File logFile = new File(dataDir, MigrationRunner.METADATA_LOG_FILE);
            boolean hasFileLog = logFile.exists();

            // 1. 若存在 file 历史日志，先规整为 V1（FileV0ToV1 补稳定 storageId）
            if (hasFileLog) {
                MigrationResult fileMigration = MigrationRunner.run(StorageMode.FILE, dataDir, null);
                if (fileMigration.isFailed()) {
                    LOG.error("file 日志迁移失败，拒绝启动。原因: {}", fileMigration.getMessage());
                    System.exit(2);
                }
                LOG.info("file 日志已规整: {}", fileMigration.getMessage());
            }

            // 2. 建单一 H2 数据源（迁移 + 业务共用，生命周期由 H2MetadataManager 接管）
            //    h2Path 非空则用之，空则 DataDirResolver 默认解析（与 StorageConfig 7 字段契约一致）
            File h2Dir = (storageCfg.h2Path != null && !storageCfg.h2Path.isEmpty())
                    ? new File(storageCfg.h2Path)
                    : dataDir;
            if (!h2Dir.exists() && !h2Dir.mkdirs()) {
                LOG.error("H2 数据目录不存在且无法创建: {}，拒绝启动", h2Dir.getAbsolutePath());
                System.exit(2);
                return; // unreachable，仅为编译器满足
            }
            com.zaxxer.hikari.HikariDataSource h2Ds = null;
            try {
                h2Ds = H2MetadataManager.createLocalDataSource(h2Dir);

                // 3. H2 迁移链：建 schema_version + 全部表，写版本 5
                //    dataDir（APP_HOME）传入仅用于 file 模式步骤占位；H2 步骤只用 dataSource
                MigrationResult h2Migration = MigrationRunner.run(StorageMode.H2, dataDir, h2Ds);
                if (h2Migration.isFailed()) {
                    LOG.error("H2 数据迁移失败，拒绝启动。原因: {}", h2Migration.getMessage());
                    System.exit(2);
                    return; // unreachable，仅为编译器满足
                }
                LOG.info("H2 数据迁移: {}", h2Migration.getMessage());

                // 4. file→H2 自动导入（log 存在且无 file_to_h2_imported 标记；per-row 幂等 + 原子标记）
                if (hasFileLog) {
                    try {
                        FileToH2Importer.importIfApplicable(dataDir, h2Ds);
                    } catch (Exception e) {
                        LOG.error("file→H2 自动导入失败，拒绝启动。原因: {}", e.getMessage(), e);
                        System.exit(2);
                        return; // unreachable，仅为编译器满足
                    }
                }

                // 5. 构造 H2MetadataManager（父类构造时执行锚点表 DDL 兜底）
                metadataManager = new H2MetadataManager(h2Ds);
            } catch (Exception e) {
                LOG.error("H2 初始化失败，拒绝启动。原因: {}", e.getMessage(), e);
                if (h2Ds != null) {
                    h2Ds.close();
                }
                System.exit(2);
                return; // unreachable，仅为编译器满足
            }

            // 不删 namenode_meta.log：作为回滚安全网 + FileToH2Importer 导入源，保留原始文件

            // 冗余组件（与 mysql 分支对齐：H2 同机多磁盘多副本；未配置冗余组时 selectReplicaTargets
            // 自动降级单副本，与旧行为一致）。H2 迁移链已建齐全部冗余表（MysqlV1ToV2/V2ToV3/V3ToV4 声明 supports H2）。
            replicationGroupStore = new ReplicationGroupStore(metadataManager.getDataSource());
            replicationGroupStore.start();
            LOG.info("冗余组配置缓存已启动（h2 模式，多副本可用）");

            io.netty.channel.EventLoopGroup syncWorkerGroup = new io.netty.channel.nio.NioEventLoopGroup();
            replicaSyncScheduler = new org.jnfs.namenode.replication.ReplicaSyncScheduler(
                    metadataManager.getDataSource(),
                    replicationGroupStore,
                    advertisedHost, port,
                    syncWorkerGroup,
                    null);   // 默认策略
            replicaSyncScheduler.start();
            LOG.info("对账同步调度器已启动（h2 模式）");

            // 后台文件类型嗅探调度器（独立 workerGroup，兜底 file_type/file_size，不碰上传下载链路）
            io.netty.channel.EventLoopGroup fileTypeGroup = new io.netty.channel.nio.NioEventLoopGroup();
            fileTypeDetectScheduler = new FileTypeDetectScheduler(metadataManager.getDataSource(), fileTypeGroup);
            fileTypeDetectScheduler.start();
            LOG.info("文件类型嗅探调度器已启动（h2 模式）");
        }

        // --- 初始化 MetadataCacheManager ---
        MetadataCacheManager cacheManager = new MetadataCacheManager(metadataManager, cacheEnabled, cacheMaxSize);

        // 注入到 Handler (recover 可能因日志损坏抛 IOException，拒绝启动)
        try {
            NameNodeHandler.initMetadataManager(metadataManager, cacheManager);
        } catch (Exception e) {
            LOG.error("元数据恢复失败，拒绝启动。原因: {}", e.getMessage());
            System.exit(2);
        }

        // 注入冗余组配置缓存到 Handler
        NameNodeHandler.initReplicationGroupStore(replicationGroupStore);

        // 注入对账同步调度器到 Handler（DATA_REPLICA_COMMIT 登记时使用）
        NameNodeHandler.initReplicaSyncScheduler(replicaSyncScheduler);

        // 注入排空节点集合到 Handler（§6.2：JDBC 模式从 node_drain 表加载；H2 无数据返回空集合，安全）
        if (metadataManager.isJdbcBacked()) {
            java.util.Set<String> drained = loadDrainedNodes(metadataManager.getDataSource());
            NameNodeHandler.initDrainedNodes(drained);
        } else {
            NameNodeHandler.initDrainedNodes(java.util.Collections.emptySet());
        }

        new NameNodeServer(port, advertisedHost, nodeId, registryAddresses,
                replicationGroupStore, replicaSyncScheduler, fileTypeDetectScheduler, metadataManager).run();
    }

    /**
     * 启动期加载排空节点集合（§6.2）。
     * <p>
     * 读 node_drain 表 drain_status=1 的 node_id，装入 HashSet 返回。
     * 读失败仅 warn 并返回空集——drain 是运维态，读失败不应阻断 NameNode 启动。
     *
     * @param ds 元数据库 DataSource（mysql 模式）
     * @return drain_status=1 的 node_id 集合；读失败返回空集
     */
    private static java.util.Set<String> loadDrainedNodes(javax.sql.DataSource ds) {
        java.util.Set<String> result = new java.util.HashSet<>();
        try (java.sql.Connection conn = ds.getConnection();
             java.sql.PreparedStatement stmt = conn.prepareStatement(
                     "SELECT node_id FROM node_drain WHERE drain_status = 1");
             java.sql.ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                result.add(rs.getString("node_id"));
            }
        } catch (Exception e) {
            LOG.error("加载 drained 节点集合失败，drainedNodes 将为空（不阻断启动）: {}", e.getMessage(), e);
        }
        return result;
    }

    /**
     * 启动期从 Registry 拉取 storage 配置（AES 加密传输）。
     * <p>
     * 多地址 failover：任一 Registry 成功即返回；全部失败按 {@link #STORAGE_CONFIG_FETCH_RETRY_INTERVAL_MS}
     * 间隔重试，直至 {@link #STORAGE_CONFIG_FETCH_MAX_WAIT_MS} 预算耗尽，此时抛异常由调用方拒绝启动。
     * 重试是为了容忍 Registry 与 NameNode 同时启动的时序竞态（Registry H2 首次建库可能耗时数秒到十几秒）。
     * 解密失败（HMAC 校验不通过 / 密钥不匹配）也会抛异常，安全失败。
     *
     * @param addresses Registry 地址列表
     * @return 解析后的 StorageConfig
     */
    private static StorageConfig fetchStorageConfigFromRegistry(List<InetSocketAddress> addresses) {
        NioEventLoopGroup group = new NioEventLoopGroup();
        long deadline = System.currentTimeMillis() + STORAGE_CONFIG_FETCH_MAX_WAIT_MS;
        try {
            while (true) {
                for (InetSocketAddress addr : addresses) {
                    StorageConfigFetchHandler handler = new StorageConfigFetchHandler();
                    try {
                        Bootstrap b = NettyClientBootstrap.createWithHandler(group, handler);
                        Channel ch = NettyClientBootstrap.connectSync(b,
                                addr.getHostString(), addr.getPort(), 6000);
                        try {
                            Packet req = new Packet();
                            req.setCommandType(CommandType.REGISTRY_GET_STORAGE_CONFIG);
                            req.setToken(Constants.getValidToken());
                            ch.writeAndFlush(req);

                            Packet resp = handler.waitResponse(5000);
                            if (resp != null
                                    && resp.getCommandType() == CommandType.REGISTRY_RESPONSE_STORAGE_CONFIG) {
                                byte[] plain = new SecurityUtil(SecurityConfig.getAesKey())
                                        .decryptBytes(resp.getData());
                                return StorageConfig.parse(new String(plain, StandardCharsets.UTF_8));
                            }
                            LOG.warn("Registry ({}) 返回异常响应: {}",
                                    addr, resp != null ? resp.getCommandType() : "null");
                        } finally {
                            ch.close().sync();
                        }
                    } catch (Exception e) {
                        LOG.warn("从 Registry 拉取 storage 配置失败 ({}): {}", addr, e.getMessage());
                    }
                }
                if (System.currentTimeMillis() >= deadline) {
                    break;
                }
                LOG.info("所有 Registry 均不可达，{}ms 后重试（剩余 {}ms）...",
                        STORAGE_CONFIG_FETCH_RETRY_INTERVAL_MS, deadline - System.currentTimeMillis());
                try {
                    Thread.sleep(STORAGE_CONFIG_FETCH_RETRY_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            throw new RuntimeException("所有 Registry 地址均不可达，无法拉取 storage 配置");
        } finally {
            group.shutdownGracefully();
        }
    }

    // --- 内部 Discovery Handler ---
    private static class DiscoveryHandler extends SimpleChannelInboundHandler<Packet> {

        private final CompletableFuture<Boolean> promise;

        public DiscoveryHandler(CompletableFuture<Boolean> promise) {
            this.promise = promise;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            if (packet.getCommandType() == CommandType.REGISTRY_RESPONSE_DATANODES) {
                String nodesStr = new String(packet.getData(), StandardCharsets.UTF_8);
                if (!nodesStr.isEmpty()) {
                    String[] nodes = nodesStr.split(",");
                    LOG.info("更新 DataNode 列表: {}", Arrays.toString(nodes));
                    List<String> nodeList = Arrays.asList(nodes);
                    // 必须先更新 NodeAddressResolver 映射：
                    // initDataNodes 首次调用会同步触发 backfillNodeIds()，
                    // 若映射尚未建立，host:port 无法解析为 node_id，回填将为 0 行
                    NodeAddressResolver.updateMappingFromDataNodes(nodeList);
                    NameNodeHandler.initDataNodes(nodeList);
                } else {
                    LOG.info("当前无活跃 DataNode");
                    NameNodeHandler.initDataNodes(null);
                }
                // 通知完成
                promise.complete(true);
            }
            // 注意：这里不再 ctx.close()，因为连接要复用
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            promise.completeExceptionally(cause);
            // 发生异常时可以关闭连接，Pool 会检测到
            ctx.close();
        }
    }

    /**
     * 从 Registry 拉取的存储配置（值对象，不可变）。
     * <p>
     * 7 字段契约（与 {@code jnfs-registry} {@code serializeStorageConfig} 严格一致）：
     * {@code mode|mysqlHost|mysqlPort|mysqlDatabase|mysqlUser|mysqlPassword|h2Path}
     * <ul>
     *   <li>mysql 模式：{@code mysql|host|port|db|user|password|}（第 7 位 h2Path 为空）</li>
     *   <li>h2 模式：{@code h2||||||<h2Path 或空>}（2-6 位为空；第 7 位为 h2 路径，空表示
     *       NameNode 侧用 {@code DataDirResolver} 默认解析）</li>
     *   <li>file（已退役）/其它：7 字段按 file-like 处理</li>
     * </ul>
     * 向后兼容：旧 6 字段 payload 缺第 7 位，h2Path 视为空。
     */
    private static final class StorageConfig {
        final String mode;          // "file" | "mysql" | "h2"
        final String mysqlHost;
        final int mysqlPort;
        final String mysqlDatabase;
        final String mysqlUser;
        final String mysqlPassword;
        final String h2Path;        // h2 模式下的 h2 文件库路径；空 = DataDirResolver 默认解析

        StorageConfig(String mode, String mysqlHost, int mysqlPort, String mysqlDatabase,
                      String mysqlUser, String mysqlPassword, String h2Path) {
            this.mode = mode;
            this.mysqlHost = mysqlHost;
            this.mysqlPort = mysqlPort;
            this.mysqlDatabase = mysqlDatabase;
            this.mysqlUser = mysqlUser;
            this.mysqlPassword = mysqlPassword;
            this.h2Path = h2Path;
        }

        /**
         * 解析 7 字段 payload：{@code mode|mysqlHost|mysqlPort|mysqlDatabase|mysqlUser|mysqlPassword|h2Path}
         * <p>
         * 用 {@code split("\\|", -1)} 保留尾空串，确保第 7 位 h2Path 存在时能被读出。
         * 向后兼容：旧 6 字段 payload 缺第 7 位，h2Path 视为空。
         */
        static StorageConfig parse(String payload) {
            String[] parts = payload.split("\\|", -1);
            // 反转义：unesc 会跳过未转义的 pipe（split 后不会出现在字段值中）
            // 但字段内通过 Registry 侧 esc() 转义为 \| 的 pipe 需还原
            String mode = parts[0];
            if ("mysql".equalsIgnoreCase(mode)) {
                String host = unesc((parts.length > 1 && !parts[1].isEmpty()) ? parts[1] : "localhost");
                int port = parts.length > 2 ? parseIntSafe(parts[2], 3306) : 3306;
                String db = unesc((parts.length > 3 && !parts[3].isEmpty()) ? parts[3] : "jnfs");
                String user = unesc((parts.length > 4 && !parts[4].isEmpty()) ? parts[4] : "root");
                String password = unesc(parts.length > 5 ? parts[5] : "");
                String h2 = unesc(parts.length >= 7 ? parts[6] : ""); // 第 7 位 h2Path（mysql 模式固定空）
                return new StorageConfig(mode, host, port, db, user, password, h2);
            }
            // h2 / file / 其它：mysql 字段忽略，仅第 7 位 h2Path 有意义
            String h2 = unesc(parts.length >= 7 ? parts[6] : "");
            return new StorageConfig(mode, null, 0, null, null, null, h2);
        }

        /**
         * 反转义：Registry 侧 {@code esc()} 将字段值内的 {@code |} 转义为 {@code \\|}。
         * split("\\|", -1) 不会拆开 {@code \\|} 中的 pipe（因为紧随反斜杠），故反转义只需
         * 把 {@code \\|} 替换回 {@code |}。
         */
        private static String unesc(String v) {
            return v == null ? "" : v.replace("\\|", "|");
        }

        private static int parseIntSafe(String s, int def) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                return def;
            }
        }
    }

    /**
     * 启动期拉取 storage 配置的临时 Handler，用 CompletableFuture 同步等待响应。
     * 仅在 main() 单线程下触发，无并发问题。每次连接使用新实例（避免 static 状态被重试复用）。
     */
    private static class StorageConfigFetchHandler extends SimpleChannelInboundHandler<Packet> {

        private final CompletableFuture<Packet> promise = new CompletableFuture<>();

        Packet waitResponse(long timeoutMs) throws Exception {
            return promise.get(timeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            if (packet.getCommandType() == CommandType.REGISTRY_RESPONSE_STORAGE_CONFIG
                    || packet.getCommandType() == CommandType.ERROR) {
                promise.complete(packet);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            promise.completeExceptionally(cause);
            ctx.close();
        }
    }
}
