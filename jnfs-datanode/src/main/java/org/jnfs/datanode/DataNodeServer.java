package org.jnfs.datanode;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jnfs.common.ChannelPoolUtils;
import org.jnfs.common.CommandType;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.HeartbeatSender;
import org.jnfs.common.NetUtils;
import org.jnfs.common.NettyServerUtils;
import org.jnfs.common.NodeIdManager;
import org.jnfs.common.ServerShutdownHelper;
import org.jnfs.common.SecurityConfig;
import org.jnfs.common.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.hutool.core.io.FileUtil;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * DataNode 服务启动类
 * 负责实际的文件存储
 *
 * 升级：添加后台垃圾回收线程 (GC)
 * 优化：使用连接池复用 Registry 连接 (方案 B)
 */
public class DataNodeServer {

    private static final Logger LOG = LoggerFactory.getLogger(DataNodeServer.class);

    private final int port;
    private final String advertisedHost;
    private final String nodeId;
    private final List<String> storagePaths;
    // 支持多个注册中心地址
    private final List<InetSocketAddress> registryAddresses;

    // 复用 EventLoopGroup
    private final EventLoopGroup workerGroup;
    // 连接池映射
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> registryPoolMap;

    // 调度器 (使用 Daemon 线程)
    private final ScheduledExecutorService heartbeatScheduler;
    private final ScheduledExecutorService gcScheduler;

    public DataNodeServer(int port, String advertisedHost, String nodeId, List<String> storagePaths, List<InetSocketAddress> registryAddresses) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.nodeId = nodeId;
        this.storagePaths = storagePaths;
        this.registryAddresses = registryAddresses;

        // 初始化共享的 Worker Group
        this.workerGroup = new NioEventLoopGroup();

        // 初始化连接池 (使用通用工具类)
        this.registryPoolMap = ChannelPoolUtils.createDefaultPoolMap(workerGroup);

        // 初始化调度器 (使用统一的 Daemon 线程工厂)
        this.heartbeatScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("DataNode-Heartbeat"));
        this.gcScheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("DataNode-GC"));
    }

    // 运行标志
    private final AtomicBoolean running = new AtomicBoolean(true);

    public void run() throws Exception {
        // 启动后台线程负责注册和心跳
        startHeartbeatThread();
        // 启动垃圾回收线程
        startGarbageCollectorThread();

        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(32);

        // 注册 Shutdown Hook (必须在 start 阻塞前注册)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown hook triggered...");
            shutdown();
        }));

        try {
            // 使用 NettyServerUtils 启动服务
            // 关键修复: 传入 Supplier 以便为每个连接创建新的 DataNodeHandler 实例
            // DataNodeHandler 是有状态的 (包含文件流)，绝对不能共享！
            NettyServerUtils.start("DataNode", port,
                () -> new DataNodeHandler(storagePaths),
                businessGroup);
        } finally {
            // 正常退出时的清理
            shutdown();
            businessGroup.shutdownGracefully();
        }
    }

    /**
     * 统一的资源释放方法，支持幂等调用
     */
    private void shutdown() {
        ServerShutdownHelper.shutdownAll(LOG, "DataNodeServer", running,
                new ScheduledExecutorService[]{heartbeatScheduler, gcScheduler},
                registryPoolMap, workerGroup);
    }

    private void startHeartbeatThread() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                long totalFreeSpace = computeTotalFreeSpace();
                // 新格式: node_id|host:port|freeSpace
                String payload = nodeId + "|" + advertisedHost + ":" + port + "|" + totalFreeSpace;
                HeartbeatSender.broadcastString(LOG, registryPoolMap, registryAddresses,
                        CommandType.REGISTRY_HEARTBEAT, addr -> payload);
            } catch (Exception e) {
                LOG.error("发送心跳失败: {}", e.getMessage(), e);
            }
        }, 2, 5, TimeUnit.SECONDS);
    }

    private long computeTotalFreeSpace() {
        long total = 0;
        for (String path : storagePaths) {
            File storeDir = new File(path);
            if (!storeDir.exists()) {
                storeDir.mkdirs();
            }
            total += storeDir.getFreeSpace();
        }
        return total;
    }

    /**
     * 垃圾回收线程：定期扫描并删除过期的 .tmp 文件
     */
    private void startGarbageCollectorThread() {
        // 每 1 小时执行一次 GC (测试时可缩短)
        gcScheduler.scheduleAtFixedRate(() -> {
            LOG.info("[GC] 开始执行垃圾回收...");
            try {
                cleanupTmpFiles();
            } catch (Exception e) {
                LOG.error("[GC] 执行失败: {}", e.getMessage(), e);
            }
        }, 1, 60, TimeUnit.MINUTES);
    }

    private void cleanupTmpFiles() throws IOException {
        long now = System.currentTimeMillis();
        // 过期时间：1 小时前的临时文件会被删除
        long expirationTime = 1 * 60 * 60 * 1000L;

        for (String path : storagePaths) {
            Path root = Paths.get(path);
            if (!Files.exists(root)) {
                continue;
            }

            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.toString().endsWith(DataNodeHandler.TMP_SUFFIX)) {
                        long lastModified = attrs.lastModifiedTime().toMillis();
                        if (now - lastModified > expirationTime) {
                            LOG.info("[GC] 删除过期临时文件: {}", file);
                            Files.delete(file);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("datanode.yml");

        // 初始化安全配置
        SecurityConfig.init("datanode.yml");

        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 5369);
        // 如果没有配置 advertised_host，则自动获取本机 IP (统一使用项目自带的 NetUtils)
        String advertisedHost = (String) serverConfig.getOrDefault("advertised_host", NetUtils.getLocalIp());

        Map<String, Object> storageConfig = (Map<String, Object>) config.get("storage");
        List<String> storagePaths = new ArrayList<>();

        if (storageConfig.containsKey("paths")) {
            List<String> paths = (List<String>) storageConfig.get("paths");
            for (String p : paths) {
                storagePaths.add(FileUtil.normalize(p));
            }
        } else if (storageConfig.containsKey("path")) {
            storagePaths.add(FileUtil.normalize((String) storageConfig.get("path")));
        } else {
            storagePaths.add("datanode_files");
        }

        List<InetSocketAddress> registryAddresses = ConfigUtil.parseRegistryAddresses(config);

        LOG.info("使用注册中心集群: {}", registryAddresses);
        LOG.info("对外广播地址: {}", advertisedHost);

        // 初始化 node_id (配置指定 > 本地文件 > 自动生成)
        String nodeId = NodeIdManager.initialize(serverConfig);
        LOG.info("节点ID: {}", nodeId);

        new DataNodeServer(port, advertisedHost, nodeId, storagePaths, registryAddresses).run();
    }
}
