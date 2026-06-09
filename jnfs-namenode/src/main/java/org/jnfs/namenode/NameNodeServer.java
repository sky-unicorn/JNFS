package org.jnfs.namenode;

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

import org.jnfs.common.ChannelPoolUtils;
import org.jnfs.common.CommandType;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.Constants;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.HeartbeatSender;
import org.jnfs.common.NetUtils;
import org.jnfs.common.NettyServerUtils;
import org.jnfs.common.Packet;
import org.jnfs.common.ServerShutdownHelper;
import org.jnfs.common.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(NameNodeServer.class);

    private final int port;
    private final String advertisedHost;
    // 支持多个注册中心地址
    private final List<InetSocketAddress> registryAddresses;

    // 复用 EventLoopGroup
    private final EventLoopGroup workerGroup;
    // 连接池映射
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> registryPoolMap;

    // 调度器 (使用 Daemon 线程)
    private final ScheduledExecutorService heartbeatScheduler;
    private final ScheduledExecutorService discoveryScheduler;

    public NameNodeServer(int port, String advertisedHost, List<InetSocketAddress> registryAddresses) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.registryAddresses = registryAddresses;

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
        ServerShutdownHelper.shutdownAll(LOG, "NameNodeServer", running,
                new ScheduledExecutorService[]{heartbeatScheduler, discoveryScheduler},
                registryPoolMap, workerGroup);
    }

    private void startRegistrationHeartbeatThread() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                String payload = advertisedHost + ":" + port;
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

        // 加载安全配置

        // --- 初始化 MetadataManager ---
        MetadataManager metadataManager = null;

        // 缓存配置默认值
        boolean cacheEnabled = true;
        long cacheMaxSize = 100000L;

        if (config.containsKey("metadata")) {
            Map<String, Object> metaConfig = (Map<String, Object>) config.get("metadata");
            String mode = (String) metaConfig.getOrDefault("mode", "file");

            // 读取缓存配置
            if (metaConfig.containsKey("cache")) {
                Map<String, Object> cacheConfig = (Map<String, Object>) metaConfig.get("cache");
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
                Map<String, Object> mysqlConfig = (Map<String, Object>) metaConfig.get("mysql");
                String dbHost = (String) mysqlConfig.getOrDefault("host", "localhost");
                int dbPort = (int) mysqlConfig.getOrDefault("port", 3306);
                String dbName = (String) mysqlConfig.getOrDefault("database", "jnfs");
                String user = (String) mysqlConfig.getOrDefault("user", "root");
                String password = (String) mysqlConfig.getOrDefault("password", "");

                metadataManager = new MySQLMetadataManager(dbHost, dbPort, dbName, user, password);
            } else {
                LOG.info("使用本地文件元数据存储");
                metadataManager = new MetadataManager();
            }
        } else {
            LOG.info("默认使用本地文件元数据存储");
            metadataManager = new MetadataManager();
        }

        // --- 初始化 MetadataCacheManager ---
        MetadataCacheManager cacheManager = new MetadataCacheManager(metadataManager, cacheEnabled, cacheMaxSize);

        // 注入到 Handler
        NameNodeHandler.initMetadataManager(metadataManager, cacheManager);

        new NameNodeServer(port, advertisedHost, registryAddresses).run();
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
                    NameNodeHandler.initDataNodes(Arrays.asList(nodes));
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
}
