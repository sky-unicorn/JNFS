package org.jnfs.namenode;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jnfs.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * NameNode 服务启动类
 * 负责管理元数据和调度 DataNode
 *
 * 升级：集成注册中心发现机制，并根据配置初始化元数据管理器
 */
public class NameNodeServer {

    private static final Logger LOG = LoggerFactory.getLogger(NameNodeServer.class);

    private final int port;
    private final String advertisedHost;
    // 支持多个注册中心地址
    private final List<InetSocketAddress> registryAddresses;

    public NameNodeServer(int port, String advertisedHost, List<InetSocketAddress> registryAddresses) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.registryAddresses = registryAddresses;
    }

    public void run() throws Exception {
        // 启动后台线程定期从注册中心拉取 DataNode 列表
        startDiscoveryThread();
        // 启动注册与心跳线程
        startRegistrationHeartbeatThread();

        // 共享的 Handler 实例
        NameNodeHandler sharedHandler = new NameNodeHandler();
        
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(16);
        
        // 使用 NettyServerUtils 启动服务，传入共享 Handler
        NettyServerUtils.start("NameNode", port, sharedHandler, businessGroup);
    }

    private void startRegistrationHeartbeatThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeatToRegistry();
            } catch (Exception e) {
                LOG.error("发送心跳失败: {}", e.getMessage(), e);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    private void sendHeartbeatToRegistry() {
        // 向所有配置的 Registry 发送心跳 (广播模式，确保所有节点都收到)
        for (InetSocketAddress addr : registryAddresses) {
            EventLoopGroup group = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(group)
                 .channel(NioSocketChannel.class)
                 .handler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) {
                         ch.pipeline().addLast(new PacketEncoder());
                     }
                 });

                // 快速连接超时，避免阻塞
                ChannelFuture f = b.connect(addr).sync();
                Channel channel = f.channel();

                String payload = advertisedHost + ":" + port;

                Packet packet = new Packet();
                packet.setCommandType(CommandType.REGISTRY_HEARTBEAT_NAMENODE);
                packet.setToken(Constants.VALID_TOKEN);
                packet.setData(payload.getBytes(StandardCharsets.UTF_8));

                channel.writeAndFlush(packet).sync();
                channel.close().sync();
            } catch (Exception e) {
                // System.err.println("注册/心跳失败 (" + addr + "): " + e.getMessage());
            } finally {
                group.shutdownGracefully();
            }
        }
    }

    private void startDiscoveryThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
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
            EventLoopGroup group = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                DiscoveryHandler handler = new DiscoveryHandler();
                b.group(group)
                 .channel(NioSocketChannel.class)
                 .handler(new ChannelInitializer<SocketChannel>() {
                     @Override
                     protected void initChannel(SocketChannel ch) {
                         ch.pipeline().addLast(new PacketDecoder());
                         ch.pipeline().addLast(new PacketEncoder());
                         ch.pipeline().addLast(handler);
                     }
                 });

                ChannelFuture f = b.connect(addr).sync();
                Channel channel = f.channel();

                Packet request = new Packet();
                request.setCommandType(CommandType.REGISTRY_GET_DATANODES);
                request.setToken(Constants.VALID_TOKEN);
                channel.writeAndFlush(request);

                f.channel().closeFuture().sync();

                // 如果成功获取数据，直接返回，不再尝试下一个
                return;
            } catch (Exception e) {
                // System.err.println("连接注册中心失败 (" + addr + "): " + e.getMessage());
            } finally {
                group.shutdownGracefully();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("namenode.yml");

        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 5368);
        // 如果没有配置 advertised_host，则自动获取本机 IP
        String advertisedHost = (String) serverConfig.getOrDefault("advertised_host", NetUtils.getLocalIp());

        // 读取注册中心配置 (支持逗号分隔的多个地址)
        List<InetSocketAddress> registryAddresses = ConfigUtil.parseRegistryAddresses(config);

        LOG.info("使用注册中心集群: {}", registryAddresses);
        LOG.info("对外广播地址: {}", advertisedHost);

        // --- 初始化 MetadataManager ---
        MetadataManager metadataManager = null;
        
        // 缓存配置默认值
        boolean cacheEnabled = true;
        long cacheMaxSize = 100000L;
        String cacheWritePolicy = "sync";

        if (config.containsKey("metadata")) {
            Map<String, Object> metaConfig = (Map<String, Object>) config.get("metadata");
            String mode = (String) metaConfig.getOrDefault("mode", "file");

            // 读取缓存配置
            if (metaConfig.containsKey("cache")) {
                Map<String, Object> cacheConfig = (Map<String, Object>) metaConfig.get("cache");
                cacheEnabled = (boolean) cacheConfig.getOrDefault("enabled", true);
                // 处理 Integer 到 Long 的转换 (snakeyaml 可能返回 Integer)
                Object maxSizeObj = cacheConfig.getOrDefault("max-size", 100000);
                if (maxSizeObj instanceof Integer) {
                    cacheMaxSize = ((Integer) maxSizeObj).longValue();
                } else if (maxSizeObj instanceof Long) {
                    cacheMaxSize = (Long) maxSizeObj;
                }
                cacheWritePolicy = (String) cacheConfig.getOrDefault("write-policy", "sync");
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
        MetadataCacheManager cacheManager = new MetadataCacheManager(metadataManager, cacheEnabled, cacheMaxSize, cacheWritePolicy);

        // 注入到 Handler
        NameNodeHandler.initMetadataManager(metadataManager, cacheManager);

        new NameNodeServer(port, advertisedHost, registryAddresses).run();
    }

    // --- 内部 Discovery Handler ---
    private static class DiscoveryHandler extends SimpleChannelInboundHandler<Packet> {
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
            }
            ctx.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
