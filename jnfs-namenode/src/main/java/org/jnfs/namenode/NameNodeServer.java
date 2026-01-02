package org.jnfs.namenode;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jnfs.common.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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

    private static final String VALID_TOKEN = "jnfs-secure-token-2025";

    private final int port;
    private final String advertisedHost;
    private final String registryHost;
    private final int registryPort;

    public NameNodeServer(int port, String advertisedHost, String registryHost, int registryPort) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public void run() throws Exception {
        // 启动后台线程定期从注册中心拉取 DataNode 列表
        startDiscoveryThread();
        // 启动注册与心跳线程
        startRegistrationHeartbeatThread();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(16);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                     ch.pipeline().addLast(businessGroup, new NameNodeHandler());
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 1024)
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .childOption(ChannelOption.SO_RCVBUF, 64 * 1024)
             .childOption(ChannelOption.SO_SNDBUF, 64 * 1024);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS NameNode 启动成功 (Registry Mode)，端口: " + port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            businessGroup.shutdownGracefully();
        }
    }

    private void startRegistrationHeartbeatThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeatToRegistry();
            } catch (Exception e) {
                System.err.println("发送心跳失败: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    private void sendHeartbeatToRegistry() {
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

            ChannelFuture f = b.connect(registryHost, registryPort).sync();
            Channel channel = f.channel();

            String payload = advertisedHost + ":" + port;
            
            Packet packet = new Packet();
            packet.setCommandType(CommandType.REGISTRY_HEARTBEAT_NAMENODE);
            packet.setToken(VALID_TOKEN);
            packet.setData(payload.getBytes(StandardCharsets.UTF_8));
            
            channel.writeAndFlush(packet).sync();
            channel.close().sync();
        } catch (Exception e) {
            // System.err.println("注册/心跳失败: " + e.getMessage());
        } finally {
            group.shutdownGracefully();
        }
    }

    private void startDiscoveryThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                fetchDataNodesFromRegistry();
            } catch (Exception e) {
                System.err.println("从注册中心获取节点失败: " + e.getMessage());
            }
        }, 0, 10, TimeUnit.SECONDS); // 每10秒刷新一次
    }

    private void fetchDataNodesFromRegistry() {
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

            ChannelFuture f = b.connect(registryHost, registryPort).sync();
            Channel channel = f.channel();

            Packet request = new Packet();
            request.setCommandType(CommandType.REGISTRY_GET_DATANODES);
            request.setToken(VALID_TOKEN);
            channel.writeAndFlush(request);

            f.channel().closeFuture().sync();
        } catch (Exception e) {
            // e.printStackTrace();
            System.err.println("连接注册中心失败: " + e.getMessage());
        } finally {
            group.shutdownGracefully();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("namenode.yml");

        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 9090);
        // 如果没有配置 advertised_host，则自动获取本机 IP
        String advertisedHost = (String) serverConfig.getOrDefault("advertised_host", NetUtils.getLocalIp());

        // 读取注册中心配置
        String regHost = "localhost";
        int regPort = 8000;
        if (config.containsKey("registry")) {
            Map<String, Object> regConfig = (Map<String, Object>) config.get("registry");
            regHost = (String) regConfig.getOrDefault("host", "localhost");
            regPort = (int) regConfig.getOrDefault("port", 8000);
        }

        System.out.println("使用注册中心: " + regHost + ":" + regPort);
        System.out.println("对外广播地址: " + advertisedHost);

        // --- 初始化 MetadataManager ---
        MetadataManager metadataManager = null;
        if (config.containsKey("metadata")) {
            Map<String, Object> metaConfig = (Map<String, Object>) config.get("metadata");
            String mode = (String) metaConfig.getOrDefault("mode", "file");

            if ("mysql".equalsIgnoreCase(mode)) {
                System.out.println("使用 MySQL 元数据存储");
                Map<String, Object> mysqlConfig = (Map<String, Object>) metaConfig.get("mysql");
                String dbHost = (String) mysqlConfig.getOrDefault("host", "localhost");
                int dbPort = (int) mysqlConfig.getOrDefault("port", 3306);
                String dbName = (String) mysqlConfig.getOrDefault("database", "jnfs");
                String user = (String) mysqlConfig.getOrDefault("user", "root");
                String password = (String) mysqlConfig.getOrDefault("password", "");

                metadataManager = new MySQLMetadataManager(dbHost, dbPort, dbName, user, password);
            } else {
                System.out.println("使用本地文件元数据存储");
                metadataManager = new MetadataManager();
            }
        } else {
            System.out.println("默认使用本地文件元数据存储");
            metadataManager = new MetadataManager();
        }

        // 注入到 Handler
        NameNodeHandler.initMetadataManager(metadataManager);

        new NameNodeServer(port, advertisedHost, regHost, regPort).run();
    }

    // --- 内部 Discovery Handler ---
    private static class DiscoveryHandler extends SimpleChannelInboundHandler<Packet> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            if (packet.getCommandType() == CommandType.REGISTRY_RESPONSE_DATANODES) {
                String nodesStr = new String(packet.getData(), StandardCharsets.UTF_8);
                if (!nodesStr.isEmpty()) {
                    String[] nodes = nodesStr.split(",");
                    System.out.println("更新 DataNode 列表: " + Arrays.toString(nodes));
                    NameNodeHandler.initDataNodes(Arrays.asList(nodes));
                } else {
                    System.out.println("当前无活跃 DataNode");
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
