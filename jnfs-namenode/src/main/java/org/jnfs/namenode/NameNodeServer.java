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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.CopyOnWriteArrayList;
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
                packet.setToken(VALID_TOKEN);
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
                System.err.println("从注册中心获取节点失败: " + e.getMessage());
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
                request.setToken(VALID_TOKEN);
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
        List<InetSocketAddress> registryAddresses = new ArrayList<>();
        String regHost = "localhost";
        int regPort = 5367;
        
        if (config.containsKey("registry")) {
            Map<String, Object> regConfig = (Map<String, Object>) config.get("registry");
            // 优先检查 'addresses' (list) 或 'address' (string, comma-separated)
            Object addressesObj = regConfig.get("addresses");
            if (addressesObj instanceof List) {
                List<String> addrList = (List<String>) addressesObj;
                for (String addr : addrList) {
                    parseAndAddAddress(addr, registryAddresses);
                }
            } else if (addressesObj instanceof String) {
                String[] addrs = ((String) addressesObj).split(",");
                for (String addr : addrs) {
                    parseAndAddAddress(addr, registryAddresses);
                }
            } else {
                // 兼容旧配置 host/port
                regHost = (String) regConfig.getOrDefault("host", "localhost");
                regPort = (int) regConfig.getOrDefault("port", 5367);
                registryAddresses.add(new InetSocketAddress(regHost, regPort));
            }
        } else {
            registryAddresses.add(new InetSocketAddress(regHost, regPort));
        }

        System.out.println("使用注册中心集群: " + registryAddresses);
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

        new NameNodeServer(port, advertisedHost, registryAddresses).run();
    }
    
    private static void parseAndAddAddress(String addr, List<InetSocketAddress> list) {
        try {
            String[] parts = addr.trim().split(":");
            if (parts.length == 2) {
                list.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
            }
        } catch (Exception e) {
            System.err.println("解析注册中心地址失败: " + addr);
        }
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
