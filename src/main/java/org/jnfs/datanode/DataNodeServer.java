package org.jnfs.datanode;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jnfs.common.CommandType;
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DataNode 服务启动类
 * 负责实际的文件存储
 * 
 * 升级：集成注册中心自动注册与心跳，并上报磁盘剩余空间
 */
public class DataNodeServer {

    private static final String VALID_TOKEN = "jnfs-secure-token-2025";

    private final int port;
    private final String storagePath;
    private final String registryHost;
    private final int registryPort;

    public DataNodeServer(int port, String storagePath, String registryHost, int registryPort) {
        this.port = port;
        this.storagePath = storagePath;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public void run() throws Exception {
        // 启动后台线程负责注册和心跳
        startHeartbeatThread();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(); 
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(32);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                     ch.pipeline().addLast(businessGroup, new DataNodeHandler(storagePath));
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 4096) 
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .childOption(ChannelOption.SO_RCVBUF, 1024 * 1024) 
             .childOption(ChannelOption.SO_SNDBUF, 1024 * 1024);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS DataNode 启动成功 (Registry Mode)，端口: " + port);
            System.out.println("存储目录: " + storagePath);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            businessGroup.shutdownGracefully();
        }
    }

    private void startHeartbeatThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        // 初始延迟 2 秒，之后每 5 秒发送一次心跳 (包含注册逻辑)
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeatToRegistry();
            } catch (Exception e) {
                System.err.println("发送心跳失败: " + e.getMessage());
            }
        }, 2, 5, TimeUnit.SECONDS);
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

            // Payload: host:port|freeSpace
            File storeDir = new File(storagePath);
            if (!storeDir.exists()) {
                storeDir.mkdirs();
            }
            long freeSpace = storeDir.getFreeSpace();
            
            // 实际上应该汇报对外服务的 IP，这里简单使用 localhost
            // 格式: localhost:8080|10737418240
            String payload = "localhost:" + port + "|" + freeSpace;
            
            Packet packet = new Packet();
            packet.setCommandType(CommandType.REGISTRY_HEARTBEAT);
            packet.setToken(VALID_TOKEN);
            packet.setData(payload.getBytes(StandardCharsets.UTF_8));
            
            channel.writeAndFlush(packet);
            
            f.channel().closeFuture().sync();
        } catch (Exception e) {
            System.err.println("连接注册中心失败: " + e.getMessage());
        } finally {
            group.shutdownGracefully();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("datanode.yml");
        
        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 8080);
        
        Map<String, Object> storageConfig = (Map<String, Object>) config.get("storage");
        String storagePath = (String) storageConfig.getOrDefault("path", "datanode_files");
        
        // 读取注册中心配置
        String regHost = "localhost";
        int regPort = 8000;
        if (config.containsKey("registry")) {
            Map<String, Object> regConfig = (Map<String, Object>) config.get("registry");
            regHost = (String) regConfig.getOrDefault("host", "localhost");
            regPort = (int) regConfig.getOrDefault("port", 8000);
        }
        
        System.out.println("使用注册中心: " + regHost + ":" + regPort);
        
        new DataNodeServer(port, storagePath, regHost, regPort).run();
    }
}
