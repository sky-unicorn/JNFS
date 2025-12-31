package org.jnfs.datanode;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.net.NetUtil;
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DataNode 服务启动类
 * 负责实际的文件存储
 *
 * 升级：添加后台垃圾回收线程 (GC)
 */
public class DataNodeServer {

    private static final String VALID_TOKEN = "jnfs-secure-token-2025";

    private final int port;
    private final String advertisedHost;
    private final String storagePath;
    private final String registryHost;
    private final int registryPort;

    // 心跳专用的 EventLoopGroup，避免每次创建销毁
    private final EventLoopGroup heartbeatGroup = new NioEventLoopGroup(1);
    private Channel heartbeatChannel;
    private final AtomicBoolean isConnecting = new AtomicBoolean(false);

    public DataNodeServer(int port, String advertisedHost, String storagePath, String registryHost, int registryPort) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.storagePath = storagePath;
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public void run() throws Exception {
        // 启动后台线程负责注册和心跳
        startHeartbeatThread();
        // 启动垃圾回收线程
        startGarbageCollectorThread();

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
            heartbeatGroup.shutdownGracefully();
        }
    }

    private void startHeartbeatThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeatToRegistry();
            } catch (Exception e) {
                System.err.println("发送心跳失败: " + e.getMessage());
            }
        }, 2, 5, TimeUnit.SECONDS);
    }

    /**
     * 垃圾回收线程：定期扫描并删除过期的 .tmp 文件
     */
    private void startGarbageCollectorThread() {
        ScheduledExecutorService gcScheduler = Executors.newSingleThreadScheduledExecutor();
        // 每 1 小时执行一次 GC (测试时可缩短)
        gcScheduler.scheduleAtFixedRate(() -> {
            System.out.println("[GC] 开始执行垃圾回收...");
            try {
                cleanupTmpFiles();
            } catch (Exception e) {
                System.err.println("[GC] 执行失败: " + e.getMessage());
            }
        }, 1, 60, TimeUnit.MINUTES);
    }

    private void cleanupTmpFiles() throws IOException {
        Path root = Paths.get(storagePath);
        if (!Files.exists(root)) {
            return;
        }

        long now = System.currentTimeMillis();
        // 过期时间：1 小时前的临时文件会被删除
        long expirationTime = 1 * 60 * 60 * 1000L;

        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(DataNodeHandler.TMP_SUFFIX)) {
                    long lastModified = attrs.lastModifiedTime().toMillis();
                    if (now - lastModified > expirationTime) {
                        System.out.println("[GC] 删除过期临时文件: " + file);
                        Files.delete(file);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void sendHeartbeatToRegistry() {
        // 如果连接已建立且活跃，直接发送
        if (heartbeatChannel != null && heartbeatChannel.isActive()) {
            doSendHeartbeat(heartbeatChannel);
            return;
        }

        // 如果正在重连中，跳过本次
        if (isConnecting.get()) {
            return;
        }

        isConnecting.set(true);
        Bootstrap b = new Bootstrap();
        b.group(heartbeatGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.SO_KEEPALIVE, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new PacketEncoder());
                 // 简单的 Handler 处理断开重连或响应
                 ch.pipeline().addLast(new SimpleChannelInboundHandler<Packet>() {
                    @Override
                    protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
                        // 忽略响应
                    }
                 });
             }
         });

        b.connect(registryHost, registryPort).addListener((ChannelFuture future) -> {
            isConnecting.set(false);
            if (future.isSuccess()) {
                System.out.println("注册中心连接建立成功");
                heartbeatChannel = future.channel();
                doSendHeartbeat(heartbeatChannel);
            } else {
                System.err.println("连接注册中心失败: " + future.cause().getMessage());
            }
        });
    }

    private void doSendHeartbeat(Channel channel) {
        File storeDir = new File(storagePath);
        if (!storeDir.exists()) {
            storeDir.mkdirs();
        }
        long freeSpace = storeDir.getFreeSpace();

        String payload = advertisedHost + ":" + port + "|" + freeSpace;

        Packet packet = new Packet();
        packet.setCommandType(CommandType.REGISTRY_HEARTBEAT);
        packet.setToken(VALID_TOKEN);
        packet.setData(payload.getBytes(StandardCharsets.UTF_8));

        channel.writeAndFlush(packet);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("datanode.yml");

        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 8080);
        String advertisedHost = (String) serverConfig.getOrDefault("advertised_host", NetUtil.getLocalhostStr());

        Map<String, Object> storageConfig = (Map<String, Object>) config.get("storage");
        String storagePath = (String) storageConfig.getOrDefault("path", "datanode_files");
        storagePath = FileUtil.normalize(storagePath);

        String regHost = "localhost";
        int regPort = 8000;
        if (config.containsKey("registry")) {
            Map<String, Object> regConfig = (Map<String, Object>) config.get("registry");
            regHost = (String) regConfig.getOrDefault("host", "localhost");
            regPort = (int) regConfig.getOrDefault("port", 8000);
        }

        System.out.println("使用注册中心: " + regHost + ":" + regPort);
        System.out.println("对外广播地址: " + advertisedHost);

        new DataNodeServer(port, advertisedHost, storagePath, regHost, regPort).run();
    }
}
