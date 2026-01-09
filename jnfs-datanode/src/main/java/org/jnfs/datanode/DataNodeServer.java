package org.jnfs.datanode;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.net.NetUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jnfs.common.*;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
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

    private final int port;
    private final String advertisedHost;
    private final List<String> storagePaths;
    // 支持多个注册中心地址
    private final List<InetSocketAddress> registryAddresses;

    // 心跳专用的 EventLoopGroup，避免每次创建销毁
    private final EventLoopGroup heartbeatGroup = new NioEventLoopGroup(1);

    public DataNodeServer(int port, String advertisedHost, List<String> storagePaths, List<InetSocketAddress> registryAddresses) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.storagePaths = storagePaths;
        this.registryAddresses = registryAddresses;
    }

    public void run() throws Exception {
        // 启动后台线程负责注册和心跳
        startHeartbeatThread();
        // 启动垃圾回收线程
        startGarbageCollectorThread();

        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(32);

        try {
            // 使用 NettyServerUtils 启动服务
            // 关键修复: 传入 Supplier 以便为每个连接创建新的 DataNodeHandler 实例
            // DataNodeHandler 是有状态的 (包含文件流)，绝对不能共享！
            NettyServerUtils.start("DataNode", port, 
                () -> new DataNodeHandler(storagePaths), 
                businessGroup);
        } finally {
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
                            System.out.println("[GC] 删除过期临时文件: " + file);
                            Files.delete(file);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    private void sendHeartbeatToRegistry() {
        // 向所有配置的 Registry 发送心跳 (广播模式)
        for (InetSocketAddress addr : registryAddresses) {
            doSendHeartbeatToSingleRegistry(addr);
        }
    }

    private void doSendHeartbeatToSingleRegistry(InetSocketAddress registryAddr) {
        // DataNode 的心跳逻辑目前比较简单，每次都新建连接发送 (短连接)
        // 或者维护一个 Map<Address, Channel> 实现长连接。这里为了简化修改，使用短连接广播。

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .option(ChannelOption.SO_KEEPALIVE, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new PacketEncoder());
                     // 忽略响应
                     ch.pipeline().addLast(new SimpleChannelInboundHandler<Packet>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {}
                     });
                 }
             });

            // 快速超时
            b.connect(registryAddr).addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    Channel ch = future.channel();
                    doSendHeartbeat(ch);
                    ch.close();
                } else {
                    // System.err.println("连接注册中心失败 (" + registryAddr + "): " + future.cause().getMessage());
                }
                group.shutdownGracefully();
            });

        } catch (Exception e) {
            group.shutdownGracefully();
        }
    }

    private void doSendHeartbeat(Channel channel) {
        long totalFreeSpace = 0;
        for (String path : storagePaths) {
            File storeDir = new File(path);
            if (!storeDir.exists()) {
                storeDir.mkdirs();
            }
            totalFreeSpace += storeDir.getFreeSpace();
        }

        String payload = advertisedHost + ":" + port + "|" + totalFreeSpace;

        Packet packet = new Packet();
        packet.setCommandType(CommandType.REGISTRY_HEARTBEAT);
        packet.setToken(Constants.VALID_TOKEN);
        packet.setData(payload.getBytes(StandardCharsets.UTF_8));

        channel.writeAndFlush(packet);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> config = ConfigUtil.loadConfig("datanode.yml");

        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 5369);
        // 如果没有配置 advertised_host，则自动获取本机 IP
        String advertisedHost = (String) serverConfig.getOrDefault("advertised_host", NetUtil.getLocalhostStr());

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

        System.out.println("使用注册中心集群: " + registryAddresses);
        System.out.println("对外广播地址: " + advertisedHost);

        new DataNodeServer(port, advertisedHost, storagePaths, registryAddresses).run();
    }
}
