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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import org.jnfs.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
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
    private final List<String> storagePaths;
    // 支持多个注册中心地址
    private final List<InetSocketAddress> registryAddresses;

    // 复用 EventLoopGroup
    private final EventLoopGroup workerGroup;
    // 连接池映射
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> registryPoolMap;

    public DataNodeServer(int port, String advertisedHost, List<String> storagePaths, List<InetSocketAddress> registryAddresses) {
        this.port = port;
        this.advertisedHost = advertisedHost;
        this.storagePaths = storagePaths;
        this.registryAddresses = registryAddresses;
        
        // 初始化共享的 Worker Group
        this.workerGroup = new NioEventLoopGroup();
        
        // 初始化连接池
        this.registryPoolMap = new AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
            @Override
            protected SimpleChannelPool newPool(InetSocketAddress key) {
                Bootstrap b = new Bootstrap()
                        .group(workerGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true);
                        
                // 使用 FixedChannelPool 限制最大连接数 (每 Registry 10 连接)
                return new FixedChannelPool(b.remoteAddress(key), new RegistryChannelPoolHandler(), 10);
            }
        };
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
            
            // 关闭连接池资源
            if (registryPoolMap instanceof Closeable) {
                try {
                    ((Closeable) registryPoolMap).close();
                } catch (Exception e) {
                    // ignore
                }
            }
            workerGroup.shutdownGracefully();
        }
    }

    private void startHeartbeatThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendHeartbeatToRegistry();
            } catch (Exception e) {
                LOG.error("发送心跳失败: {}", e.getMessage(), e);
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

    private void sendHeartbeatToRegistry() {
        // 向所有配置的 Registry 发送心跳 (广播模式)
        for (InetSocketAddress addr : registryAddresses) {
            SimpleChannelPool pool = registryPoolMap.get(addr);
            Future<Channel> future = pool.acquire();
            
            future.addListener((FutureListener<Channel>) f -> {
                if (f.isSuccess()) {
                    Channel ch = f.getNow();
                    try {
                        doSendHeartbeat(ch).addListener(writeFuture -> {
                            // 写入完成后释放连接
                            pool.release(ch);
                        });
                    } catch (Exception e) {
                        pool.release(ch);
                        LOG.error("发送心跳异常 ({}) : {}", addr, e.getMessage());
                    }
                } else {
                    LOG.warn("连接注册中心失败 ({}) : {}", addr, f.cause().getMessage());
                }
            });
        }
    }

    private ChannelFuture doSendHeartbeat(Channel channel) {
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

        return channel.writeAndFlush(packet);
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

        LOG.info("使用注册中心集群: {}", registryAddresses);
        LOG.info("对外广播地址: {}", advertisedHost);

        new DataNodeServer(port, advertisedHost, storagePaths, registryAddresses).run();
    }
    
    // --- 连接池 Handler ---
    private static class RegistryChannelPoolHandler implements ChannelPoolHandler {
        @Override
        public void channelReleased(Channel ch) throws Exception {}

        @Override
        public void channelAcquired(Channel ch) throws Exception {}

        @Override
        public void channelCreated(Channel ch) throws Exception {
            // 初始化 Pipeline
            ch.pipeline().addLast(new PacketDecoder());
            ch.pipeline().addLast(new PacketEncoder());
        }
    }
}
