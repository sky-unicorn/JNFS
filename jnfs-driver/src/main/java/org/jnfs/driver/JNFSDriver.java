package org.jnfs.driver;

import cn.hutool.crypto.digest.DigestUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.jnfs.common.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JNFS Driver (SDK)
 * 提供给客户端应用使用的核心 API
 *
 * 升级：使用 Netty ChannelPool 复用 NameNode 连接
 */
public class JNFSDriver {

    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025";

    private final String registryHost;
    private final int registryPort;
    private final boolean useRegistry;
    
    private final EventLoopGroup group;
    private final String downloadPath;

    // NameNode 列表 (Registry 模式下使用)
    private final List<InetSocketAddress> nameNodes = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextNameNodeIndex = new AtomicInteger(0);

    // 连接池映射: Address -> Pool
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap;

    /**
     * 直连模式构造函数
     */
    public JNFSDriver(String nameNodeHost, int nameNodePort) {
        this(nameNodeHost, nameNodePort, null, 0);
    }

    /**
     * 注册中心模式 (静态工厂方法)
     */
    public static JNFSDriver useRegistry(String registryHost, int registryPort) {
        return new JNFSDriver(null, 0, registryHost, registryPort);
    }

    private JNFSDriver(String nameNodeHost, int nameNodePort, String registryHost, int registryPort) {
        this.registryHost = registryHost;
        this.registryPort = registryPort;
        this.useRegistry = (registryHost != null);
        
        this.group = new NioEventLoopGroup();
        this.downloadPath = "D:\\data\\jnfs\\download";

        // 初始化连接池
        this.poolMap = new AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
            @Override
            protected SimpleChannelPool newPool(InetSocketAddress key) {
                Bootstrap b = new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true);

                // 使用 FixedChannelPool 限制最大连接数，防止资源耗尽
                return new FixedChannelPool(b.remoteAddress(key), new NameNodeChannelPoolHandler(), 10);
            }
        };
        
        if (useRegistry) {
            refreshNameNodes();
            startNameNodeRefreshThread();
        } else {
            // 直连模式，直接添加单一节点
            nameNodes.add(new InetSocketAddress(nameNodeHost, nameNodePort));
        }
    }
    
    private void startNameNodeRefreshThread() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Driver-Refresh");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::refreshNameNodes, 10, 10, TimeUnit.SECONDS);
    }

    private void refreshNameNodes() {
        Bootstrap b = new Bootstrap();
        RegistryDiscoveryHandler handler = new RegistryDiscoveryHandler();
        
        // 使用临时的 EventLoopGroup 避免阻塞主 group，或者直接复用 group
        // 这里为了简单复用 group，注意不要阻塞
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

        try {
            ChannelFuture f = b.connect(registryHost, registryPort).sync();
            Channel channel = f.channel();

            Packet request = new Packet();
            request.setCommandType(CommandType.REGISTRY_GET_NAMENODES);
            request.setToken(CLIENT_TOKEN);
            channel.writeAndFlush(request);

            f.channel().closeFuture().sync();
            
            List<String> nodes = handler.getNodes();
            if (nodes != null && !nodes.isEmpty()) {
                nameNodes.clear();
                for (String node : nodes) {
                    // node format: host:port
                    String[] parts = node.split(":");
                    if (parts.length == 2) {
                        nameNodes.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                    }
                }
                System.out.println("[Driver] 刷新 NameNode 列表: " + nameNodes);
            }
        } catch (Exception e) {
            System.err.println("[Driver] 刷新 NameNode 列表失败: " + e.getMessage());
        }
    }

    public void close() {
        group.shutdownGracefully();
    }

    /**
     * 上传文件
     * 1. 客户端本地加密
     * 2. 上传密文到 DataNode
     */
    public String uploadFile(File file) throws Exception {
        if (!file.exists()) {
            throw new IOException("文件不存在: " + file.getAbsolutePath());
        }

        System.out.println("[Driver] 正在计算文件摘要...");
        String fileHash = DigestUtil.sha256Hex(file);
        System.out.println("[Driver] 文件摘要 (SHA256): " + fileHash);

        String existingAddr = requestUploadPermission(fileHash);

        if (existingAddr != null) {
            System.out.println("[Driver] 发现相同文件 (节点: " + existingAddr + ")，触发秒传...");
            String storageId = commitFile(file.getName(), fileHash, existingAddr);
            System.out.println("[Driver] 秒传成功！存储编号: " + storageId);
            return storageId;
        }

        // --- 加密环节 ---
        System.out.println("[Driver] 正在对文件进行本地加密...");
        File encryptedFile = new File(file.getParent(), file.getName() + ".enc");
        SecurityUtil.encryptFile(file, encryptedFile);
        System.out.println("[Driver] 加密完成，准备上传密文");

        try {
            System.out.println("[Driver] 获得上传许可，开始上传...");

            String dataNodeAddr = getDataNodeForUpload();
            System.out.println("[Driver] 获得上传节点: " + dataNodeAddr);

            String[] parts = dataNodeAddr.split(":");
            String dnHost = parts[0];
            int dnPort = Integer.parseInt(parts[1]);

            // 上传密文，但使用原始文件的 Hash (用于秒传和校验)
            uploadToDataNode(dnHost, dnPort, encryptedFile, fileHash);
            System.out.println("[Driver] 文件数据传输完成");

            String storageId = commitFile(file.getName(), fileHash, dataNodeAddr);
            System.out.println("[Driver] 文件元数据提交完成，存储编号: " + storageId);

            return storageId;
        } finally {
            // 清理临时密文文件
            if (encryptedFile.exists()) {
                encryptedFile.delete();
            }
        }
    }

    /**
     * 下载文件
     * 1. 下载密文
     * 2. 本地解密
     */
    public File downloadFile(String storageId) throws Exception {
        String locInfo = getDownloadLocation(storageId);
        System.out.println("[Driver] 获取下载信息: " + locInfo);

        String[] parts = locInfo.split("\\|");
        if (parts.length != 3) {
            throw new IOException("无效的下载位置信息: " + locInfo);
        }

        String filename = parts[0];
        String hash = parts[1]; // 获取 Hash 用于请求下载
        String address = parts[2];

        String[] addrParts = address.split(":");
        String dnHost = addrParts[0];
        int dnPort = Integer.parseInt(addrParts[1]);

        File downloadDir = new File(downloadPath);
        if (!downloadDir.exists()) {
            downloadDir.mkdirs();
        }

        // 先下载到临时密文文件
        File encryptedFile = new File(downloadDir, filename + ".enc");
        File targetFile = new File(downloadDir, filename);

        // DataNode 存储的是 Hash 命名的文件 (假设 DataNode 已按 Hash 存储)
        // 或者 DataNode 仍按原名存储? 根据之前的实现是按文件名存储
        // 这里需要与 DataNode 约定下载标识：现在改为传 Hash 下载
        downloadFromDataNode(dnHost, dnPort, hash, encryptedFile);
        System.out.println("[Driver] 密文下载完成: " + encryptedFile.getAbsolutePath());

        // --- 解密环节 ---
        System.out.println("[Driver] 正在解密文件...");
        SecurityUtil.decryptFile(encryptedFile, targetFile);
        System.out.println("[Driver] 解密完成: " + targetFile.getAbsolutePath());

        // 清理密文
        encryptedFile.delete();

        return targetFile;
    }

    // ... 辅助方法 ...

    private String requestUploadPermission(String hash) throws Exception {
        while (true) {
            Packet response = sendRequestToNameNode(CommandType.NAMENODE_PRE_UPLOAD, hash.getBytes(StandardCharsets.UTF_8));
            CommandType type = response.getCommandType();

            if (type == CommandType.NAMENODE_RESPONSE_ALLOW) {
                return null;
            } else if (type == CommandType.NAMENODE_RESPONSE_EXIST) {
                return new String(response.getData(), StandardCharsets.UTF_8);
            } else if (type == CommandType.NAMENODE_RESPONSE_WAIT) {
                System.out.println("[Driver] 文件正在上传中，等待重试...");
                Thread.sleep(1000);
            } else if (type == CommandType.ERROR) {
                throw new IOException("错误: " + new String(response.getData(), StandardCharsets.UTF_8));
            } else {
                throw new IOException("预上传申请失败: " + type);
            }
        }
    }

    private String getDataNodeForUpload() throws Exception {
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_REQUEST_UPLOAD_LOC, new byte[0]);
        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("获取上传节点失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private String getDownloadLocation(String storageId) throws Exception {
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_REQUEST_DOWNLOAD_LOC, storageId.getBytes(StandardCharsets.UTF_8));
        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("获取下载节点失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private String commitFile(String filename, String hash, String dataNodeAddr) throws Exception {
        String payload = filename + "|" + hash + "|" + dataNodeAddr;
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_COMMIT_FILE, payload.getBytes(StandardCharsets.UTF_8));

        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("提交元数据失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }

        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private void uploadToDataNode(String host, int port, File file, String hash) throws Exception {
        Bootstrap b = new Bootstrap();
        SyncHandler handler = new SyncHandler();
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

        ChannelFuture f = b.connect(host, port).sync();
        Channel channel = f.channel();

        long fileSize = file.length();
        // 关键修改: 上传时不再传文件名，而是传 Hash，作为 DataNode 的存储文件名
        byte[] hashBytes = hash.getBytes(StandardCharsets.UTF_8);

        Packet packet = new Packet();
        packet.setCommandType(CommandType.UPLOAD_REQUEST);
        packet.setToken(CLIENT_TOKEN);
        packet.setData(hashBytes);
        packet.setStreamLength(fileSize);

        channel.write(packet);
        channel.write(new DefaultFileRegion(file, 0, fileSize));
        channel.flush();

        Packet response = handler.getResponse();
        if (response.getCommandType() == CommandType.ERROR) {
            throw new IOException("DataNode 上传失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }

        channel.close().sync();
    }

    private void downloadFromDataNode(String host, int port, String hash, File targetFile) throws Exception {
        Bootstrap b = new Bootstrap();
        // 使用 PacketDecoder 复用协议解析逻辑
        DownloadHandler handler = new DownloadHandler(targetFile);
        b.group(group)
         .channel(NioSocketChannel.class)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new PacketDecoder()); // 复用标准 Decoder
                 ch.pipeline().addLast(new PacketEncoder());
                 ch.pipeline().addLast(handler);
             }
         });

        ChannelFuture f = b.connect(host, port).sync();
        Channel channel = f.channel();

        Packet request = new Packet();
        request.setCommandType(CommandType.DOWNLOAD_REQUEST);
        request.setToken(CLIENT_TOKEN);
        request.setData(hash.getBytes(StandardCharsets.UTF_8));
        channel.writeAndFlush(request);

        handler.waitForCompletion();
        channel.close().sync();
    }

    // --- 使用连接池发送请求 ---
    private Packet sendRequestToNameNode(CommandType type, byte[] data) throws Exception {
        if (nameNodes.isEmpty()) {
            if (useRegistry) refreshNameNodes();
            if (nameNodes.isEmpty()) throw new IOException("无可用 NameNode");
        }

        Exception lastException = null;
        int attempts = 0;
        int maxAttempts = nameNodes.size(); // 尝试所有节点

        // 简单的负载均衡 + 故障转移
        while (attempts < maxAttempts) {
            int index = nextNameNodeIndex.getAndIncrement();
            InetSocketAddress address = nameNodes.get(Math.abs(index % nameNodes.size()));
            
            try {
                return doSendRequest(address, type, data);
            } catch (Exception e) {
                System.err.println("[Driver] 连接 NameNode (" + address + ") 失败: " + e.getMessage() + "，尝试下一个...");
                lastException = e;
                attempts++;
            }
        }
        throw new IOException("所有 NameNode 均不可用", lastException);
    }

    private Packet doSendRequest(InetSocketAddress address, CommandType type, byte[] data) throws Exception {
        SimpleChannelPool pool = poolMap.get(address);
        
        Future<Channel> future = pool.acquire();
        // 设置连接超时，防止卡死
        if (!future.await(3000)) {
             throw new IOException("获取连接超时");
        }
        if (!future.isSuccess()) {
            throw new IOException("无法连接 NameNode", future.cause());
        }
        
        Channel channel = future.getNow();
        
        SyncHandler handler = new SyncHandler();
        try {
            if (channel.pipeline().get("syncHandler") != null) {
                channel.pipeline().remove("syncHandler");
            }

            channel.pipeline().addLast("syncHandler", handler);
            
            Packet packet = new Packet();
            packet.setCommandType(type);
            packet.setToken(CLIENT_TOKEN);
            packet.setData(data);
            
            channel.writeAndFlush(packet);
            
            return handler.getResponse();
        } finally {
            try {
                if (channel.pipeline().get("syncHandler") != null) {
                    channel.pipeline().remove("syncHandler");
                }
            } catch (Exception e) {
                // ignore
            }
            pool.release(channel);
        }
    }

    // ... Handlers ...

    private static class SyncHandler extends SimpleChannelInboundHandler<Packet> {
        private final BlockingQueue<Packet> queue = new LinkedBlockingQueue<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
            queue.offer(msg);
        }

        public Packet getResponse() throws InterruptedException {
            Packet p = queue.poll(10, TimeUnit.SECONDS);
            if (p == null) {
                throw new RuntimeException("等待响应超时");
            }
            return p;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    private static class RegistryDiscoveryHandler extends SimpleChannelInboundHandler<Packet> {
        private final List<String> nodes = new CopyOnWriteArrayList<>();
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            if (packet.getCommandType() == CommandType.REGISTRY_RESPONSE_NAMENODES) {
                String content = new String(packet.getData(), StandardCharsets.UTF_8);
                if (!content.isEmpty()) {
                    String[] parts = content.split(",");
                    for (String part : parts) {
                        nodes.add(part);
                    }
                }
            }
            ctx.close();
        }
        
        public List<String> getNodes() {
            return nodes;
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static class DownloadHandler extends SimpleChannelInboundHandler<Object> {
        private final File targetFile;
        private FileOutputStream fos;
        private FileChannel fileChannel;
        private long fileSize = -1;
        private long receivedBytes = 0;
        private final BlockingQueue<Boolean> completionSignal = new LinkedBlockingQueue<>();

        public DownloadHandler(File targetFile) {
            this.targetFile = targetFile;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof Packet) {
                // 处理协议包 (DOWNLOAD_RESPONSE)
                Packet packet = (Packet) msg;
                if (packet.getCommandType() == CommandType.ERROR) {
                    throw new IOException("服务端错误: " + new String(packet.getData()));
                }

                // 获取文件大小
                long streamLen = packet.getStreamLength();
                if (streamLen > 0) {
                    fileSize = streamLen;
                } else {
                    try {
                        String sizeStr = new String(packet.getData(), StandardCharsets.UTF_8);
                        fileSize = Long.parseLong(sizeStr);
                    } catch (NumberFormatException e) {
                        fileSize = 0;
                    }
                }

                // 准备接收文件
                if (targetFile.exists()) {
                    targetFile.delete();
                }
                this.fos = new FileOutputStream(targetFile);
                this.fileChannel = this.fos.getChannel();
                System.out.println("[Driver] 开始接收文件流，大小: " + fileSize);

            } else if (msg instanceof ByteBuf) {
                // 处理文件流数据
                ByteBuf buf = (ByteBuf) msg;
                if (fileChannel != null) {
                    int readable = buf.readableBytes();
                    buf.readBytes(fileChannel, receivedBytes, readable);
                    receivedBytes += readable;

                    if (receivedBytes >= fileSize) {
                        System.out.println("[Driver] 下载完成");
                        closeFile();
                        completionSignal.offer(true);
                    }
                }
            }
        }

        private void closeFile() {
             try {
                 if (fileChannel != null) {
                     fileChannel.close();
                 }
                 if (fos != null) {
                     fos.close();
                 }
             } catch (IOException e) {
                 e.printStackTrace();
             } finally {
                 fileChannel = null;
                 fos = null;
             }
        }

        public void waitForCompletion() throws InterruptedException {
            Boolean result = completionSignal.poll(30, TimeUnit.MINUTES);
            if (result == null || !result) {
                throw new RuntimeException("下载超时或失败");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            closeFile();
            ctx.close();
            completionSignal.offer(false);
        }
    }
}
