package org.jnfs.driver;

import cn.hutool.core.io.FileUtil;
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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.InputStream;

/**
 * JNFS Driver (SDK)
 * 提供给客户端应用使用的核心 API
 *
 * 升级：使用 Netty ChannelPool 复用 NameNode 连接
 */
public class JNFSDriver {

    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025";

    // Registry 地址列表 (用于集群/高可用)
    private final List<InetSocketAddress> registryAddresses = new CopyOnWriteArrayList<>();
    private final boolean useRegistry;

    private final EventLoopGroup group;

    // NameNode 列表 (Registry 模式下使用)
    private final List<InetSocketAddress> nameNodes = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextNameNodeIndex = new AtomicInteger(0);

    // 连接池映射: Address -> Pool
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap;

    /**
     * 直连模式构造函数
     */
    public JNFSDriver(String nameNodeHost, int nameNodePort) {
        this(nameNodeHost, nameNodePort, null);
    }

    /**
     * 注册中心模式 (静态工厂方法)
     * 支持传入多个注册中心地址，用逗号分隔，例如 "192.168.1.10:8000,192.168.1.11:8000"
     */
    public static JNFSDriver useRegistry(String registryAddresses) {
        return new JNFSDriver(null, 0, registryAddresses);
    }

    /**
     * 兼容旧版 API：单点注册中心
     */
    public static JNFSDriver useRegistry(String registryHost, int registryPort) {
        return useRegistry(registryHost + ":" + registryPort);
    }

    private JNFSDriver(String nameNodeHost, int nameNodePort, String registryAddrStr) {
        this.useRegistry = (registryAddrStr != null);

        if (useRegistry) {
            String[] addrs = registryAddrStr.split(",");
            for (String addr : addrs) {
                String[] parts = addr.trim().split(":");
                if (parts.length == 2) {
                    registryAddresses.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                }
            }
            if (registryAddresses.isEmpty()) {
                throw new IllegalArgumentException("无效的注册中心地址: " + registryAddrStr);
            }
        }

        this.group = new NioEventLoopGroup();

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
        // 遍历所有注册中心地址，直到成功获取列表 (Failover)
        for (InetSocketAddress registryAddr : registryAddresses) {
            Bootstrap b = new Bootstrap();
            RegistryDiscoveryHandler handler = new RegistryDiscoveryHandler();

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
                // 连接当前 Registry
                ChannelFuture f = b.connect(registryAddr).sync();
                Channel channel = f.channel();

                Packet request = new Packet();
                request.setCommandType(CommandType.REGISTRY_GET_NAMENODES);
                request.setToken(CLIENT_TOKEN);
                channel.writeAndFlush(request);

                f.channel().closeFuture().sync();

                List<String> nodes = handler.getNodes();
                if (nodes != null) {
                    // 即使列表为空也可能是正常的(刚启动)，但只要通信成功就算成功
                    if (!nodes.isEmpty()) {
                        nameNodes.clear();
                        for (String node : nodes) {
                            String[] parts = node.split(":");
                            if (parts.length == 2) {
                                nameNodes.add(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                            }
                        }
                    }
                    System.out.println("[Driver] 从 Registry (" + registryAddr + ") 刷新 NameNode 列表: " + nameNodes);
                    // 成功后跳出循环，不再尝试其他 Registry
                    return;
                }
            } catch (Exception e) {
                System.err.println("[Driver] 连接 Registry (" + registryAddr + ") 失败，尝试下一个...");
            }
        }
        System.err.println("[Driver] 无法连接任何 Registry，刷新 NameNode 失败");
    }

    public void close() {
        group.shutdownGracefully();
    }

    /**
     * 上传文件 (byte[] 模式)
     * 适用于小文件或已读取到内存的数据
     * @param data 文件内容
     * @param filename 文件名 (用于元数据记录)
     * @return storageId
     */
    public String uploadFile(byte[] data, String filename) throws Exception {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("文件内容不能为空");
        }

        // 创建临时文件
        File tmpFile = File.createTempFile("jnfs_upload_", "_" + filename);
        try {
            FileUtil.writeBytes(data, tmpFile);
            return uploadFile(tmpFile);
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    /**
     * 上传文件 (InputStream 模式)
     * 适用于 Web 上传 (MultipartFile.getInputStream) 或其他流式输入
     * 注意：此方法会先将流写入临时文件以计算 Hash 和支持零拷贝上传
     * 
     * @param in 输入流
     * @param filename 文件名
     * @return storageId
     */
    public String uploadFile(InputStream in, String filename) throws Exception {
        if (in == null) {
            throw new IllegalArgumentException("输入流不能为空");
        }

        // 创建临时文件
        File tmpFile = File.createTempFile("jnfs_upload_stream_", "_" + filename);
        try {
            FileUtil.writeFromStream(in, tmpFile);
            return uploadFile(tmpFile);
        } finally {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }

    /**
     * 上传文件 (File 模式)
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
     * @param storageId 文件存储ID
     * @param targetPath 下载目标路径 (文件夹或文件全路径)
     */
    public File downloadFile(String storageId, String targetPath) throws Exception {
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

        // 解析目标文件路径
        File targetFile;
        // 使用 Hutool 标准化路径 (处理分隔符、..、重复斜杠等)
        String normalizedPath = FileUtil.normalize(targetPath);
        File destination = new File(normalizedPath);

        // 判断是否为目录：
        // 1. 原始路径以分隔符结尾 (Hutool normalize 会去掉末尾分隔符，所以要用 targetPath 判断)
        // 2. 或者 destination 已存在且是目录
        boolean isDirectory = targetPath.endsWith("/") || targetPath.endsWith("\\") || FileUtil.isDirectory(destination);

        if (isDirectory) {
            // 如果是目录路径，确保目录存在
            FileUtil.mkdir(destination);
            targetFile = new File(destination, filename);
        } else {
            // 否则视为完整的文件路径 (重命名)
            targetFile = destination;
            // 确保父目录存在
            FileUtil.mkParentDirs(targetFile);
        }

        // 先下载到临时密文文件 (与目标文件同目录)
        // File encryptedFile = new File(targetFile.getParentFile(), targetFile.getName() + ".enc");

        // DataNode 存储的是 Hash 命名的文件 (假设 DataNode 已按 Hash 存储)
        // 支持流式解密，直接下载到目标文件
        downloadFromDataNode(dnHost, dnPort, hash, targetFile);
        System.out.println("[Driver] 下载并解密完成: " + targetFile.getAbsolutePath());

        /*
        // --- 解密环节 (已改为流式解密，不再需要后续步骤) ---
        System.out.println("[Driver] 正在解密文件...");
        if (targetFile.exists()) {
            targetFile.delete();
        }
        SecurityUtil.decryptFile(encryptedFile, targetFile);
        System.out.println("[Driver] 解密完成: " + targetFile.getAbsolutePath());

        // 清理密文
        encryptedFile.delete();
        */

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
        private OutputStream out;
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
                // 使用 SecurityUtil 创建流式解密输出流
                this.out = SecurityUtil.createDecryptOutputStream(new FileOutputStream(targetFile));
                System.out.println("[Driver] 开始接收文件流，大小: " + fileSize);

            } else if (msg instanceof ByteBuf) {
                // 处理文件流数据
                ByteBuf buf = (ByteBuf) msg;
                if (out != null) {
                    byte[] bytes = new byte[buf.readableBytes()];
                    buf.readBytes(bytes);
                    out.write(bytes);
                    receivedBytes += bytes.length;

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
                 if (out != null) {
                     out.close();
                 }
             } catch (IOException e) {
                 e.printStackTrace();
             } finally {
                 out = null;
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
