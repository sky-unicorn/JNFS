package org.jnfs.driver;

import cn.hutool.core.io.FileUtil;
import cn.hutool.crypto.digest.DigestUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.jnfs.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLogger;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * JNFS Driver (SDK)
 * 提供给客户端应用使用的核心 API
 *
 * 升级：使用 Netty ChannelPool 复用 NameNode 连接
 */
public class JNFSDriver {

    // 改为实例变量，以便根据配置决定是否输出日志
    private final Logger LOG;

    // 加密工具实例
    private final SecurityUtil securityUtil;

    // Registry 地址列表 (用于集群/高可用)
    private final List<InetSocketAddress> registryAddresses = new CopyOnWriteArrayList<>();
    private final boolean useRegistry;

    private final EventLoopGroup group;

    // NameNode 列表 (Registry 模式下使用)
    private final List<InetSocketAddress> nameNodes = new CopyOnWriteArrayList<>();
    private final AtomicInteger nextNameNodeIndex = new AtomicInteger(0);
    private ScheduledExecutorService scheduler;

    // 连接池映射: Address -> Pool
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap;

    // 最后一次连接状态
    private volatile ConnectionStatus lastStatus = new ConnectionStatus(
            ConnectionState.SUCCESS, "尚未初始化",
            null, null, null);

    // 是否已启动后台刷新线程
    private final AtomicBoolean refreshThreadStarted = new AtomicBoolean(false);

    /**
     * 直连模式构造函数 (默认开启日志)
     */
    public JNFSDriver(String nameNodeHost, int nameNodePort) {
        this(nameNodeHost, nameNodePort, null, true);
    }

    /**
     * 直连模式构造函数 (可控制日志)
     */
    public JNFSDriver(String nameNodeHost, int nameNodePort, boolean enableLog) {
        this(nameNodeHost, nameNodePort, null, enableLog);
    }

    /**
     * 注册中心模式 (静态工厂方法 - 默认开启日志)
     * 支持传入多个注册中心地址，用逗号分隔，例如 "192.168.1.10:8000,192.168.1.11:8000"
     */
    public static JNFSDriver useRegistry(String registryAddresses) {
        return new JNFSDriver(null, 0, registryAddresses, true);
    }

    /**
     * 注册中心模式 (静态工厂方法 - 可控制日志)
     */
    public static JNFSDriver useRegistry(String registryAddresses, boolean enableLog) {
        return new JNFSDriver(null, 0, registryAddresses, enableLog);
    }

    /**
     * 兼容旧版 API：单点注册中心 (默认开启日志)
     */
    public static JNFSDriver useRegistry(String registryHost, int registryPort) {
        return useRegistry(registryHost + ":" + registryPort, true);
    }

    /**
     * 兼容旧版 API：单点注册中心 (可控制日志)
     */
    public static JNFSDriver useRegistry(String registryHost, int registryPort, boolean enableLog) {
        return useRegistry(registryHost + ":" + registryPort, enableLog);
    }

    private JNFSDriver(String nameNodeHost, int nameNodePort, String registryAddrStr, boolean enableLog) {
        this.securityUtil = new SecurityUtil(SecurityConfig.getAesKey());

        // 初始化 Logger：如果启用日志则使用标准 LoggerFactory，否则使用 NOPLogger (不输出任何日志)
        if (enableLog) {
            this.LOG = LoggerFactory.getLogger(JNFSDriver.class);
        } else {
            this.LOG = NOPLogger.NOP_LOGGER;
        }

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

        // 初始化连接池 (使用通用工具类)
        this.poolMap = ChannelPoolUtils.createDefaultPoolMap(group);

        if (useRegistry) {
            initialize();
        } else {
            // 直连模式，直接添加单一节点
            nameNodes.add(new InetSocketAddress(nameNodeHost, nameNodePort));
            lastStatus = new ConnectionStatus(ConnectionState.SUCCESS, "直连模式",
                    null, null, List.of(new InetSocketAddress(nameNodeHost, nameNodePort)));
        }
    }

    private void startNameNodeRefreshThread() {
        if (!refreshThreadStarted.compareAndSet(false, true)) {
            return; // 已启动，不重复启动
        }
        // 使用统一的 Daemon 线程工厂
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("Driver-Refresh"));
        this.scheduler.scheduleAtFixedRate(() -> {
            ConnectionStatus status = refreshNameNodes();
            lastStatus = status;
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 同步初始化连接
     * 刷新 NameNode 列表，启动后台刷新线程，并返回连接状态
     *
     * @return 连接状态
     */
    public ConnectionStatus initialize() {
        if (!useRegistry) {
            // 直连模式：构造函数中已初始化 nameNodes 和 lastStatus，无需刷新
            return lastStatus;
        }
        ConnectionStatus status = refreshNameNodes();
        lastStatus = status;
        startNameNodeRefreshThread();
        return status;
    }

    /**
     * 异步初始化连接
     * 在独立线程中执行 initialize()，完成后回调
     *
     * @param callback 初始化完成后的回调，参数为连接状态
     */
    public void initialize(Consumer<ConnectionStatus> callback) {
        CompletableFuture.runAsync(() -> {
            ConnectionStatus status = initialize();
            if (callback != null) {
                callback.accept(status);
            }
        });
    }

    /**
     * 获取最后一次的连接状态
     * 如果从未调用过 initialize()，返回默认值 (SUCCESS，消息提示未初始化)
     *
     * @return 最后一次的连接状态
     */
    public ConnectionStatus getConnectionStatus() {
        return lastStatus;
    }

    private ConnectionStatus refreshNameNodes() {
        List<InetSocketAddress> reachableRegistries = new ArrayList<>();
        List<InetSocketAddress> unreachableRegistries = new ArrayList<>();
        List<InetSocketAddress> discoveredNameNodes = new ArrayList<>();

        // 遍历所有注册中心地址，直到成功获取列表 (Failover)
        for (InetSocketAddress registryAddr : registryAddresses) {
            RegistryDiscoveryHandler handler = new RegistryDiscoveryHandler();

            // 使用通用工具类创建 Bootstrap
            Bootstrap b = NettyClientBootstrap.createWithHandler(group, 5000, handler);

            try {
                // 连接当前 Registry，设置连接超时
                ChannelFuture f = b.connect(registryAddr);
                boolean connected = f.awaitUninterruptibly(5000, TimeUnit.MILLISECONDS);
                if (!connected || !f.isSuccess()) {
                    String reason = f.cause() != null ? f.cause().getMessage() : "连接超时";
                    LOG.warn("[Driver] 连接 Registry ({}) 失败: {}，尝试下一个...", registryAddr, reason);
                    unreachableRegistries.add(registryAddr);
                    continue;
                }
                Channel channel = f.channel();

                Packet request = new Packet();
                request.setCommandType(CommandType.REGISTRY_GET_NAMENODES);
                request.setToken(SecurityConfig.getToken());
                channel.writeAndFlush(request);

                // 等待响应，设置超时防止永久阻塞
                channel.closeFuture().await(10, TimeUnit.SECONDS);

                String error = handler.getError();
                if (error != null) {
                    LOG.warn("[Driver] Registry ({}) 返回错误: {}，尝试下一个...", registryAddr, error);
                    // 检查是否是 Token 错误
                    if (error.contains("Token") || error.contains("token") || error.contains("认证")) {
                        LOG.error("[Driver] 认证 Token 无效");
                        return new ConnectionStatus(ConnectionState.TOKEN_INVALID,
                                "认证 Token 无效: " + error,
                                reachableRegistries, unreachableRegistries, discoveredNameNodes);
                    }
                    unreachableRegistries.add(registryAddr);
                    continue;
                }

                reachableRegistries.add(registryAddr);

                List<String> nodes = handler.getNodes();
                if (nodes != null) {
                    if (!nodes.isEmpty()) {
                        // 先构建完整的新列表，再用 clear()+addAll() 替换
                        // 相比原来逐个 add() 极大缩短了并发读取窗口
                        // sendRequestToNameNode 已有空列表 fallback 逻辑可覆盖间隙
                        List<InetSocketAddress> newNodes = new ArrayList<>();
                        for (String node : nodes) {
                            // 兼容新旧格式:
                            // 新格式: nodeId|host:port (由 Registry 返回)
                            // 旧格式: host:port
                            String address = node;
                            if (node.contains("|")) {
                                String[] nodeParts = node.split("\\|");
                                address = nodeParts[nodeParts.length - 1];
                            }
                            String[] parts = address.split(":");
                            if (parts.length == 2) {
                                InetSocketAddress addr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
                                newNodes.add(addr);
                                discoveredNameNodes.add(addr);
                            }
                        }
                        nameNodes.clear();
                        nameNodes.addAll(newNodes);
                        LOG.info("[Driver] 从 Registry ({}) 刷新 NameNode 列表: {}", registryAddr, nameNodes);
                        // 判断状态：是否所有 Registry 都可达
                        if (unreachableRegistries.isEmpty()) {
                            return new ConnectionStatus(ConnectionState.SUCCESS,
                                    "所有 Registry 可达",
                                    reachableRegistries, unreachableRegistries, discoveredNameNodes);
                        } else {
                            return new ConnectionStatus(ConnectionState.PARTIAL_SUCCESS,
                                    "部分 Registry 不可达",
                                    reachableRegistries, unreachableRegistries, discoveredNameNodes);
                        }
                    } else {
                        // Registry 可达但未返回任何 NameNode
                        LOG.warn("[Driver] Registry ({}) 可达但未发现 NameNode", registryAddr);
                        return new ConnectionStatus(ConnectionState.NO_NAMENODE,
                                "Registry 可达但未发现 NameNode: " + registryAddr,
                                reachableRegistries, unreachableRegistries, discoveredNameNodes);
                    }
                }
            } catch (Exception e) {
                LOG.warn("[Driver] 连接 Registry ({}) 失败: {}，尝试下一个...", registryAddr, e.getMessage());
                unreachableRegistries.add(registryAddr);
            }
        }

        // 所有 Registry 都不可达
        LOG.error("[Driver] 无法连接任何 Registry，刷新 NameNode 失败");
        return new ConnectionStatus(ConnectionState.REGISTRY_UNREACHABLE,
                "所有 Registry 不可达",
                reachableRegistries, unreachableRegistries, discoveredNameNodes);
    }

    public void close() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
        if (poolMap instanceof Closeable) {
            try {
                ((Closeable) poolMap).close();
            } catch (Exception e) {
                // ignore
            }
        }
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

        LOG.info("[Driver] 正在计算文件摘要...");
        String fileHash = DigestUtil.sha256Hex(file);
        LOG.info("[Driver] 文件摘要 (SHA256): {}", fileHash);

        String existingAddr = requestUploadPermission(fileHash);

        if (existingAddr != null) {
            LOG.info("[Driver] 发现相同文件 (节点: {})，触发秒传...", existingAddr);
            String storageId = commitFile(file.getName(), fileHash, existingAddr);
            LOG.info("[Driver] 秒传成功！存储编号: {}", storageId);
            return storageId;
        }

        // --- 加密环节 ---
        LOG.info("[Driver] 正在对文件进行本地加密...");
        File encryptedFile = new File(file.getParent(), file.getName() + ".enc");
        securityUtil.encryptFile(file, encryptedFile);
        LOG.info("[Driver] 加密完成，准备上传密文");

        try {
            LOG.info("[Driver] 获得上传许可，开始上传...");

            String dataNodeAddr = getDataNodeForUpload();
            LOG.info("[Driver] 获得上传节点: {}", dataNodeAddr);

            String[] parts = dataNodeAddr.split(":");
            String dnHost = parts[0];
            int dnPort = Integer.parseInt(parts[1]);

            // 上传密文，使用但原始文件的 Hash (用于秒传和校验)
            uploadToDataNode(dnHost, dnPort, encryptedFile, fileHash);
            LOG.info("[Driver] 文件数据传输完成");

            String storageId = commitFile(file.getName(), fileHash, dataNodeAddr);
            LOG.info("[Driver] 文件元数据提交完成，存储编号: {}", storageId);

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
        LOG.info("[Driver] 获取下载信息: {}", locInfo);

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
        //支持 流式解密，直接下载到目标文件
        downloadFromDataNode(dnHost, dnPort, hash, targetFile);
        LOG.info("[Driver] 下载并解密完成: {}", targetFile.getAbsolutePath());

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
                LOG.info("[Driver] 文件正在上传中，等待重试...");
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
        SyncHandler handler = new SyncHandler(LOG);
        // 使用通用工具类创建 Bootstrap
        Bootstrap b = NettyClientBootstrap.createWithHandler(group, 5000, handler);

        Channel channel = NettyClientBootstrap.connectSync(b, host, port, 6000);

        try {
            long fileSize = file.length();
            // 关键修改: 上传时不再传文件名，而是传 Hash，作为 DataNode 的存储文件名
            byte[] hashBytes = hash.getBytes(StandardCharsets.UTF_8);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(SecurityConfig.getToken());
            packet.setData(hashBytes);
            packet.setStreamLength(fileSize);

            channel.write(packet);
            channel.write(new DefaultFileRegion(file, 0, fileSize));
            channel.flush();

            // 动态计算超时时间：基础 60秒 + (文件大小 / 50KB/s)
            // 假设最差网络环境 50KB/s，确保大文件有足够时间传输
            long timeoutSeconds = 60 + (fileSize / 51200);

            Packet response = handler.getResponse(timeoutSeconds, TimeUnit.SECONDS);
            if (response.getCommandType() == CommandType.ERROR) {
                throw new IOException("DataNode 上传失败: " + new String(response.getData(), StandardCharsets.UTF_8));
            }
        } finally {
            channel.close().sync();
        }
    }

    private void downloadFromDataNode(String host, int port, String hash, File targetFile) throws Exception {
        // 使用 PacketDecoder 复用协议解析逻辑
        DownloadHandler handler = new DownloadHandler(targetFile, securityUtil, LOG);
        // 使用通用工具类创建 Bootstrap
        Bootstrap b = NettyClientBootstrap.createWithHandler(group, 5000, handler);

        Channel channel = NettyClientBootstrap.connectSync(b, host, port, 6000);

        Packet request = new Packet();
        request.setCommandType(CommandType.DOWNLOAD_REQUEST);
        request.setToken(SecurityConfig.getToken());
        request.setData(hash.getBytes(StandardCharsets.UTF_8));
        channel.writeAndFlush(request);

        handler.waitForCompletion();
        channel.close().sync();
    }

    // --- 使用连接池发送请求 ---
    private Packet sendRequestToNameNode(CommandType type, byte[] data) throws Exception {
        if (nameNodes.isEmpty()) {
            if (useRegistry) {
                ConnectionStatus status = refreshNameNodes();
                lastStatus = status;
            }
            if (nameNodes.isEmpty()) throw new IOException("无可用 NameNode");
        }

        Exception lastException = null;
        int attempts = 0;
        int maxAttempts = nameNodes.size(); // 尝试所有节点

        // 简单的负载均衡 + 故障转移
        while (attempts < maxAttempts) {
            int index = nextNameNodeIndex.getAndIncrement();
            // 注意：Math.abs(Integer.MIN_VALUE) 仍为负数，需用位运算确保非负
            int safeIndex = (index & 0x7FFFFFFF) % nameNodes.size();
            InetSocketAddress address = nameNodes.get(safeIndex);

            try {
                return doSendRequest(address, type, data);
            } catch (Exception e) {
                LOG.warn("[Driver] 连接 NameNode ({}) 失败: {}，尝试下一个...", address, e.getMessage());
                lastException = e;
                attempts++;
            }
        }
        throw new IOException("所有 NameNode 均不可用", lastException);
    }

    private Packet doSendRequest(InetSocketAddress address, CommandType type, byte[] data) throws Exception {
        SimpleChannelPool pool = poolMap.get(address);

        Future<Channel> future = pool.acquire();
        // 等待连接获取完成，超时时间略大于 CONNECT_TIMEOUT_MILLIS (5s) + 余量
        boolean acquired = future.await(6000);
        // 先检查是否已明确失败 (如连接被拒绝)，提供更精确的错误信息
        if (!acquired || !future.isSuccess()) {
            Throwable cause = future.cause();
            String msg;
            if (cause instanceof java.net.ConnectException) {
                msg = "连接被拒绝 (" + address + ")，NameNode 服务未启动";
            } else if (cause instanceof java.net.NoRouteToHostException) {
                msg = "无法到达 NameNode (" + address + ")，请检查网络或防火墙配置";
            } else if (!acquired) {
                msg = "连接 NameNode 超时 (" + address + ")，服务可能未启动";
            } else {
                msg = "无法连接 NameNode (" + address + ")";
            }
            throw new IOException(msg, cause);
        }

        Channel channel = future.getNow();

        SyncHandler handler = new SyncHandler(LOG);
        try {
            if (channel.pipeline().get("syncHandler") != null) {
                channel.pipeline().remove("syncHandler");
            }

            channel.pipeline().addLast("syncHandler", handler);

            Packet packet = new Packet();
            packet.setCommandType(type);
            packet.setToken(SecurityConfig.getToken());
            packet.setData(data);

            channel.writeAndFlush(packet);

            // NameNode 响应通常较快，设置 10 秒超时
            return handler.getResponse(10, TimeUnit.SECONDS);
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
        private final Logger logger;
        private volatile boolean channelClosed = false;

        public SyncHandler(Logger logger) {
            this.logger = logger;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
            queue.offer(msg);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            channelClosed = true;
            // 如果队列中还没有响应，放入一个特殊的错误包以唤醒等待线程
            if (queue.isEmpty()) {
                Packet errorPacket = new Packet();
                errorPacket.setCommandType(CommandType.ERROR);
                errorPacket.setData("连接已断开".getBytes(StandardCharsets.UTF_8));
                queue.offer(errorPacket);
            }
        }

        public Packet getResponse(long timeout, TimeUnit unit) throws IOException, InterruptedException {
            Packet p = queue.poll(timeout, unit);
            if (p == null) {
                if (channelClosed) {
                    throw new IOException("连接已断开，服务可能未启动");
                }
                throw new IOException("等待响应超时 (" + timeout + " " + unit.toString().toLowerCase() + ")");
            }
            return p;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("ConnectionHandler异常", cause);
            channelClosed = true;
            if (queue.isEmpty()) {
                Packet errorPacket = new Packet();
                errorPacket.setCommandType(CommandType.ERROR);
                errorPacket.setData(("连接异常: " + cause.getMessage()).getBytes(StandardCharsets.UTF_8));
                queue.offer(errorPacket);
            }
            ctx.close();
        }
    }

    private static class RegistryDiscoveryHandler extends SimpleChannelInboundHandler<Packet> {
        private final List<String> nodes = new CopyOnWriteArrayList<>();
        private volatile String error;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            if (packet.getCommandType() == CommandType.ERROR) {
                this.error = new String(packet.getData(), StandardCharsets.UTF_8);
            } else if (packet.getCommandType() == CommandType.REGISTRY_RESPONSE_NAMENODES) {
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

        public String getError() {
            return error;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }

    private static class DownloadHandler extends SimpleChannelInboundHandler<Object> {
        private final File targetFile;
        private final SecurityUtil securityUtil;
        private OutputStream out;
        private long fileSize = -1;
        private long receivedBytes = 0;
        private final BlockingQueue<Boolean> completionSignal = new LinkedBlockingQueue<>();
        private final Logger logger;

        public DownloadHandler(File targetFile, SecurityUtil securityUtil, Logger logger) {
            this.targetFile = targetFile;
            this.securityUtil = securityUtil;
            this.logger = logger;
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
                this.out = securityUtil.createDecryptOutputStream(new FileOutputStream(targetFile));
                logger.info("[Driver] 开始接收文件流，大小: {}", fileSize);

            } else if (msg instanceof ByteBuf) {
                // 处理文件流数据
                ByteBuf buf = (ByteBuf) msg;
                if (out != null) {
                    byte[] bytes = new byte[buf.readableBytes()];
                    buf.readBytes(bytes);
                    out.write(bytes);
                    receivedBytes += bytes.length;

                    if (receivedBytes >= fileSize) {
                        logger.info("[Driver] 下载完成");
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
                 logger.error("关闭文件输出流失败", e);
             } finally {
                 out = null;
             }
        }

        public void waitForCompletion() throws IOException, InterruptedException {
            Boolean result = completionSignal.poll(30, TimeUnit.MINUTES);
            if (result == null || !result) {
                throw new IOException("下载超时或失败");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("DownloadHandler异常", cause);
            closeFile();
            ctx.close();
            completionSignal.offer(false);
        }
    }
}