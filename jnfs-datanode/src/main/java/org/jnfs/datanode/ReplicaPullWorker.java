package org.jnfs.datanode;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.NettyClientBootstrap;
import org.jnfs.common.Packet;
import org.jnfs.common.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 后台副本拉取工作线程
 * 从源 DataNode 拉取密文文件，校验完整性后向 NameNode 发送 COMMIT
 *
 * 完整流程（步骤 3-7）：
 * 3. 连接源 DataNode，发 DATA_REPLICA_PULL_REQUEST
 * 4. 接收 DATA_REPLICA_PULL_RESPONSE + 流式密文字节
 * 5. 写入本地存储（tmp + rename 模式）
 * 6. 完整性校验 receivedBytes == fileLength（M4）
 * 7. 校验通过后发 DATA_REPLICA_COMMIT 给 NameNode
 */
class ReplicaPullWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaPullWorker.class);

    /** 连接超时（毫秒） */
    private static final int CONNECT_TIMEOUT_MILLIS = 6000;
    /** 连接等待超时（毫秒） */
    private static final long CONNECT_AWAIT_MILLIS = 6000L;
    /** 全局硬上限超时（秒） */
    private static final long HARD_TIMEOUT_SECONDS = 600L; // 10 分钟
    /** 首响应超时（秒） */
    private static final long FIRST_RESPONSE_TIMEOUT_SECONDS = 30L;
    /** 默认限速（字节/秒），50 MB/s */
    private static final long DEFAULT_RATE_LIMIT_BYTES_PER_SEC = 50L * 1024 * 1024;
    /** 限速检查粒度（字节），1 MB */
    private static final long RATE_LIMIT_CHUNK_SIZE = 1024L * 1024;

    private final String fileHash;
    private final String nodeId;
    private final String sourceHost;
    private final int sourcePort;
    private final String namenodeHost;
    private final int namenodePort;
    private final List<String> storagePaths;
    private final io.netty.channel.EventLoopGroup outboundWorkerGroup;
    private final long rateLimitBytesPerSec;

    /**
     * @param fileHash           要拉取的文件 Hash
     * @param nodeId             本节点 ID（COMMIT payload 需含 nodeId，NameNode 据此登记 file_location）
     * @param sourceHost         源 DataNode 主机
     * @param sourcePort         源 DataNode 端口
     * @param namenodeHost       NameNode 主机（用于发 COMMIT）
     * @param namenodePort       NameNode 端口
     * @param storagePaths       本地存储路径列表
     * @param outboundWorkerGroup 出站连接共享的 EventLoopGroup
     */
    ReplicaPullWorker(String fileHash, String nodeId, String sourceHost, int sourcePort,
                      String namenodeHost, int namenodePort,
                      List<String> storagePaths,
                      io.netty.channel.EventLoopGroup outboundWorkerGroup) {
        this(fileHash, nodeId, sourceHost, sourcePort, namenodeHost, namenodePort,
                storagePaths, outboundWorkerGroup, DEFAULT_RATE_LIMIT_BYTES_PER_SEC);
    }

    /**
     * 可配置限速的构造函数（Phase 5 会传入策略值）
     */
    ReplicaPullWorker(String fileHash, String nodeId, String sourceHost, int sourcePort,
                      String namenodeHost, int namenodePort,
                      List<String> storagePaths,
                      io.netty.channel.EventLoopGroup outboundWorkerGroup,
                      long rateLimitBytesPerSec) {
        this.fileHash = fileHash;
        this.nodeId = nodeId;
        this.sourceHost = sourceHost;
        this.sourcePort = sourcePort;
        this.namenodeHost = namenodeHost;
        this.namenodePort = namenodePort;
        this.storagePaths = storagePaths;
        this.outboundWorkerGroup = outboundWorkerGroup;
        this.rateLimitBytesPerSec = rateLimitBytesPerSec > 0 ? rateLimitBytesPerSec : DEFAULT_RATE_LIMIT_BYTES_PER_SEC;
    }

    @Override
    public void run() {
        LOG.info("[PullWorker] 开始拉取: hash={}, source={}:{}, namenode={}:{}",
                fileHash, sourceHost, sourcePort, namenodeHost, namenodePort);

        boolean filePersisted = false;
        try {
            // 幂等检查：目标文件已存在则跳过
            File targetFile = getStorageFile(fileHash);
            if (targetFile.exists()) {
                LOG.info("[PullWorker] 文件已存在，跳过拉取（幂等）: {}", fileHash);
                filePersisted = true;
                // 仍需发 COMMIT（NameNode 可能不知道本节点持有该文件）
                sendCommitToNameNode();
                return;
            }

            // 执行拉取
            pullFromSource(targetFile);
            filePersisted = true; // 拉取成功，文件已落盘

            // 拉取成功，发 COMMIT
            sendCommitToNameNode();

            LOG.info("[PullWorker] 拉取完成: hash={}", fileHash);
        } catch (Exception e) {
            if (filePersisted) {
                // 文件已落盘但 COMMIT 失败，等下一轮对账补登
                LOG.warn("[PullWorker] 文件已落盘但 COMMIT 失败，等下一轮对账: hash={}, error={}",
                        fileHash, e.getMessage());
            } else {
                // 拉取本身失败（连接/传输/校验），文件未落盘
                LOG.error("[PullWorker] 拉取失败: hash={}, error={}", fileHash, e.getMessage(), e);
            }
        }
    }

    /**
     * 从源 DataNode 拉取文件
     * 步骤 3-6：连接源 → 发 PULL_REQUEST → 接收 RESPONSE + 流式字节 → 写入本地 → 校验
     */
    private void pullFromSource(File targetFile) throws Exception {
        PullDownloadHandler handler = new PullDownloadHandler(targetFile, fileHash, rateLimitBytesPerSec);
        Bootstrap b = NettyClientBootstrap.createWithHandler(outboundWorkerGroup, CONNECT_TIMEOUT_MILLIS, handler);

        Channel channel = null;
        try {
            channel = NettyClientBootstrap.connectSync(b, sourceHost, sourcePort, CONNECT_AWAIT_MILLIS);

            // 发送 PULL_REQUEST
            Packet request = new Packet();
            request.setCommandType(CommandType.DATA_REPLICA_PULL_REQUEST);
            request.setToken(SecurityConfig.getToken());
            request.setData(fileHash.getBytes(StandardCharsets.UTF_8));
            channel.writeAndFlush(request);

            // 等待完成（动态超时 + 硬上限）
            handler.waitForCompletion(HARD_TIMEOUT_SECONDS);
        } finally {
            if (channel != null) {
                try {
                    channel.close().sync();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 向 NameNode 发送 DATA_REPLICA_COMMIT
     * 步骤 7：短连接，payload = fileHash|nodeId
     *
     * @throws Exception 连接/响应/ERROR 任一失败均抛出，由 run() 统一归类日志（此时文件已落盘）
     */
    private void sendCommitToNameNode() throws Exception {
        CommitResponseHandler handler = new CommitResponseHandler();
        Bootstrap b = NettyClientBootstrap.createWithHandler(outboundWorkerGroup, CONNECT_TIMEOUT_MILLIS, handler);

        Channel channel = null;
        try {
            channel = NettyClientBootstrap.connectSync(b, namenodeHost, namenodePort, CONNECT_AWAIT_MILLIS);

            Packet commit = new Packet();
            commit.setCommandType(CommandType.DATA_REPLICA_COMMIT);
            commit.setToken(SecurityConfig.getToken());
            // payload = fileHash|nodeId（NameNode 需 nodeId 登记 file_location 行）
            commit.setData((fileHash + "|" + nodeId).getBytes(StandardCharsets.UTF_8));
            channel.writeAndFlush(commit);

            // 等待 NameNode 响应（10 秒超时）
            Packet response = handler.getResponse(10, TimeUnit.SECONDS);
            if (response.getCommandType() == CommandType.ERROR) {
                throw new IOException("NameNode 拒绝 COMMIT: "
                        + new String(response.getData(), StandardCharsets.UTF_8));
            }
        } finally {
            if (channel != null) {
                try {
                    channel.close().sync();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 根据 Hash 获取存储路径（复用 DataNodeHandler 的路径解析逻辑）
     * 规则: 1-2位为一级目录, 3-4位为二级目录
     * 多路径策略: 已存在返回该路径，不存在选剩余空间最大的
     */
    private File getStorageFile(String hash) throws IOException {
        if (hash == null || hash.isEmpty()) {
            throw new IOException("文件名为空");
        }
        if (!hash.matches("^[a-zA-Z0-9]+$")) {
            throw new IOException("非法的文件名/Hash检测: " + hash);
        }

        String dir1 = hash.length() >= 2 ? hash.substring(0, 2) : "00";
        String dir2 = hash.length() >= 4 ? hash.substring(2, 4) : "00";
        String relativePath = dir1 + File.separator + dir2;

        // 检查文件是否已存在
        for (String path : storagePaths) {
            File rootDir = new File(path).getCanonicalFile();
            File target = new File(rootDir, relativePath + File.separator + hash);
            if (target.getCanonicalPath().startsWith(rootDir.getPath()) && target.exists()) {
                return target;
            }
        }

        // 文件不存在，选择剩余空间最大的路径
        String bestPath = null;
        long maxFreeSpace = -1;
        for (String path : storagePaths) {
            File root = new File(path);
            if (!root.exists()) root.mkdirs();
            long free = root.getFreeSpace();
            if (free > maxFreeSpace) {
                maxFreeSpace = free;
                bestPath = path;
            }
        }
        if (bestPath == null) {
            throw new IOException("没有可用的存储路径");
        }

        File rootDir = new File(bestPath).getCanonicalFile();
        File dir = new File(rootDir, relativePath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File target = new File(dir, hash);
        if (!target.getCanonicalPath().startsWith(rootDir.getPath())) {
            throw new IOException("路径遍历攻击检测: " + target.getName());
        }
        return target;
    }

    // ========== 内部 Handler ==========

    /**
     * 拉取下载处理器
     * 仿 JNFSDriver.DownloadHandler 的 Packet + ByteBuf 处理模式
     * 但写入用 tmp + rename 模式（仿 DataNodeHandler.initiateUpload/handleFileChunk/finishUpload
     * DataNode 间传输密文，不加密/解密
     */
    private class PullDownloadHandler extends SimpleChannelInboundHandler<Object> {

        private final File targetFile;
        private final String hash;
        private final long rateLimitBytesPerSec;

        private FileChannel fileChannel;
        private FileOutputStream fos;
        private File tmpFile;
        private long fileSize = -1;
        private long receivedBytes = 0;

        // 限速状态
        private long lastChunkBoundary = 0;   // 上次限速检查时的 receivedBytes
        private long lastChunkTimestamp = 0;   // 上次限速检查时的时间戳

        private final BlockingQueue<Object> completionSignal = new LinkedBlockingQueue<>();
        private volatile String failureMessage;

        PullDownloadHandler(File targetFile, String hash, long rateLimitBytesPerSec) {
            this.targetFile = targetFile;
            this.hash = hash;
            this.rateLimitBytesPerSec = rateLimitBytesPerSec;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof Packet) {
                handlePacket(ctx, (Packet) msg);
            } else if (msg instanceof ByteBuf) {
                handleStreamChunk((ByteBuf) msg);
            }
        }

        private void handlePacket(ChannelHandlerContext ctx, Packet packet) throws Exception {
            if (packet.getCommandType() == CommandType.ERROR) {
                throw new IOException("源 DataNode 错误: " + new String(packet.getData(), StandardCharsets.UTF_8));
            }

            if (packet.getCommandType() != CommandType.DATA_REPLICA_PULL_RESPONSE) {
                throw new IOException("期望 DATA_REPLICA_PULL_RESPONSE，收到: " + packet.getCommandType());
            }

            // 解析文件大小
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

            LOG.info("[PullWorker] 开始接收密文流: hash={}, size={}", hash, fileSize);

            // 准备临时文件（UUID 唯一名，仿 DataNodeHandler.initiateUpload）
            String uniqueTmpName = hash + "." + UUID.randomUUID().toString() + DataNodeHandler.TMP_SUFFIX;
            tmpFile = new File(targetFile.getParentFile(), uniqueTmpName);

            fos = new FileOutputStream(tmpFile);
            fileChannel = fos.getChannel();

            // 初始化限速状态
            lastChunkBoundary = 0;
            lastChunkTimestamp = System.nanoTime();

            // 空文件
            if (fileSize == 0) {
                finishPull();
                completionSignal.offer(true);
            }
        }

        private void handleStreamChunk(ByteBuf chunk) throws Exception {
            if (fileChannel == null) {
                return;
            }

            int readable = chunk.readableBytes();
            chunk.readBytes(fileChannel, receivedBytes, readable);
            receivedBytes += readable;

            // 限速检查：每 RATE_LIMIT_CHUNK_SIZE (1MB) 检查一次
            if (rateLimitBytesPerSec > 0 && receivedBytes - lastChunkBoundary >= RATE_LIMIT_CHUNK_SIZE) {
                long now = System.nanoTime();
                long elapsedNanos = now - lastChunkTimestamp;
                long expectedNanos = (long) ((receivedBytes - lastChunkBoundary) * 1_000_000_000L / rateLimitBytesPerSec);

                if (elapsedNanos < expectedNanos) {
                    long sleepMs = (expectedNanos - elapsedNanos) / 1_000_000L;
                    if (sleepMs > 0) {
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("拉取被中断");
                        }
                    }
                }

                lastChunkBoundary = receivedBytes;
                lastChunkTimestamp = System.nanoTime();
            }

            // 检查是否接收完毕
            if (receivedBytes >= fileSize) {
                finishPull();
                completionSignal.offer(true);
            }
        }

        /**
         * 完成拉取：关闭文件 → M4 完整性校验 → rename
         */
        private void finishPull() throws IOException {
            closeFile();

            // M4 完整性校验：receivedBytes == fileLength
            if (receivedBytes != fileSize) {
                LOG.error("[PullWorker] 完整性校验失败: hash={}, expected={}, actual={}",
                        hash, fileSize, receivedBytes);
                // 删除 tmp，不 COMMIT
                if (tmpFile != null && tmpFile.exists()) {
                    tmpFile.delete();
                }
                throw new IOException("完整性校验失败: expected=" + fileSize + ", actual=" + receivedBytes);
            }

            // rename tmp -> 正式文件（使用 DataNodeHandler 共享的分段锁，上传与拉取同 hash 互斥）
            synchronized (DataNodeHandler.LOCKS.getLock(hash)) {
                // 幂等：目标文件已存在（并发拉取），跳过
                if (targetFile.exists()) {
                    LOG.info("[PullWorker] 文件已存在，跳过重命名（并发拉取）: {}", hash);
                    if (tmpFile != null) tmpFile.delete();
                    return;
                }

                if (!tmpFile.renameTo(targetFile)) {
                    // 双重检查：并发场景
                    if (targetFile.exists()) {
                        LOG.info("[PullWorker] 重命名失败但文件已存在（并发拉取）: {}", hash);
                        if (tmpFile != null) tmpFile.delete();
                    } else {
                        LOG.error("[PullWorker] 重命名临时文件失败: {}", tmpFile.getAbsolutePath());
                        if (tmpFile != null) tmpFile.delete();
                        throw new IOException("重命名临时文件失败");
                    }
                }
            }

            LOG.info("[PullWorker] 密文存储完成: hash={}, size={}", hash, fileSize);
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
                LOG.error("[PullWorker] 关闭文件流失败", e);
            }
            fileChannel = null;
            fos = null;
        }

        /**
         * 等待拉取完成（动态超时，仿 JNFSDriver.DownloadHandler.waitForCompletion）
         *
         * 三层超时机制：
         * 1. 首响应超时：30s 内未收到 PULL_RESPONSE（fileSize 仍 -1）即抛异常
         * 2. 传输超时：fileSize 已知后动态计算 6 + fileSize/51200 秒
         * 3. 硬上限：maxTimeoutSeconds（10 分钟）兜底
         */
        void waitForCompletion(long maxTimeoutSeconds) throws IOException, InterruptedException {
            long hardDeadlineMs = System.currentTimeMillis() + maxTimeoutSeconds * 1000L;
            long firstResponseDeadlineMs = System.currentTimeMillis() + FIRST_RESPONSE_TIMEOUT_SECONDS * 1000L;
            long transferDeadlineMs = -1;

            while (true) {
                long now = System.currentTimeMillis();

                // 首响应超时
                if (fileSize < 0 && now >= firstResponseDeadlineMs) {
                    throw new IOException("源 DataNode 首响应超时（30s 未收到 PULL_RESPONSE）");
                }

                // fileSize 已知后计算传输截止时间
                if (transferDeadlineMs < 0 && fileSize > 0) {
                    long dynamicSeconds = 6 + fileSize / 51200;
                    transferDeadlineMs = now + dynamicSeconds * 1000L;
                }

                long effectiveDeadline = (transferDeadlineMs > 0)
                        ? Math.min(transferDeadlineMs, hardDeadlineMs)
                        : hardDeadlineMs;
                if (now >= effectiveDeadline) {
                    throw new IOException("拉取超时");
                }

                long pollTimeoutMs = Math.min(500L, effectiveDeadline - now);
                Object result = completionSignal.poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
                if (result == null) {
                    continue;
                }
                if (result instanceof IOException) {
                    throw (IOException) result;
                }
                if (result.equals(false)) {
                    String msg = failureMessage != null ? failureMessage : "拉取超时或失败";
                    throw new IOException(msg);
                }
                return; // result == true，拉取完成
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // 源 DataNode 中途断开连接
            try {
                closeFile();
            } catch (Exception e) {
                LOG.error("[PullWorker] 连接断开时关闭文件失败", e);
                failureMessage = e.getMessage();
                if (completionSignal.isEmpty()) {
                    completionSignal.offer(e);
                }
            }
            if (completionSignal.isEmpty()) {
                if (failureMessage == null) {
                    failureMessage = "源 DataNode 连接中断";
                }
                completionSignal.offer(false);
            }
            // 清理未完成的 tmp 文件
            if (tmpFile != null && tmpFile.exists() && receivedBytes < fileSize) {
                tmpFile.delete();
                LOG.info("[PullWorker] 连接断开，清理未完成临时文件: {}", tmpFile.getAbsolutePath());
            }
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.error("[PullWorker] PullDownloadHandler异常", cause);
            if (failureMessage == null) {
                failureMessage = cause.getMessage();
            }
            try {
                closeFile();
            } catch (Exception e) {
                LOG.error("[PullWorker] 异常处理中关闭文件失败", e);
                failureMessage = e.getMessage();
            }
            // 清理未完成的 tmp 文件
            if (tmpFile != null && tmpFile.exists()) {
                tmpFile.delete();
            }
            ctx.close();
            completionSignal.offer(false);
        }
    }

    /**
     * COMMIT 响应处理器（短连接，仿 JNFSDriver.SyncHandler）
     */
    private static class CommitResponseHandler extends SimpleChannelInboundHandler<Packet> {
        private final BlockingQueue<Packet> queue = new LinkedBlockingQueue<>();
        private volatile boolean channelClosed = false;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
            queue.offer(msg);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            channelClosed = true;
            if (queue.isEmpty()) {
                Packet errorPacket = new Packet();
                errorPacket.setCommandType(CommandType.ERROR);
                errorPacket.setData("NameNode 连接已断开".getBytes(StandardCharsets.UTF_8));
                queue.offer(errorPacket);
            }
        }

        Packet getResponse(long timeout, TimeUnit unit) throws IOException, InterruptedException {
            Packet p = queue.poll(timeout, unit);
            if (p == null) {
                if (channelClosed) {
                    throw new IOException("NameNode 连接已断开");
                }
                throw new IOException("等待 COMMIT 响应超时");
            }
            return p;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            channelClosed = true;
            if (queue.isEmpty()) {
                Packet errorPacket = new Packet();
                errorPacket.setCommandType(CommandType.ERROR);
                errorPacket.setData(("COMMIT 连接异常: " + cause.getMessage()).getBytes(StandardCharsets.UTF_8));
                queue.offer(errorPacket);
            }
            ctx.close();
        }
    }
}
