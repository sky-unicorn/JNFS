package org.jnfs.datanode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.NettyHandlerHelper;
import org.jnfs.common.Packet;
import org.jnfs.common.SecurityConfig;
import org.jnfs.common.SecurityUtil;
import org.jnfs.common.SegmentedLocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * DataNode 业务处理器
 * 处理文件上传和下载的数据流
 *
 */
public class DataNodeHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(DataNodeHandler.class);

    // 临时文件后缀
    public static final String TMP_SUFFIX = ".tmp";

    private final List<String> storagePaths;

    // 副本拉取共享资源（由 DataNodeServer 注入，所有连接实例共享同一批）
    // null 表示不支持跨节点副本拉取（向后兼容）
    private final ExecutorService pullExecutor;
    private final EventLoopGroup outboundWorkerGroup;
    private final long pullRateLimitBytesPerSec;
    private final String nodeId;

    // 当前正在接收的文件写入通道
    private FileChannel currentFileChannel;
    // 当前文件输出流
    private FileOutputStream currentFos;
    // 当前文件名 (Hash)
    private String currentFileName;
    // 当前临时文件对象
    private File currentTmpFile;
    // 当前文件总大小
    private long currentFileSize;
    // 已接收字节数
    private long receivedBytes;

    /**
     * 兼容旧构造函数（不支持副本拉取）
     */
    public DataNodeHandler(List<String> storagePaths) {
        this(storagePaths, null, null, 0L, null);
    }

    /**
     * 完整构造函数（注入副本拉取共享资源）
     *
     * @param storagePaths          本地存储路径列表
     * @param pullExecutor          后台拉取线程池（所有连接共享）
     * @param outboundWorkerGroup   出站连接共享 EventLoopGroup（拉取/COMMIT 用）
     * @param pullRateLimitBytesPerSec 拉取限速（字节/秒，0 表示用默认值）
     * @param nodeId                本节点 ID（COMMIT payload 需含 nodeId）
     */
    public DataNodeHandler(List<String> storagePaths,
                           ExecutorService pullExecutor,
                           EventLoopGroup outboundWorkerGroup,
                           long pullRateLimitBytesPerSec,
                           String nodeId) {
        this.storagePaths = storagePaths;
        this.pullExecutor = pullExecutor;
        this.outboundWorkerGroup = outboundWorkerGroup;
        this.pullRateLimitBytesPerSec = pullRateLimitBytesPerSec;
        this.nodeId = nodeId;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Packet) {
            handlePacket(ctx, (Packet) msg);
        } else if (msg instanceof ByteBuf) {
            handleFileChunk(ctx, (ByteBuf) msg);
        }
    }

    private void handlePacket(ChannelHandlerContext ctx, Packet packet) {
        // 验证 Token (仅针对控制指令)
        if (!NettyHandlerHelper.validateToken(packet.getToken())) {
            LOG.warn("安全拦截: 无效的 Token");
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "Authentication Failed".getBytes(StandardCharsets.UTF_8));
            ctx.close();
            return;
        }

        if (packet.getCommandType() == CommandType.UPLOAD_REQUEST) {
            initiateUpload(ctx, packet);
        } else if (packet.getCommandType() == CommandType.DOWNLOAD_REQUEST) {
            handleDownload(ctx, packet);
        } else if (packet.getCommandType() == CommandType.DATA_REPLICA_PULL_REQUEST) {
            // 源侧：被其他 DataNode 拉取（步骤 4）
            handleReplicaPullRequest(ctx, packet);
        } else if (packet.getCommandType() == CommandType.DATA_REPLICA_PULL_CMD) {
            // 目标侧：NameNode 协调本节点开始拉取（步骤 1-2）
            handleReplicaPullCmd(ctx, packet);
        } else if (packet.getCommandType() == CommandType.DATA_HEAD_READ_REQUEST) {
            // 文件头读取：NameNode 后台类型嗅探用（读解密后的头部 ≤8KB，不碰下载主链路）
            handleHeadRead(ctx, packet);
        }
    }

    private void initiateUpload(ChannelHandlerContext ctx, Packet packet) {
        byte[] data = packet.getData();
        if (data == null || data.length == 0) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "无效的元数据".getBytes(StandardCharsets.UTF_8));
            return;
        }

        long fileSize = packet.getStreamLength();

        String fileName = new String(data, StandardCharsets.UTF_8);

        LOG.info("准备接收文件: {}, 大小: {} 字节", fileName, fileSize);

        try {
            if (currentFos != null) {
                currentFos.close();
            }
            // --- 目录分级逻辑 ---
            File targetFile = getStorageFile(fileName);

            // 修复：使用 UUID 生成唯一临时文件名，防止并发上传同一文件时的数据冲突
            String uniqueTmpName = fileName + "." + UUID.randomUUID().toString() + TMP_SUFFIX;
            File tmpFile = new File(targetFile.getParentFile(), uniqueTmpName);

            currentFos = new FileOutputStream(tmpFile);
            currentFileChannel = currentFos.getChannel();
            currentFileName = fileName;
            currentTmpFile = tmpFile;
            currentFileSize = fileSize;
            receivedBytes = 0;

            if (fileSize == 0) {
                finishUpload(ctx);
            }
        } catch (IOException e) {
            LOG.error("文件上传初始化失败", e);
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, ("服务端错误: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleFileChunk(ChannelHandlerContext ctx, ByteBuf chunk) {
        if (currentFileChannel == null) {
            return;
        }

        try {
            int readable = chunk.readableBytes();
            chunk.readBytes(currentFileChannel, receivedBytes, readable);
            receivedBytes += readable;

            if (receivedBytes >= currentFileSize) {
                finishUpload(ctx);
            }
        } catch (IOException e) {
            LOG.error("文件块写入失败", e);
            closeCurrentFile();
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, ("写入错误: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    // 使用通用工具类提供分段锁
    // 包级私有：ReplicaPullWorker 复用同一实例，使上传(finishUpload)与拉取(finishPull)同 hash 互斥
    static final SegmentedLocks LOCKS = new SegmentedLocks(128);

    private void finishUpload(ChannelHandlerContext ctx) {
        closeCurrentFile();

        // 重命名 .tmp -> 正式文件
        File finalFile;
        try {
            finalFile = getStorageFile(currentFileName);
        } catch (IOException e) {
            LOG.error("获取存储文件路径失败", e);
            // 尝试手动删除失败的 tmp
            if (currentTmpFile != null) currentTmpFile.delete();
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, ("文件存储失败(路径校验错误): " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
            return;
        }

        // 使用分段锁，仅锁定当前文件 Hash 对应的分段，避免全局竞争
        synchronized (LOCKS.getLock(currentFileName)) {
            // 如果目标文件已存在，直接删除临时文件并返回成功 (视为幂等上传)
            if (finalFile.exists()) {
                LOG.info("文件已存在，跳过重命名: {}", currentFileName);
                currentTmpFile.delete();
                NettyHandlerHelper.sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功(秒传): " + currentFileName).getBytes(StandardCharsets.UTF_8));
                // 重置状态
                resetState();
                return;
            }

            if (currentTmpFile.renameTo(finalFile)) {
                LOG.info("文件存储完成: {}", currentFileName);
                NettyHandlerHelper.sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功: " + currentFileName).getBytes(StandardCharsets.UTF_8));
            } else {
                // 双重检查: 可能在重命名的一瞬间被其他线程抢先了 (虽然有 FILE_LOCK，但防御性编程)
                if (finalFile.exists()) {
                     LOG.info("重命名失败但文件已存在 (并发上传): {}", currentFileName);
                     currentTmpFile.delete();
                     NettyHandlerHelper.sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功: " + currentFileName).getBytes(StandardCharsets.UTF_8));
                } else {
                    LOG.error("重命名临时文件失败: {}", currentTmpFile.getAbsolutePath());
                    // 尝试手动删除失败的 tmp
                    currentTmpFile.delete();
                    NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "文件存储失败(重命名错误)".getBytes(StandardCharsets.UTF_8));
                }
            }
        }

        resetState();
    }

    private void resetState() {
        currentFileName = null;
        currentTmpFile = null;
        currentFileSize = 0;
        receivedBytes = 0;
    }

    private void handleDownload(ChannelHandlerContext ctx, Packet packet) {
        String filename = new String(packet.getData(), StandardCharsets.UTF_8);

        // --- 目录分级逻辑 ---
        File file;
        try {
            file = getStorageFile(filename);
        } catch (IOException e) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, ("非法的文件名: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
            return;
        }

        if (!file.exists()) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "文件不存在".getBytes(StandardCharsets.UTF_8));
            return;
        }

        LOG.info("开始发送文件: {}", filename);

        long fileLength = file.length();
        Packet response = new Packet();
        response.setCommandType(CommandType.DOWNLOAD_RESPONSE);
        response.setData(String.valueOf(fileLength).getBytes(StandardCharsets.UTF_8));
        response.setStreamLength(fileLength); // 设置流长度，让 Client 正确跳过 Header
        ctx.write(response);

        DefaultFileRegion region = new DefaultFileRegion(file, 0, fileLength);
        ctx.writeAndFlush(region);
    }

    // ==================== 文件头读取（后台类型嗅探） ====================

    /** 头部嗅探最多返回的明文字节数（8KB，Tika 检测足够） */
    private static final int HEAD_READ_MAX_PLAIN_BYTES = 8192;

    /**
     * 处理 DATA_HEAD_READ_REQUEST：读文件解密后的头部（≤8KB）供 NameNode 后台类型嗅探。
     * <p>
     * 存储文件为 v1 密文格式 {@code [version(1)][HMAC(32)][IV(16)][ciphertext]}：
     * 读前 {@code SecurityUtil.HEADER_LENGTH + 8192} 字节，跳过 49 字节头后对密文前缀做
     * AES-CTR 前缀解密（CTR 流密码无需完整密文）。HMAC 覆盖全量数据无法对前缀校验，
     * 因此本接口仅用于类型嗅探等尽力而为场景（解密失败返回空头，不报错）。
     * <p>
     * 响应 payload：{@code [8B 大端逻辑长度][明文头部 ≤8KB]}。
     * 逻辑长度 = 文件长度 - HEADER_LENGTH（钳制 ≥0），供 NameNode 回填 file_size。
     * 本指令与 DOWNLOAD 主链路完全独立（仅读 ≤8KB，走控制包小响应），不经过 DefaultFileRegion 流式通道。
     */
    private void handleHeadRead(ChannelHandlerContext ctx, Packet packet) {
        String fileHash = new String(packet.getData(), StandardCharsets.UTF_8);

        File file;
        try {
            file = getStorageFile(fileHash);
        } catch (IOException e) {
            NettyHandlerHelper.sendError(ctx, "非法的文件名: " + e.getMessage());
            return;
        }

        if (!file.exists()) {
            NettyHandlerHelper.sendError(ctx, "文件不存在");
            return;
        }

        long fileLength = file.length();
        long logicalLength = Math.max(0L, fileLength - SecurityUtil.HEADER_LENGTH);
        int readLen = (int) Math.min(fileLength, SecurityUtil.HEADER_LENGTH + HEAD_READ_MAX_PLAIN_BYTES);

        byte[] plainHead = new byte[0];
        if (readLen > SecurityUtil.HEADER_LENGTH) {
            byte[] enc = new byte[readLen];
            try (java.io.FileInputStream fis = new java.io.FileInputStream(file)) {
                int off = 0;
                while (off < readLen) {
                    int n = fis.read(enc, off, readLen - off);
                    if (n < 0) {
                        break;
                    }
                    off += n;
                }
            } catch (IOException e) {
                LOG.warn("[HeadRead] 读取文件头失败: {}", fileHash, e);
                NettyHandlerHelper.sendError(ctx, "读取文件头失败");
                return;
            }
            try {
                plainHead = new SecurityUtil(SecurityConfig.getAesKey()).decryptHead(enc);
            } catch (Exception e) {
                // 尽力而为：解密失败（密钥不匹配/格式异常）返回空头，不阻断嗅探链路
                LOG.debug("[HeadRead] 头部解密失败（按空头处理）: {}", fileHash);
            }
        }

        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8 + plainHead.length);
        buf.putLong(logicalLength);
        buf.put(plainHead);
        NettyHandlerHelper.sendResponse(ctx, CommandType.DATA_HEAD_READ_RESPONSE, buf.array());
        LOG.debug("[HeadRead] 响应头部: hash={}, logicalLen={}, plainBytes={}",
                fileHash, logicalLength, plainHead.length);
    }

    // ==================== 副本拉取相关（源侧） ====================

    /**
     * 源侧：处理其他 DataNode 发来的 DATA_REPLICA_PULL_REQUEST
     * 步骤 4：按 hash 找文件，回 DATA_REPLICA_PULL_RESPONSE（含 fileLength）+ 流式发送密文
     * 仿 handleDownload，但响应命令为 DATA_REPLICA_PULL_RESPONSE
     */
    private void handleReplicaPullRequest(ChannelHandlerContext ctx, Packet packet) {
        String fileHash = new String(packet.getData(), StandardCharsets.UTF_8);

        File file;
        try {
            file = getStorageFile(fileHash);
        } catch (IOException e) {
            NettyHandlerHelper.sendError(ctx, "非法的文件名: " + e.getMessage());
            return;
        }

        if (!file.exists()) {
            LOG.warn("[Pull] 源侧文件不存在: {}", fileHash);
            NettyHandlerHelper.sendError(ctx, "文件不存在");
            return;
        }

        LOG.info("[Pull] 源侧开始发送副本: {}, size={}", fileHash, file.length());

        long fileLength = file.length();
        Packet response = new Packet();
        response.setCommandType(CommandType.DATA_REPLICA_PULL_RESPONSE);
        response.setData(String.valueOf(fileLength).getBytes(StandardCharsets.UTF_8));
        response.setStreamLength(fileLength); // 设置流长度，让目标端 PacketDecoder 正确进入流式模式
        ctx.write(response);

        DefaultFileRegion region = new DefaultFileRegion(file, 0, fileLength);
        ctx.writeAndFlush(region);
    }

    // ==================== 副本拉取相关（目标侧） ====================

    /**
     * 目标侧：处理 NameNode 发来的 DATA_REPLICA_PULL_CMD
     * 步骤 1-2：解析 payload → 派发到后台拉取线程 → 立即 ACK
     *
     * payload 格式: fileHash|sourceHost:port|namenodeHost:port
     */
    private void handleReplicaPullCmd(ChannelHandlerContext ctx, Packet packet) {
        if (pullExecutor == null || outboundWorkerGroup == null) {
            NettyHandlerHelper.sendError(ctx, "本节点未启用副本拉取能力");
            return;
        }

        String payload = new String(packet.getData(), StandardCharsets.UTF_8);
        // 三段 | 分隔
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 3) {
            NettyHandlerHelper.sendError(ctx, "非法的 PULL_CMD payload 格式");
            return;
        }

        String fileHash = parts[0].trim();
        String[] sourceAddr = parseHostPort(parts[1].trim(), "source");
        String[] namenodeAddr = parseHostPort(parts[2].trim(), "namenode");
        if (sourceAddr == null || namenodeAddr == null) {
            NettyHandlerHelper.sendError(ctx, "非法的 PULL_CMD 地址格式");
            return;
        }

        // 校验 hash 格式（防止注入非法字符构造路径）
        if (!fileHash.matches("^[a-zA-Z0-9]+$")) {
            NettyHandlerHelper.sendError(ctx, "非法的 fileHash");
            return;
        }

        // 构造拉取任务（先 submit 成功再 ACK，避免 NameNode 误以为任务已接受但实际丢失）
        ReplicaPullWorker worker = new ReplicaPullWorker(
                fileHash,
                nodeId,
                sourceAddr[0], Integer.parseInt(sourceAddr[1]),
                namenodeAddr[0], Integer.parseInt(namenodeAddr[1]),
                storagePaths,
                outboundWorkerGroup,
                pullRateLimitBytesPerSec);
        try {
            pullExecutor.submit(worker);
            // submit 成功后才 ACK（用 UPLOAD_RESPONSE 作为通用成功包，Phase 5 只关心不是 ERROR）
            NettyHandlerHelper.sendResponse(ctx, CommandType.UPLOAD_RESPONSE,
                    ("PULL_ACCEPTED: " + fileHash).getBytes(StandardCharsets.UTF_8));
            LOG.info("[Pull] 已派发拉取任务: hash={}, source={}:{}, namenode={}:{}",
                    fileHash, sourceAddr[0], sourceAddr[1], namenodeAddr[0], namenodeAddr[1]);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // submit 失败（池满/已关闭）→ ACK ERROR，NameNode 可立即重试或等下一轮对账
            LOG.warn("[Pull] 拉取任务被拒绝（池满/关闭）: hash={}", fileHash);
            NettyHandlerHelper.sendError(ctx, "拉取任务被拒绝，请重试");
        }
    }

    /**
     * 解析 host:port 字符串，返回 [host, port] 或 null（格式非法）
     */
    private String[] parseHostPort(String addr, String label) {
        int colon = addr.lastIndexOf(':');
        if (colon <= 0 || colon >= addr.length() - 1) {
            LOG.warn("[Pull] 非法的 {} 地址格式: {}", label, addr);
            return null;
        }
        try {
            String host = addr.substring(0, colon);
            String port = addr.substring(colon + 1);
            return new String[]{host, port};
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 根据 Hash (fileName) 获取存储路径
     * 规则: 1-2位为一级目录, 3-4位为二级目录
     *
     * 多路径策略:
     * 1. 如果文件已存在于任一路径，返回该路径的文件对象。
     * 2. 如果文件不存在，选择剩余空间最大的路径。
     *
     * 安全修复: 增加路径遍历检查和文件名格式校验
     */
    private File getStorageFile(String hash) throws IOException {
        if (hash == null || hash.isEmpty()) {
            throw new IOException("文件名为空");
        }

        // 1. 严格校验文件名格式 (仅允许字母数字，禁止 .. / \ 等特殊字符)
        if (!hash.matches("^[a-zA-Z0-9]+$")) {
            throw new IOException("非法的文件名/Hash检测 (包含非法字符): " + hash);
        }

        // 路径计算逻辑
        String dir1 = hash.length() >= 2 ? hash.substring(0, 2) : "00";
        String dir2 = hash.length() >= 4 ? hash.substring(2, 4) : "00";
        String relativePath = dir1 + File.separator + dir2;

        // 2. 检查文件是否已存在 (读优先)
        for (String path : storagePaths) {
            File rootDir = new File(path).getCanonicalFile();
            File target = new File(rootDir, relativePath + File.separator + hash);
            // 简单校验防止遍历
            if (target.getCanonicalPath().startsWith(rootDir.getPath()) && target.exists()) {
                 return target;
            }
        }

        // 3. 文件不存在，选择剩余空间最大的路径 (写策略)
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
        validatePath(target, rootDir);
        return target;
    }

    private void validatePath(File target, File rootDir) throws IOException {
        if (!target.getCanonicalPath().startsWith(rootDir.getPath())) {
            throw new IOException("路径遍历攻击检测: " + target.getName());
        }
    }

    private void closeCurrentFile() {
        try {
            if (currentFileChannel != null) {
                currentFileChannel.close();
            }
            if (currentFos != null) {
                currentFos.close();
            }
        } catch (IOException e) {
            LOG.error("关闭文件流失败", e);
        }
        currentFileChannel = null;
        currentFos = null;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        closeCurrentFile();
        // 只有在上传未完成时才删除临时文件
        // 区分：如果是下载连接断开，currentTmpFile 为空，不会误删
        // 如果是上传完成但还没来得及清理状态(理论上finishUpload已清理)，这里也是安全的
        if (currentTmpFile != null && currentTmpFile.exists()) {
            try {
                // 判断是否已经接收完毕
                if (receivedBytes < currentFileSize) {
                    currentTmpFile.delete();
                    LOG.info("连接异常断开，清理未完成临时文件: {}", currentTmpFile.getAbsolutePath());
                }
            } catch (Exception ignore) {
            }
        }
        resetState();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("DataNodeHandler异常", cause);
        closeCurrentFile();
        ctx.close();
    }
}
