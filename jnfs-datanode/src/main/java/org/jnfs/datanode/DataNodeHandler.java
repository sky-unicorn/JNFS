package org.jnfs.datanode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * DataNode 业务处理器
 * 处理文件上传和下载的数据流
 *
 */
public class DataNodeHandler extends SimpleChannelInboundHandler<Object> {

    // 预设的安全令牌 (与 NameNode 保持一致)
    private static final String VALID_TOKEN = "jnfs-secure-token-2025";
    // 临时文件后缀
    public static final String TMP_SUFFIX = ".tmp";

    private final List<String> storagePaths;

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

    public DataNodeHandler(List<String> storagePaths) {
        this.storagePaths = storagePaths;
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
        if (!VALID_TOKEN.equals(packet.getToken())) {
            System.out.println("安全拦截: 无效的 Token");
            sendResponse(ctx, CommandType.ERROR, "Authentication Failed".getBytes(StandardCharsets.UTF_8));
            ctx.close();
            return;
        }

        if (packet.getCommandType() == CommandType.UPLOAD_REQUEST) {
            initiateUpload(ctx, packet);
        } else if (packet.getCommandType() == CommandType.DOWNLOAD_REQUEST) {
            handleDownload(ctx, packet);
        }
    }

    private void initiateUpload(ChannelHandlerContext ctx, Packet packet) {
        byte[] data = packet.getData();
        if (data == null || data.length == 0) {
            sendResponse(ctx, CommandType.ERROR, "无效的元数据".getBytes(StandardCharsets.UTF_8));
            return;
        }

        long fileSize = packet.getStreamLength();

        String fileName = new String(data, StandardCharsets.UTF_8);

        System.out.println("准备接收文件: " + fileName + ", 大小: " + fileSize + " 字节");

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
            e.printStackTrace();
            sendResponse(ctx, CommandType.ERROR, ("服务端错误: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
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
            e.printStackTrace();
            closeCurrentFile();
            sendResponse(ctx, CommandType.ERROR, ("写入错误: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    // 文件锁对象，用于同步文件操作
    private static final Object FILE_LOCK = new Object();

    private void finishUpload(ChannelHandlerContext ctx) {
        closeCurrentFile();

        // 重命名 .tmp -> 正式文件
        File finalFile;
        try {
            finalFile = getStorageFile(currentFileName);
        } catch (IOException e) {
            e.printStackTrace();
            // 尝试手动删除失败的 tmp
            if (currentTmpFile != null) currentTmpFile.delete();
            sendResponse(ctx, CommandType.ERROR, ("文件存储失败(路径校验错误): " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
            return;
        }

        // 引入文件锁，确保存在性检查和重命名操作的原子性
        synchronized (FILE_LOCK) {
            // 如果目标文件已存在，直接删除临时文件并返回成功 (视为幂等上传)
            if (finalFile.exists()) {
                System.out.println("文件已存在，跳过重命名: " + currentFileName);
                currentTmpFile.delete();
                sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功(秒传): " + currentFileName).getBytes(StandardCharsets.UTF_8));
                // 重置状态
                resetState();
                return;
            }

            if (currentTmpFile.renameTo(finalFile)) {
                System.out.println("文件存储完成: " + currentFileName);
                sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功: " + currentFileName).getBytes(StandardCharsets.UTF_8));
            } else {
                // 双重检查: 可能在重命名的一瞬间被其他线程抢先了 (虽然有 FILE_LOCK，但防御性编程)
                if (finalFile.exists()) {
                     System.out.println("重命名失败但文件已存在 (并发上传): " + currentFileName);
                     currentTmpFile.delete();
                     sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功: " + currentFileName).getBytes(StandardCharsets.UTF_8));
                } else {
                    System.err.println("重命名临时文件失败: " + currentTmpFile.getAbsolutePath());
                    // 尝试手动删除失败的 tmp
                    currentTmpFile.delete();
                    sendResponse(ctx, CommandType.ERROR, "文件存储失败(重命名错误)".getBytes(StandardCharsets.UTF_8));
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
            sendResponse(ctx, CommandType.ERROR, ("非法的文件名: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
            return;
        }

        if (!file.exists()) {
            sendResponse(ctx, CommandType.ERROR, "文件不存在".getBytes(StandardCharsets.UTF_8));
            return;
        }

        System.out.println("开始发送文件: " + filename);

        long fileLength = file.length();
        Packet response = new Packet();
        response.setCommandType(CommandType.DOWNLOAD_RESPONSE);
        response.setData(String.valueOf(fileLength).getBytes(StandardCharsets.UTF_8));
        response.setStreamLength(fileLength); // 设置流长度，让 Client 正确跳过 Header
        ctx.write(response);

        DefaultFileRegion region = new DefaultFileRegion(file, 0, fileLength);
        ctx.writeAndFlush(region);
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
            e.printStackTrace();
        }
        currentFileChannel = null;
        currentFos = null;
    }

    private void sendResponse(ChannelHandlerContext ctx, CommandType type, byte[] data) {
        Packet response = new Packet();
        response.setCommandType(type);
        response.setData(data);
        ctx.writeAndFlush(response);
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
                    System.out.println("连接异常断开，清理未完成临时文件: " + currentTmpFile.getAbsolutePath());
                }
            } catch (Exception ignore) {
            }
        }
        resetState();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeCurrentFile();
        ctx.close();
    }
}
