package org.jnfs.namenode;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * NameNode 业务处理器
 * 处理客户端的元数据请求
 */
public class NameNodeHandler extends SimpleChannelInboundHandler<Packet> {

    // 文件名映射: filename -> "hash"
    private static final Map<String, String> filenameToHash = new ConcurrentHashMap<>();

    // 哈希索引: hash -> "host:port"
    private static final Map<String, String> hashToStorage = new ConcurrentHashMap<>();

    // 正在上传中的 Hash 集合 (并发控制)
    private static final Set<String> pendingUploads = ConcurrentHashMap.newKeySet();

    // 元数据持久化管理器
    private static final MetadataManager metadataManager = new MetadataManager();

    // 活跃的 DataNode 列表
    private static final List<String> dataNodes = new ArrayList<>();

    static {
        // 启动时恢复元数据
        metadataManager.recover(filenameToHash, hashToStorage);
    }

    /**
     * 初始化 DataNode 列表 (由 NameNodeServer 启动时调用)
     */
    public static void initDataNodes(List<String> nodes) {
        dataNodes.clear();
        if (nodes != null) {
            dataNodes.addAll(nodes);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        CommandType type = packet.getCommandType();
        switch (type) {
            case NAMENODE_CHECK_EXISTENCE:
                handleCheckExistence(ctx, packet);
                break;
            case NAMENODE_PRE_UPLOAD:
                handlePreUpload(ctx, packet);
                break;
            case NAMENODE_REQUEST_UPLOAD_LOC:
                handleUploadLocRequest(ctx);
                break;
            case NAMENODE_COMMIT_FILE:
                handleCommitFile(ctx, packet);
                break;
            case NAMENODE_REQUEST_DOWNLOAD_LOC:
                handleDownloadLocRequest(ctx, packet);
                break;
            default:
                sendResponse(ctx, CommandType.ERROR, "未知命令".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleCheckExistence(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);
        String storageAddr = hashToStorage.get(hash);
        
        if (storageAddr != null) {
            System.out.println("命中秒传: Hash=" + hash);
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, storageAddr.getBytes(StandardCharsets.UTF_8));
        } else {
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_NOT_EXIST, "Not Found".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * 处理预上传申请 (并发控制核心逻辑)
     * Payload: hash
     */
    private void handlePreUpload(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);
        
        // 全局加锁或同步块，确保状态检查和更新的原子性
        // 虽然 Map 是并发安全的，但我们需要组合检查多个条件
        synchronized (hashToStorage) {
            // 1. 再次检查是否已存在 (可能刚刚被别人传完)
            String storageAddr = hashToStorage.get(hash);
            if (storageAddr != null) {
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, storageAddr.getBytes(StandardCharsets.UTF_8));
                return;
            }

            // 2. 检查是否有人正在上传
            if (pendingUploads.contains(hash)) {
                System.out.println("并发上传冲突，通知等待: Hash=" + hash);
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_WAIT, "Waiting".getBytes(StandardCharsets.UTF_8));
                return;
            }

            // 3. 标记为正在上传
            pendingUploads.add(hash);
            System.out.println("允许上传: Hash=" + hash);
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_ALLOW, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleUploadLocRequest(ChannelHandlerContext ctx) {
        if (dataNodes.isEmpty()) {
            sendResponse(ctx, CommandType.ERROR, "无可用 DataNode".getBytes(StandardCharsets.UTF_8));
            return;
        }
        
        // 简单负载均衡: 随机选择
        String selectedNode = dataNodes.get(ThreadLocalRandom.current().nextInt(dataNodes.size()));
        sendResponse(ctx, CommandType.NAMENODE_RESPONSE_UPLOAD_LOC, selectedNode.getBytes(StandardCharsets.UTF_8));
    }

    private void handleCommitFile(ChannelHandlerContext ctx, Packet packet) {
        String data = new String(packet.getData(), StandardCharsets.UTF_8);
        String[] parts = data.split("\\|");
        if (parts.length != 3) {
            sendResponse(ctx, CommandType.ERROR, "格式错误".getBytes(StandardCharsets.UTF_8));
            return;
        }
        
        String filename = parts[0];
        String hash = parts[1];
        String address = parts[2];
        
        synchronized (hashToStorage) {
            // 上传完成，移除 Pending 状态
            pendingUploads.remove(hash);

            // 检查元数据是否已存在，防止重复记录日志
            String existingHash = filenameToHash.get(filename);
            String existingAddr = hashToStorage.get(hash);
            
            boolean metaExists = hash.equals(existingHash);
            boolean dataExists = address.equals(existingAddr);

            if (metaExists && dataExists) {
                System.out.println("忽略重复元数据提交: " + filename);
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, "OK (Dedup)".getBytes(StandardCharsets.UTF_8));
                return;
            }

            // 1. 先持久化
            metadataManager.logAddFile(filename, hash, address);

            // 2. 再更新内存
            filenameToHash.put(filename, hash);
            hashToStorage.put(hash, address);
            
            System.out.println("文件已注册: " + filename);
        }
        
        sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, "OK".getBytes(StandardCharsets.UTF_8));
    }

    private void handleDownloadLocRequest(ChannelHandlerContext ctx, Packet packet) {
        String filename = new String(packet.getData(), StandardCharsets.UTF_8);
        String hash = filenameToHash.get(filename);
        
        if (hash != null) {
            String address = hashToStorage.get(hash);
            if (address != null) {
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_DOWNLOAD_LOC, address.getBytes(StandardCharsets.UTF_8));
                return;
            }
        }
        
        sendResponse(ctx, CommandType.ERROR, "文件不存在".getBytes(StandardCharsets.UTF_8));
    }

    private void sendResponse(ChannelHandlerContext ctx, CommandType type, byte[] data) {
        Packet response = new Packet();
        response.setCommandType(type);
        response.setData(data);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
