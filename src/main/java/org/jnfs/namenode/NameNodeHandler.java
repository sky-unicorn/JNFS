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
import java.util.UUID;
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

    // 存储编号索引: hash -> "storageId"
    private static final Map<String, String> hashToId = new ConcurrentHashMap<>();
    
    // 反向索引: storageId -> "hash"
    private static final Map<String, String> idToHash = new ConcurrentHashMap<>();

    // 已持久化ID的 Hash 集合 (用于去重)
    private static final Set<String> persistedHashes = ConcurrentHashMap.newKeySet();

    // 正在上传中的 Hash 集合 (并发控制)
    private static final Set<String> pendingUploads = ConcurrentHashMap.newKeySet();

    // 元数据持久化管理器
    private static final MetadataManager metadataManager = new MetadataManager();

    // 活跃的 DataNode 列表
    private static final List<String> dataNodes = new ArrayList<>();

    static {
        // 启动时恢复元数据
        metadataManager.recover(filenameToHash, hashToStorage, hashToId, persistedHashes);
        // 重建 idToHash 索引
        for (Map.Entry<String, String> entry : hashToId.entrySet()) {
            idToHash.put(entry.getValue(), entry.getKey());
        }
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

    private void handlePreUpload(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);
        
        synchronized (hashToStorage) {
            String storageAddr = hashToStorage.get(hash);
            if (storageAddr != null) {
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, storageAddr.getBytes(StandardCharsets.UTF_8));
                return;
            }

            if (pendingUploads.contains(hash)) {
                System.out.println("并发上传冲突，通知等待: Hash=" + hash);
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_WAIT, "Waiting".getBytes(StandardCharsets.UTF_8));
                return;
            }

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
        String storageId;

        synchronized (hashToStorage) {
            pendingUploads.remove(hash);

            // 获取或生成 Storage ID
            storageId = hashToId.computeIfAbsent(hash, k -> UUID.randomUUID().toString());
            idToHash.put(storageId, hash); // 更新反向索引

            // 检查该 Hash 的 ID 是否已经持久化
            if (persistedHashes.contains(hash)) {
                System.out.println("忽略重复元数据提交 (ID已持久化): " + filename);
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, storageId.getBytes(StandardCharsets.UTF_8));
                return;
            }

            // 1. 先持久化
            metadataManager.logAddFile(filename, hash, address, storageId);

            // 2. 再更新内存
            filenameToHash.put(filename, hash);
            hashToStorage.put(hash, address);
            hashToId.put(hash, storageId);
            idToHash.put(storageId, hash);
            
            // 标记已持久化
            persistedHashes.add(hash);
            
            System.out.println("文件已注册并持久化: " + filename + ", ID: " + storageId);
        }
        
        sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, storageId.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理下载位置请求 (By Storage ID)
     * 返回: filename|hash|dataNodeAddr
     */
    private void handleDownloadLocRequest(ChannelHandlerContext ctx, Packet packet) {
        String storageId = new String(packet.getData(), StandardCharsets.UTF_8);
        String hash = idToHash.get(storageId);
        
        if (hash != null) {
            String address = hashToStorage.get(hash);
            // 查找任意一个文件名 (由于是基于 Hash 存储，文件名可能不唯一，这里简单反查一个即可)
            // 实际上对于纯粹的 Blob 存储，文件名可能不重要，或者应该由 NameNode 记录原始文件名
            // 这里我们遍历 filenameToHash 找到对应的文件名 (性能较低，生产环境应优化索引)
            String filename = "unknown";
            for (Map.Entry<String, String> entry : filenameToHash.entrySet()) {
                if (entry.getValue().equals(hash)) {
                    filename = entry.getKey();
                    break;
                }
            }

            if (address != null) {
                // Payload: filename|hash|address
                String response = filename + "|" + hash + "|" + address;
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_DOWNLOAD_LOC, response.getBytes(StandardCharsets.UTF_8));
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
