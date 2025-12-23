package org.jnfs.namenode;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    // 元数据持久化管理器
    private static final MetadataManager metadataManager = new MetadataManager();

    // 模拟活跃的 DataNode 列表
    private static final List<String> dataNodes = new ArrayList<>();

    static {
        // 硬编码一个 DataNode 用于演示
        dataNodes.add("localhost:8080");
        
        // 启动时恢复元数据
        metadataManager.recover(filenameToHash, hashToStorage);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        CommandType type = packet.getCommandType();
        // 生产环境高并发下建议降低日志级别或去掉
        // System.out.println("NameNode 收到请求: " + type);

        switch (type) {
            case NAMENODE_CHECK_EXISTENCE:
                handleCheckExistence(ctx, packet);
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
        
        // 1. 先持久化
        metadataManager.logAddFile(filename, hash, address);

        // 2. 再更新内存
        filenameToHash.put(filename, hash);
        hashToStorage.put(hash, address);
        
        System.out.println("文件已注册: " + filename);
        
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
