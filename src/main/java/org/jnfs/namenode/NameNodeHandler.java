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

    // 模拟文件注册表: 文件名 -> DataNode地址列表 (这里简单起见，只存一个地址)
    // 格式: filename -> "host:port"
    private static final Map<String, String> fileRegistry = new ConcurrentHashMap<>();

    // 模拟活跃的 DataNode 列表 (实际应由 DataNode 注册上来)
    private static final List<String> dataNodes = new ArrayList<>();

    static {
        // 硬编码一个 DataNode 用于演示
        dataNodes.add("localhost:8080");
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        CommandType type = packet.getCommandType();
        System.out.println("NameNode 收到请求: " + type);

        switch (type) {
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

    /**
     * 处理上传位置请求
     * 简单负载均衡：随机选择一个 DataNode 返回
     */
    private void handleUploadLocRequest(ChannelHandlerContext ctx) {
        if (dataNodes.isEmpty()) {
            sendResponse(ctx, CommandType.ERROR, "无可用 DataNode".getBytes(StandardCharsets.UTF_8));
            return;
        }
        
        // 随机选择一个 DataNode
        String selectedNode = dataNodes.get(ThreadLocalRandom.current().nextInt(dataNodes.size()));
        sendResponse(ctx, CommandType.NAMENODE_RESPONSE_UPLOAD_LOC, selectedNode.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理文件提交请求
     * 客户端上传完毕后，告知 NameNode 文件位置
     * 数据格式: "filename|host:port"
     */
    private void handleCommitFile(ChannelHandlerContext ctx, Packet packet) {
        String data = new String(packet.getData(), StandardCharsets.UTF_8);
        String[] parts = data.split("\\|");
        if (parts.length != 2) {
            sendResponse(ctx, CommandType.ERROR, "格式错误".getBytes(StandardCharsets.UTF_8));
            return;
        }
        
        String filename = parts[0];
        String address = parts[1];
        
        fileRegistry.put(filename, address);
        System.out.println("文件已注册: " + filename + " -> " + address);
        
        sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, "OK".getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理下载位置请求
     * 查找文件所在的 DataNode
     */
    private void handleDownloadLocRequest(ChannelHandlerContext ctx, Packet packet) {
        String filename = new String(packet.getData(), StandardCharsets.UTF_8);
        String address = fileRegistry.get(filename);
        
        if (address != null) {
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_DOWNLOAD_LOC, address.getBytes(StandardCharsets.UTF_8));
        } else {
            sendResponse(ctx, CommandType.ERROR, "文件不存在".getBytes(StandardCharsets.UTF_8));
        }
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
