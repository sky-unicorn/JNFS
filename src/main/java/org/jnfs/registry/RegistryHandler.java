package org.jnfs.registry;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 注册中心业务处理器
 * 维护服务列表和心跳
 */
public class RegistryHandler extends SimpleChannelInboundHandler<Packet> {

    // 预设的安全令牌 (与 NameNode/DataNode 保持一致)
    private static final String VALID_TOKEN = "jnfs-secure-token-2025";

    // DataNode 列表: address -> lastHeartbeatTime
    private static final Map<String, Long> dataNodes = new ConcurrentHashMap<>();
    // 心跳超时时间 (毫秒)，超过此时间未收到心跳则认为节点下线
    private static final long HEARTBEAT_TIMEOUT = 30 * 1000;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        // 安全认证
        if (!VALID_TOKEN.equals(packet.getToken())) {
            System.out.println("Registry 安全拦截: 无效的 Token - " + ctx.channel().remoteAddress());
            sendResponse(ctx, CommandType.ERROR, "Authentication Failed".getBytes(StandardCharsets.UTF_8));
            ctx.close();
            return;
        }

        CommandType type = packet.getCommandType();
        switch (type) {
            case REGISTRY_REGISTER:
            case REGISTRY_HEARTBEAT:
                handleRegisterOrHeartbeat(ctx, packet);
                break;
            case REGISTRY_GET_DATANODES:
                handleGetDataNodes(ctx);
                break;
            default:
                sendResponse(ctx, CommandType.ERROR, "未知命令".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleRegisterOrHeartbeat(ChannelHandlerContext ctx, Packet packet) {
        String address = new String(packet.getData(), StandardCharsets.UTF_8);
        
        // 更新最后心跳时间
        dataNodes.put(address, System.currentTimeMillis());
        
        if (packet.getCommandType() == CommandType.REGISTRY_REGISTER) {
            System.out.println("DataNode 注册成功: " + address);
            sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER, "OK".getBytes(StandardCharsets.UTF_8));
        } else {
            // System.out.println("收到心跳: " + address); // 心跳日志太频繁，可注释
        }
    }

    private void handleGetDataNodes(ChannelHandlerContext ctx) {
        long now = System.currentTimeMillis();
        List<String> activeNodes = new ArrayList<>();
        
        // 遍历并清理过期节点
        dataNodes.entrySet().removeIf(entry -> (now - entry.getValue()) > HEARTBEAT_TIMEOUT);
        
        activeNodes.addAll(dataNodes.keySet());
        
        // 响应格式: node1,node2,node3
        String response = String.join(",", activeNodes);
        sendResponse(ctx, CommandType.REGISTRY_RESPONSE_DATANODES, response.getBytes(StandardCharsets.UTF_8));
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
