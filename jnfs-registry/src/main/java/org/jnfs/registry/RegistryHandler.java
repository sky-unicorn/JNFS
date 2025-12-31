package org.jnfs.registry;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 注册中心业务处理器
 * 维护服务列表和心跳
 * 
 * 修复：增加主动清理过期节点的定时任务，防止被动过期导致的内存泄漏
 */
public class RegistryHandler extends SimpleChannelInboundHandler<Packet> {

    private static final String VALID_TOKEN = "jnfs-secure-token-2025";

    // 节点信息内部类
    public static class NodeInfo {
        public long lastHeartbeatTime;
        public long freeSpace;

        NodeInfo(long lastHeartbeatTime, long freeSpace) {
            this.lastHeartbeatTime = lastHeartbeatTime;
            this.freeSpace = freeSpace;
        }
    }

    // DataNode 列表: address -> NodeInfo
    private static final Map<String, NodeInfo> dataNodes = new ConcurrentHashMap<>();
    
    private static final long HEARTBEAT_TIMEOUT = 30 * 1000;

    // 主动清理过期节点的定时任务
    private static final ScheduledExecutorService cleanerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "Registry-Cleaner");
        t.setDaemon(true); // 设置为守护线程，随JVM退出
        return t;
    });

    static {
        // 每 10 秒执行一次清理检查
        cleanerExecutor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();
                int initialSize = dataNodes.size();
                dataNodes.entrySet().removeIf(entry -> (now - entry.getValue().lastHeartbeatTime) > HEARTBEAT_TIMEOUT);
                int finalSize = dataNodes.size();
                if (initialSize != finalSize) {
                    System.out.println("[Registry-Cleaner] 清理了 " + (initialSize - finalSize) + " 个过期节点");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 暴露给 Dashboard 使用
     */
    public static Map<String, NodeInfo> getDataNodes() {
        return Collections.unmodifiableMap(dataNodes);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
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
        String payload = new String(packet.getData(), StandardCharsets.UTF_8);
        String address;
        long freeSpace = 0;
        
        if (payload.contains("|")) {
            String[] parts = payload.split("\\|");
            address = parts[0];
            try {
                freeSpace = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else {
            address = payload;
        }
        
        dataNodes.put(address, new NodeInfo(System.currentTimeMillis(), freeSpace));
        
        if (packet.getCommandType() == CommandType.REGISTRY_REGISTER) {
            System.out.println("DataNode 注册成功: " + address);
            sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleGetDataNodes(ChannelHandlerContext ctx) {
        long now = System.currentTimeMillis();
        List<String> activeNodes = new ArrayList<>();
        
        dataNodes.entrySet().removeIf(entry -> (now - entry.getValue().lastHeartbeatTime) > HEARTBEAT_TIMEOUT);
        
        for (Map.Entry<String, NodeInfo> entry : dataNodes.entrySet()) {
            activeNodes.add(entry.getKey() + "|" + entry.getValue().freeSpace);
        }
        
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
