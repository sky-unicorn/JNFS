package org.jnfs.registry;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
@ChannelHandler.Sharable
public class RegistryHandler extends SimpleChannelInboundHandler<Packet> {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryHandler.class);

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
    // NameNode 列表: address -> NodeInfo
    private static final Map<String, NodeInfo> nameNodes = new ConcurrentHashMap<>();

    // 心跳超时时间 (默认30秒)，可由 RegistryServer 启动时修改
    public static volatile long heartbeatTimeout = 30 * 1000;

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

                int dnInit = dataNodes.size();
                dataNodes.entrySet().removeIf(entry -> (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout);
                int dnFinal = dataNodes.size();
                if (dnInit != dnFinal) {
                    LOG.info("[Registry-Cleaner] 清理了 {} 个过期 DataNode", dnInit - dnFinal);
                }

                int nnInit = nameNodes.size();
                nameNodes.entrySet().removeIf(entry -> (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout);
                int nnFinal = nameNodes.size();
                if (nnInit != nnFinal) {
                    LOG.info("[Registry-Cleaner] 清理了 {} 个过期 NameNode", nnInit - nnFinal);
                }
            } catch (Exception e) {
                LOG.error("Registry清理任务异常", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    /**
     * 暴露给 Dashboard 使用
     */
    public static Map<String, NodeInfo> getDataNodes() {
        return Collections.unmodifiableMap(dataNodes);
    }

    public static Map<String, NodeInfo> getNameNodes() {
        return Collections.unmodifiableMap(nameNodes);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        if (!VALID_TOKEN.equals(packet.getToken())) {
            LOG.warn("Registry 安全拦截: 无效的 Token - {}", ctx.channel().remoteAddress());
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
            case REGISTRY_REGISTER_NAMENODE:
            case REGISTRY_HEARTBEAT_NAMENODE:
                handleRegisterOrHeartbeatNameNode(ctx, packet);
                break;
            case REGISTRY_GET_NAMENODES:
                handleGetNameNodes(ctx);
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
            LOG.info("DataNode 注册成功: {}", address);
            sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleRegisterOrHeartbeatNameNode(ChannelHandlerContext ctx, Packet packet) {
        String address = new String(packet.getData(), StandardCharsets.UTF_8);

        nameNodes.put(address, new NodeInfo(System.currentTimeMillis(), 0));

        if (packet.getCommandType() == CommandType.REGISTRY_REGISTER_NAMENODE) {
            LOG.info("NameNode 注册成功: {}", address);
            sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER_NAMENODE, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleGetDataNodes(ChannelHandlerContext ctx) {
        long now = System.currentTimeMillis();
        List<String> activeNodes = new ArrayList<>();

        dataNodes.entrySet().removeIf(entry -> (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout);

        for (Map.Entry<String, NodeInfo> entry : dataNodes.entrySet()) {
            activeNodes.add(entry.getKey() + "|" + entry.getValue().freeSpace);
        }

        String response = String.join(",", activeNodes);
        sendResponse(ctx, CommandType.REGISTRY_RESPONSE_DATANODES, response.getBytes(StandardCharsets.UTF_8));
    }

    private void handleGetNameNodes(ChannelHandlerContext ctx) {
        long now = System.currentTimeMillis();
        List<String> activeNodes = new ArrayList<>();

        nameNodes.entrySet().removeIf(entry -> (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout);

        for (String address : nameNodes.keySet()) {
            activeNodes.add(address);
        }

        String response = String.join(",", activeNodes);
        sendResponse(ctx, CommandType.REGISTRY_RESPONSE_NAMENODES, response.getBytes(StandardCharsets.UTF_8));
    }

    private void sendResponse(ChannelHandlerContext ctx, CommandType type, byte[] data) {
        Packet response = new Packet();
        response.setCommandType(type);
        response.setData(data);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("RegistryHandler异常", cause);
        ctx.close();
    }
}
