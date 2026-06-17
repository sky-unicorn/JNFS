package org.jnfs.registry;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.NettyHandlerHelper;
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
 * 升级：支持 node_id 作为节点标识，IP变更时自动更新映射
 * 兼容：支持旧版本 DataNode/NameNode（不带 node_id）的心跳协议
 */
@ChannelHandler.Sharable
public class RegistryHandler extends SimpleChannelInboundHandler<Packet> {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryHandler.class);

    // 节点信息内部类
    public static class NodeInfo {
        public String nodeId;
        public String address; // 当前 host:port
        public long lastHeartbeatTime;
        public long freeSpace;

        NodeInfo(String nodeId, String address, long lastHeartbeatTime, long freeSpace) {
            this.nodeId = nodeId;
            this.address = address;
            this.lastHeartbeatTime = lastHeartbeatTime;
            this.freeSpace = freeSpace;
        }
    }

    // DataNode 列表: nodeId -> NodeInfo
    private static final Map<String, NodeInfo> dataNodes = new ConcurrentHashMap<>();
    // NameNode 列表: nodeId -> NodeInfo
    private static final Map<String, NodeInfo> nameNodes = new ConcurrentHashMap<>();
    // 反向映射: host:port -> nodeId (用于快速查找)
    private static final Map<String, String> addressToDataNodeId = new ConcurrentHashMap<>();
    private static final Map<String, String> addressToNameNodeId = new ConcurrentHashMap<>();

    // 心跳超时时间 (默认30秒)，可由 RegistryServer 启动时修改
    public static volatile long heartbeatTimeout = 30 * 1000;

    // 主动清理过期节点的定时任务 (使用统一的 Daemon 线程工厂)
    private static final ScheduledExecutorService cleanerExecutor = Executors.newSingleThreadScheduledExecutor(
            new DaemonThreadFactory("Registry-Cleaner"));

    static {
        // 每 10 秒执行一次清理检查
        cleanerExecutor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();

                int dnInit = dataNodes.size();
                dataNodes.entrySet().removeIf(entry -> {
                    boolean expired = (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout;
                    if (expired) {
                        addressToDataNodeId.remove(entry.getValue().address);
                    }
                    return expired;
                });
                int dnFinal = dataNodes.size();
                if (dnInit != dnFinal) {
                    LOG.info("[Registry-Cleaner] 清理了 {} 个过期 DataNode", dnInit - dnFinal);
                }

                int nnInit = nameNodes.size();
                nameNodes.entrySet().removeIf(entry -> {
                    boolean expired = (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout;
                    if (expired) {
                        addressToNameNodeId.remove(entry.getValue().address);
                    }
                    return expired;
                });
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
        if (!NettyHandlerHelper.validateToken(packet.getToken())) {
            LOG.warn("Registry 安全拦截: 无效的 Token - {}", ctx.channel().remoteAddress());
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "Authentication Failed".getBytes(StandardCharsets.UTF_8));
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
                NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "未知命令".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleRegisterOrHeartbeat(ChannelHandlerContext ctx, Packet packet) {
        String payload = new String(packet.getData(), StandardCharsets.UTF_8);
        String nodeId;
        String address;
        long freeSpace = 0;

        // 解析 payload，兼容新旧格式
        // 新格式: node_id|host:port|freeSpace
        // 旧格式: host:port|freeSpace 或 host:port
        String[] parts = payload.split("\\|");
        if (parts.length == 3) {
            // 新格式: node_id|host:port|freeSpace
            nodeId = parts[0];
            address = parts[1];
            try {
                freeSpace = Long.parseLong(parts[2]);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else if (parts.length == 2) {
            // 旧格式: host:port|freeSpace
            address = parts[0];
            nodeId = address; // fallback: 用 host:port 作为 node_id
            try {
                freeSpace = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else {
            // 旧格式: host:port (无 freeSpace)
            address = payload;
            nodeId = address;
        }

        // 更新节点信息
        NodeInfo existing = dataNodes.get(nodeId);
        if (existing != null) {
            // IP变更检测：node_id 相同但地址不同
            if (!existing.address.equals(address)) {
                LOG.info("DataNode IP变更: nodeId={}, 旧地址={}, 新地址={}", nodeId, existing.address, address);
                addressToDataNodeId.remove(existing.address);
            }
        }

        dataNodes.put(nodeId, new NodeInfo(nodeId, address, System.currentTimeMillis(), freeSpace));
        addressToDataNodeId.put(address, nodeId);

        if (packet.getCommandType() == CommandType.REGISTRY_REGISTER) {
            LOG.info("DataNode 注册成功: nodeId={}, address={}", nodeId, address);
            NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleRegisterOrHeartbeatNameNode(ChannelHandlerContext ctx, Packet packet) {
        String payload = new String(packet.getData(), StandardCharsets.UTF_8);
        String nodeId;
        String address;

        // 解析 payload，兼容新旧格式
        // 新格式: node_id|host:port
        // 旧格式: host:port
        if (payload.contains("|")) {
            String[] parts = payload.split("\\|");
            nodeId = parts[0];
            address = parts[1];
        } else {
            // 旧格式兼容
            address = payload;
            nodeId = address; // fallback
        }

        // 更新节点信息
        NodeInfo existing = nameNodes.get(nodeId);
        if (existing != null) {
            // IP变更检测
            if (!existing.address.equals(address)) {
                LOG.info("NameNode IP变更: nodeId={}, 旧地址={}, 新地址={}", nodeId, existing.address, address);
                addressToNameNodeId.remove(existing.address);
            }
        }

        nameNodes.put(nodeId, new NodeInfo(nodeId, address, System.currentTimeMillis(), 0));
        addressToNameNodeId.put(address, nodeId);

        if (packet.getCommandType() == CommandType.REGISTRY_REGISTER_NAMENODE) {
            LOG.info("NameNode 注册成功: nodeId={}, address={}", nodeId, address);
            NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER_NAMENODE, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleGetDataNodes(ChannelHandlerContext ctx) {
        long now = System.currentTimeMillis();
        List<String> activeNodes = new ArrayList<>();

        dataNodes.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout;
            if (expired) {
                addressToDataNodeId.remove(entry.getValue().address);
            }
            return expired;
        });

        // 新格式: nodeId|host:port|freeSpace
        for (Map.Entry<String, NodeInfo> entry : dataNodes.entrySet()) {
            NodeInfo info = entry.getValue();
            activeNodes.add(info.nodeId + "|" + info.address + "|" + info.freeSpace);
        }

        String response = String.join(",", activeNodes);
        NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_DATANODES, response.getBytes(StandardCharsets.UTF_8));
    }

    private void handleGetNameNodes(ChannelHandlerContext ctx) {
        long now = System.currentTimeMillis();
        List<String> activeNodes = new ArrayList<>();

        nameNodes.entrySet().removeIf(entry -> {
            boolean expired = (now - entry.getValue().lastHeartbeatTime) > heartbeatTimeout;
            if (expired) {
                addressToNameNodeId.remove(entry.getValue().address);
            }
            return expired;
        });

        // 新格式: nodeId|host:port
        for (Map.Entry<String, NodeInfo> entry : nameNodes.entrySet()) {
            NodeInfo info = entry.getValue();
            activeNodes.add(info.nodeId + "|" + info.address);
        }

        String response = String.join(",", activeNodes);
        NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_NAMENODES, response.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("RegistryHandler异常", cause);
        ctx.close();
    }

    /**
     * 关闭 RegistryHandler 的内部资源（定时清理任务）
     */
    public static void shutdown() {
        if (cleanerExecutor != null && !cleanerExecutor.isShutdown()) {
            cleanerExecutor.shutdownNow();
            LOG.info("Registry-Cleaner 已关闭");
        }
    }
}
