package org.jnfs.registry;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.NettyHandlerHelper;
import org.jnfs.common.Packet;
import org.jnfs.registry.api.dao.NodeRegistryDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
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

    // storage 配置载荷（AES 加密后的密文 byte[]），由 RegistryServer 启动时注入；
    // null 表示未配置 storage 段。NameNode 启动时经 REGISTRY_GET_STORAGE_CONFIG 拉取。
    private static volatile byte[] storageConfigPayload;

    /**
     * 节点注册持久化 DAO（nullable）。
     * <p>
     * 非 null 时（mysql / h2 模式，RegistryServer 注入共享 DataSource）启用持久化：
     * <ul>
     *   <li>启动从 node_registry 加载历史节点到内存（离线节点保留显示，状态待心跳刷新）</li>
     *   <li>register/heartbeat 同步 upsert（失败仅 warn，不阻断注册，内存照常更新）</li>
     * </ul>
     * null 时退化为纯内存模式（file 模式 / DataSource 注入失败）：重启即失，与旧行为一致。
     */
    private static volatile NodeRegistryDao nodeRegistryDao;

    // 主动清理过期节点的定时任务 (使用统一的 Daemon 线程工厂)
    private static final ScheduledExecutorService cleanerExecutor = Executors.newSingleThreadScheduledExecutor(
            new DaemonThreadFactory("Registry-Cleaner"));

    static {
        // 每 30 秒执行一次离线统计（持久化模式下不再物理删除节点，离线节点保留显示）
        cleanerExecutor.scheduleAtFixedRate(() -> {
            try {
                long now = System.currentTimeMillis();

                long dnOffline = dataNodes.values().stream()
                        .filter(info -> (now - info.lastHeartbeatTime) > heartbeatTimeout)
                        .count();
                if (dnOffline > 0) {
                    LOG.debug("[Registry-Cleaner] {} 个 DataNode 离线（保留显示，未从注册表移除）", dnOffline);
                }

                long nnOffline = nameNodes.values().stream()
                        .filter(info -> (now - info.lastHeartbeatTime) > heartbeatTimeout)
                        .count();
                if (nnOffline > 0) {
                    LOG.debug("[Registry-Cleaner] {} 个 NameNode 离线（保留显示，未从注册表移除）", nnOffline);
                }
            } catch (Exception e) {
                LOG.error("Registry清理任务异常", e);
            }
        }, 10, 30, TimeUnit.SECONDS);
    }

    /**
     * 暴露给 Dashboard 使用：返回全部节点（在线 + 离线）。
     * <p>
     * 离线判定由 Dashboard 服务端按心跳超时实时计算，不依赖 Cleaner 是否已剔除。
     * 持久化模式下离线节点不再被 Cleaner 物理删除，故重启后仍可见。
     */
    public static Map<String, NodeInfo> getDataNodes() {
        return Collections.unmodifiableMap(dataNodes);
    }

    public static Map<String, NodeInfo> getNameNodes() {
        return Collections.unmodifiableMap(nameNodes);
    }

    /**
     * 返回在线 DataNode（供服务发现 REGISTRY_GET_DATANODES 使用）。
     * <p>
     * NameNode 路由上传必须只看到在线节点，离线节点（心跳超时）不能参与选路。
     */
    public static Map<String, NodeInfo> getActiveDataNodes() {
        long now = System.currentTimeMillis();
        Map<String, NodeInfo> active = new LinkedHashMap<>();
        for (Map.Entry<String, NodeInfo> entry : dataNodes.entrySet()) {
            if ((now - entry.getValue().lastHeartbeatTime) <= heartbeatTimeout) {
                active.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(active);
    }

    /**
     * 返回在线 NameNode（供服务发现 REGISTRY_GET_NAMENODES 使用）。
     */
    public static Map<String, NodeInfo> getActiveNameNodes() {
        long now = System.currentTimeMillis();
        Map<String, NodeInfo> active = new LinkedHashMap<>();
        for (Map.Entry<String, NodeInfo> entry : nameNodes.entrySet()) {
            if ((now - entry.getValue().lastHeartbeatTime) <= heartbeatTimeout) {
                active.put(entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(active);
    }

    /**
     * 注入节点注册持久化 DAO（nullable）。
     * <p>
     * 非 null 时启用持久化：先从 DB 加载历史节点到内存（显示为离线直到心跳刷新）。
     * null 时退化为纯内存模式（兼容旧行为）。
     *
     * @param dao 节点注册 DAO；null 表示不启用持久化
     */
    public static void initNodeRegistryDao(NodeRegistryDao dao) {
        nodeRegistryDao = dao;
        if (dao != null) {
            loadPersistedNodes();
        }
    }

    /**
     * 启动期从 node_registry 加载历史节点到内存。
     * <p>
     * 加载的节点 lastHeartbeatTime 为 DB 中的历史值，通常已超时，故显示为离线，
     * 直到节点重新心跳刷新。加载失败仅 warn，退化为空内存（不阻断启动）。
     */
    private static void loadPersistedNodes() {
        if (nodeRegistryDao == null) {
            return;
        }
        try {
            List<NodeRegistryDao.NodeRecord> records = nodeRegistryDao.listAll();
            int dn = 0, nn = 0;
            for (NodeRegistryDao.NodeRecord r : records) {
                String address = r.host + ":" + r.port;
                if ("NAMENODE".equals(r.nodeType)) {
                    nameNodes.put(r.nodeId, new NodeInfo(r.nodeId, address, r.lastHeartbeatMs, 0));
                    addressToNameNodeId.put(address, r.nodeId);
                    nn++;
                } else {
                    // DATANODE（含历史脏数据兜底为 DataNode）
                    dataNodes.put(r.nodeId, new NodeInfo(r.nodeId, address, r.lastHeartbeatMs, r.freeSpace));
                    addressToDataNodeId.put(address, r.nodeId);
                    dn++;
                }
            }
            LOG.info("从 node_registry 恢复 {} 个 DataNode, {} 个 NameNode（状态待心跳刷新）", dn, nn);
        } catch (Exception e) {
            LOG.warn("加载 node_registry 失败，退化为空内存（不影响运行）", e);
        }
    }

    /**
     * 注入 storage 配置载荷（AES 加密后的密文），供 NameNode 启动时拉取。
     *
     * @param payload 密文 byte[]；调 null 表示未配置 storage
     */
    public static void setStorageConfigPayload(byte[] payload) {
        storageConfigPayload = payload;
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
            case REGISTRY_GET_STORAGE_CONFIG:
                handleGetStorageConfig(ctx);
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

        long now = System.currentTimeMillis();
        dataNodes.put(nodeId, new NodeInfo(nodeId, address, now, freeSpace));
        addressToDataNodeId.put(address, nodeId);

        // 持久化到 DB（同步写；失败仅 warn，不阻断内存注册）
        persistNode(nodeId, "DATANODE", address, freeSpace, now);

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

        long now = System.currentTimeMillis();
        nameNodes.put(nodeId, new NodeInfo(nodeId, address, now, 0));
        addressToNameNodeId.put(address, nodeId);

        // 持久化到 DB（同步写；失败仅 warn，不阻断内存注册）
        persistNode(nodeId, "NAMENODE", address, 0L, now);

        if (packet.getCommandType() == CommandType.REGISTRY_REGISTER_NAMENODE) {
            LOG.info("NameNode 注册成功: nodeId={}, address={}", nodeId, address);
            NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_REGISTER_NAMENODE, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * 持久化节点注册记录到 node_registry（DB 不可用时降级为纯内存）。
     * <p>
     * 解析 host:port 解析失败时 warn 跳过；SQL 失败仅 warn，不阻断注册（与内存模式语义一致）。
     */
    private static void persistNode(String nodeId, String nodeType, String address,
                                    long freeSpace, long lastHeartbeatMs) {
        if (nodeRegistryDao == null) {
            return;
        }
        try {
            int colonIdx = address.lastIndexOf(':');
            if (colonIdx < 0 || colonIdx == address.length() - 1) {
                LOG.warn("持久化节点失败：address 格式非法（无 host:port）: nodeId={}, address={}", nodeId, address);
                return;
            }
            String host = address.substring(0, colonIdx);
            int port;
            try {
                port = Integer.parseInt(address.substring(colonIdx + 1));
            } catch (NumberFormatException nfe) {
                LOG.warn("持久化节点失败：port 解析失败: nodeId={}, address={}", nodeId, address);
                return;
            }
            nodeRegistryDao.upsert(nodeId, nodeType, host, port, freeSpace, lastHeartbeatMs);
        } catch (Exception e) {
            LOG.warn("持久化节点注册失败（不影响内存）: nodeId={}, type={}", nodeId, nodeType, e);
        }
    }

    private void handleGetDataNodes(ChannelHandlerContext ctx) {
        // 服务发现：仅返回在线 DataNode（NameNode 路由上传必须只看到在线节点）
        List<String> activeNodes = new ArrayList<>();
        for (Map.Entry<String, NodeInfo> entry : getActiveDataNodes().entrySet()) {
            NodeInfo info = entry.getValue();
            activeNodes.add(info.nodeId + "|" + info.address + "|" + info.freeSpace);
        }

        String response = String.join(",", activeNodes);
        NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_DATANODES, response.getBytes(StandardCharsets.UTF_8));
    }

    private void handleGetStorageConfig(ChannelHandlerContext ctx) {
        byte[] payload = storageConfigPayload;
        if (payload == null) {
            NettyHandlerHelper.sendError(ctx, "Registry 未配置 storage");
            return;
        }
        NettyHandlerHelper.sendResponse(ctx, CommandType.REGISTRY_RESPONSE_STORAGE_CONFIG, payload);
    }

    private void handleGetNameNodes(ChannelHandlerContext ctx) {
        // 服务发现：仅返回在线 NameNode
        List<String> activeNodes = new ArrayList<>();
        for (Map.Entry<String, NodeInfo> entry : getActiveNameNodes().entrySet()) {
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
