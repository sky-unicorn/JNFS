package org.jnfs.registry;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jnfs.common.SecurityConfig;
import org.jnfs.registry.auth.AuthFilter;
import org.jnfs.registry.auth.AuthManager;
import org.jnfs.registry.auth.ChangePasswordHandler;
import org.jnfs.registry.auth.LoginHandler;
import org.jnfs.registry.auth.LogoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 仪表盘服务 (HTTP Server)
 * 提供系统状态的 Web 界面和 JSON API
 * <p>
 * 可选启用登录鉴权：当传入非 null 的 {@link AuthManager} 时，
 * 受保护路由（/、/api/nodes、/api/security、/api/change-password）挂载 AuthFilter，
 * 未登录访问将 302 跳转 /login；传入 null 则保留旧的无鉴权行为。
 */
public class DashboardServer {

    private static final Logger LOG = LoggerFactory.getLogger(DashboardServer.class);

    private final int port;
    /** 鉴权管理器，null 表示关闭鉴权（保留旧行为） */
    private final AuthManager authManager;
    /** 元数据库 DataSource（决策 9：Registry 连元数据库读写冗余组/策略/任务表） */
    private final javax.sql.DataSource metadataDataSource;
    /** 顶层 storage.mode（file | h2 | mysql），用于监控页展示与空态文案；null 视为 file-like */
    private final String storageMode;

    private HttpServer server;

    public DashboardServer(int port) {
        this(port, null, null, null);
    }

    public DashboardServer(int port, AuthManager authManager) {
        this(port, authManager, null, null);
    }

    public DashboardServer(int port, AuthManager authManager, javax.sql.DataSource metadataDataSource) {
        this(port, authManager, metadataDataSource, null);
    }

    public DashboardServer(int port, AuthManager authManager, javax.sql.DataSource metadataDataSource,
                           String storageMode) {
        this.port = port;
        this.authManager = authManager;
        this.metadataDataSource = metadataDataSource;
        this.storageMode = storageMode;
    }

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            AuthFilter authFilter = (authManager != null) ? new AuthFilter(authManager) : null;

            // SPA 静态资源 handler（classpath static/ 读取构建产物，注入 __JNFS_CONFIG__）
            StaticFileHandler staticHandler = new StaticFileHandler(storageMode, authManager != null);

            // 公开静态资源（SPA 的 js/css 在登录前就要加载，不能挂 filter）
            server.createContext("/assets", staticHandler);

            if (authManager != null) {
                // 公开路由（不加 filter）
                // /login：GET 由 LoginHandler → StaticFileHandler 提供 SPA index.html；POST 交 LoginHandler 表单处理
                server.createContext("/login", new LoginHandler(authManager, staticHandler));
                server.createContext("/logout", new LogoutHandler(authManager));
            }

            // 受保护路由（鉴权启用时挂 filter）
            // / 受保护，服务 SPA index.html（AuthFilter 未登录 302 → /login）
            addProtected("/", exchange -> staticHandler.serveIndex(exchange), authFilter);
            addProtected("/api/nodes", exchange -> {
                String json = getNodesJson();
                byte[] response = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length);
                try (java.io.OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }, authFilter);
            addProtected("/api/security", exchange -> {
                String json = getSecurityJson();
                byte[] response = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length);
                try (java.io.OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            }, authFilter);
            addProtected("/api/change-password", new ChangePasswordHandler(authManager), authFilter);

            // 冗余存储管理 API（§10.1，12 端点，决策 9：Registry 连元数据库）
            if (metadataDataSource != null) {
                org.jnfs.registry.api.ReplicationApiHandler replicationHandler =
                        new org.jnfs.registry.api.ReplicationApiHandler(metadataDataSource);
                addProtected("/api/replication/groups", replicationHandler, authFilter);
                addProtected("/api/replication/groups/", replicationHandler, authFilter);
                addProtected("/api/nodes/", replicationHandler, authFilter);
                addProtected("/api/replication/policy", replicationHandler, authFilter);
                addProtected("/api/replication/sync", replicationHandler, authFilter);
                addProtected("/api/replication/sync/", replicationHandler, authFilter);
                addProtected("/api/replication/alerts", replicationHandler, authFilter);
                LOG.info("Dashboard: 冗余存储管理 API 已注册（12 端点）");

                // 文件管理 API（/api/files 分页查询 + /api/files/types 类型下拉；与元数据库同库直查）
                org.jnfs.registry.api.FilesApiHandler filesHandler =
                        new org.jnfs.registry.api.FilesApiHandler(metadataDataSource);
                addProtected("/api/files", filesHandler, authFilter);
                addProtected("/api/files/", filesHandler, authFilter);
                LOG.info("Dashboard: 文件管理 API 已注册（/api/files, /api/files/types）");
            } else {
                // S6: metadataDataSource==null 时冗余存储 API 统一返回 JSON 503，而非落到 "/" 返回 HTML
                // （前端 fetch 期望 JSON，HTML 会让 res.json() 抛错且无法区分"未配置"）。
                // 注意：h2 模式 DataSource 非 null（RegistryServer 为 h2 建 H2 DataSource 作节点持久化
                // 并传入 Dashboard），故 h2 不会走到本分支——仅 file/未配置 DataSource 场景触发。
                String disabledReason = "metadata API disabled (no metadata datasource configured)";
                HttpHandler disabled = exchange -> {
                    org.jnfs.registry.api.JsonHttpUtils.sendError(
                            exchange, 503, disabledReason);
                };
                addProtected("/api/replication/groups", disabled, authFilter);
                addProtected("/api/replication/groups/", disabled, authFilter);
                addProtected("/api/nodes/", disabled, authFilter);
                addProtected("/api/replication/policy", disabled, authFilter);
                addProtected("/api/replication/sync", disabled, authFilter);
                addProtected("/api/replication/sync/", disabled, authFilter);
                addProtected("/api/replication/alerts", disabled, authFilter);
                addProtected("/api/files", disabled, authFilter);
                addProtected("/api/files/", disabled, authFilter);
                LOG.info("Dashboard: 元数据库 DataSource 未配置，冗余存储/文件管理 API 返回 503（{} 模式）",
                        "h2".equalsIgnoreCase(storageMode) ? "h2" : "file");
            }

            server.setExecutor(null);
            server.start();
            LOG.info("JNFS Dashboard 启动成功，访问地址: http://localhost:{}{}",
                    port, authManager != null ? "（已启用登录鉴权）" : "（鉴权已禁用）");
        } catch (Exception e) {
            LOG.error("Dashboard启动失败", e);
        }
    }

    /**
     * 注册受保护路由：鉴权启用时挂 AuthFilter
     */
    private void addProtected(String path, HttpHandler handler, AuthFilter filter) {
        HttpContext ctx = server.createContext(path, handler);
        if (filter != null) {
            ctx.getFilters().add(filter);
        }
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            LOG.info("Dashboard 已停止");
        }
    }

    private String getNodesJson() {
        Map<String, RegistryHandler.NodeInfo> nodes = RegistryHandler.getDataNodes();
        Map<String, long[]> drainStatusMap = loadDrainStatus(); // nodeId → [drainStatus, drainAt]
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int i = 0;
        long now = System.currentTimeMillis();
        for (Map.Entry<String, RegistryHandler.NodeInfo> entry : nodes.entrySet()) {
            if (i > 0) sb.append(",");
            RegistryHandler.NodeInfo info = entry.getValue();
            long[] drain = drainStatusMap.get(info.nodeId);
            int drainStatus = (drain != null) ? (int) drain[0] : 0;
            long drainAtMs = (drain != null) ? drain[1] : -1L;
            sb.append("{");
            sb.append("\"nodeId\":\"").append(escapeJson(info.nodeId)).append("\",");
            sb.append("\"address\":\"").append(escapeJson(info.address)).append("\",");
            sb.append("\"freeSpace\":").append(info.freeSpace).append(",");
            sb.append("\"lastHeartbeat\":").append(info.lastHeartbeatTime).append(",");
            // 服务端计算状态，避免客户端时间不一致导致误判
            // status 仍按心跳客观判定 online/offline；drain 叠加在 online 上，靠 drainStatus 驱动徽标
            boolean isOnline = (now - info.lastHeartbeatTime) < RegistryHandler.heartbeatTimeout;
            sb.append("\"status\":\"").append(isOnline ? "online" : "offline").append("\",");
            sb.append("\"drainStatus\":").append(drainStatus).append(",");
            sb.append("\"drainAt\":").append(drainAtMs >= 0 ? Long.toString(drainAtMs) : "null");
            sb.append("}");
            i++;
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * 从 node_drain 表加载排空状态（服务端权威）。
     * 返回 nodeId → [drainStatus(0/1), drainAt(毫秒,-1表示null)] 的映射。
     * file 模式（metadataDataSource==null）直接返回空 map。
     * SQLException 记 warn，不阻断渲染。
     */
    private Map<String, long[]> loadDrainStatus() {
        if (metadataDataSource == null) return Map.of();
        try (var conn = metadataDataSource.getConnection();
             var ps = conn.prepareStatement("SELECT node_id, drain_status, drain_at FROM node_drain WHERE drain_status=1");
             var rs = ps.executeQuery()) {
            Map<String, long[]> result = new java.util.HashMap<>();
            while (rs.next()) {
                String nodeId = rs.getString("node_id");
                int drainStatus = rs.getInt("drain_status");
                java.sql.Timestamp drainAtTs = rs.getTimestamp("drain_at");
                long drainAtMs = (drainAtTs != null) ? drainAtTs.getTime() : -1L;
                result.put(nodeId, new long[]{drainStatus, drainAtMs});
            }
            return result;
        } catch (java.sql.SQLException e) {
            LOG.warn("查询 node_drain 表失败，排空状态不可用", e);
            return Map.of();
        }
    }

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String getSecurityJson() {
        String currentToken = SecurityConfig.getToken();
        boolean customConfigured = !SecurityConfig.DEFAULT_TOKEN.equals(currentToken);
        return "{\"securityConfigured\":" + customConfigured + "}";
    }
}
