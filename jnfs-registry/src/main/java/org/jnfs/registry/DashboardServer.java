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

    private HttpServer server;

    public DashboardServer(int port) {
        this(port, null, null);
    }

    public DashboardServer(int port, AuthManager authManager) {
        this(port, authManager, null);
    }

    public DashboardServer(int port, AuthManager authManager, javax.sql.DataSource metadataDataSource) {
        this.port = port;
        this.authManager = authManager;
        this.metadataDataSource = metadataDataSource;
    }

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            AuthFilter authFilter = (authManager != null) ? new AuthFilter(authManager) : null;

            if (authManager != null) {
                // 公开路由（不加 filter）
                server.createContext("/login", new LoginHandler(authManager));
                server.createContext("/logout", new LogoutHandler(authManager));
            }

            // 受保护路由（鉴权启用时挂 filter）
            addProtected("/", new DashboardHttpHandler(), authFilter);
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
            } else {
                // S6: file 模式下冗余存储 API 统一返回 JSON 503，而非落到 "/" 返回 HTML
                // （前端 fetch 期望 JSON，HTML 会让 res.json() 抛错且无法区分"未配置"）
                HttpHandler disabled = exchange -> {
                    org.jnfs.registry.api.JsonHttpUtils.sendError(
                            exchange, 503, "metadata API disabled in file mode");
                };
                addProtected("/api/replication/groups", disabled, authFilter);
                addProtected("/api/replication/groups/", disabled, authFilter);
                addProtected("/api/nodes/", disabled, authFilter);
                addProtected("/api/replication/policy", disabled, authFilter);
                addProtected("/api/replication/sync", disabled, authFilter);
                addProtected("/api/replication/sync/", disabled, authFilter);
                addProtected("/api/replication/alerts", disabled, authFilter);
                LOG.info("Dashboard: 元数据库 DataSource 未配置，冗余存储 API 返回 503（file 模式）");
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

    class DashboardHttpHandler implements HttpHandler {
        @Override
        public void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
            String html = buildHtml();
            byte[] response = html.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length);
            try (java.io.OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }

        private String buildHtml() {
            // 鉴权是否启用（由外部类 authManager 决定）
            boolean authEnabled = authManager != null;
            // 使用 Java 文本块（Java 17）内联 HTML/CSS/JS，避免 \" / \n 转义地狱。
            // 文本块内只需转义字面的 \"\"\"（本页无）；JS/CSS 引号原样书写。
            String html = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JNFS 运维监控中心</title>
    <style>
        :root {
            --primary-color: #3498db;
            --bg-color: #f4f7f6;
            --card-bg: #ffffff;
            --text-color: #333;
            --success-color: #2e7d32;  --success-bg: #e8f5e9;
            --warning-color: #e67e22;  --warning-bg: #fff3e0;
            --danger-color:  #c62828;  --danger-bg:  #ffebee;
            --info-color:    #0277bd;  --info-bg:    #e1f5fe;
        }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            margin: 0;
            padding: 0;
        }
        .header {
            background-color: var(--primary-color);
            color: white;
            padding: 1rem 2rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .header h1 { margin: 0; font-size: 1.5rem; }
        .header-actions { display: flex; gap: 0.75rem; align-items: center; }
        .header-actions button, .header-actions a {
            background: rgba(255,255,255,0.2);
            color: white;
            border: 1px solid rgba(255,255,255,0.4);
            padding: 0.4rem 0.9rem;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85rem;
            text-decoration: none;
            transition: background 0.2s;
        }
        .header-actions button:hover, .header-actions a:hover { background: rgba(255,255,255,0.35); }
        .container { max-width: 1200px; margin: 2rem auto; padding: 0 1rem; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        .card {
            background-color: var(--card-bg);
            border-radius: 8px;
            padding: 1.5rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            transition: transform 0.2s;
        }
        .card:hover { transform: translateY(-2px); }
        .card h3 {
            margin: 0 0 0.5rem 0;
            color: #7f8c8d;
            font-size: 0.9rem;
            text-transform: uppercase;
        }
        .card .value { font-size: 2rem; font-weight: bold; color: #2c3e50; }
        .table-container {
            background-color: var(--card-bg);
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            overflow: hidden;
            margin-bottom: 1.5rem;
        }
        table { width: 100%; border-collapse: collapse; }
        th, td {
            padding: 0.85rem 1rem;
            text-align: left;
            border-bottom: 1px solid #eee;
            font-size: 0.92rem;
        }
        th { background-color: #f8f9fa; font-weight: 600; color: #2c3e50; }
        tr:last-child td { border-bottom: none; }
        .status-badge {
            padding: 0.25rem 0.75rem;
            border-radius: 50px;
            font-size: 0.8rem;
            font-weight: 500;
            white-space: nowrap;
        }
        .status-online { background-color: var(--success-bg); color: var(--success-color); }
        .status-offline { background-color: var(--danger-bg); color: var(--danger-color); }
        .status-draining { background-color: var(--warning-bg); color: var(--warning-color); }
        .status-syncing { background-color: var(--info-bg); color: var(--info-color); }
        .refresh-info { text-align: right; color: #95a5a6; font-size: 0.8rem; margin-top: 0.5rem; }

        /* —— Tab 导航（§16.1） —— */
        .tab-nav { display: flex; gap: 0.5rem; margin-bottom: 1.5rem; border-bottom: 2px solid #e0e0e0; }
        .tab-nav .tab {
            padding: 0.7rem 1.4rem;
            cursor: pointer;
            border: none;
            background: none;
            font-size: 0.98rem;
            color: #7f8c8d;
            border-bottom: 3px solid transparent;
            margin-bottom: -2px;
            transition: color 0.2s;
        }
        .tab-nav .tab:hover { color: var(--primary-color); }
        .tab-nav .tab.active { color: var(--primary-color); border-bottom-color: var(--primary-color); font-weight: 600; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .sub-tab-nav { display: flex; gap: 0.4rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
        .sub-tab-nav .tab {
            padding: 0.5rem 1.1rem;
            cursor: pointer;
            border: 1px solid #d5dbdb;
            background: #fff;
            border-radius: 50px;
            font-size: 0.88rem;
            color: #566573;
            transition: all 0.2s;
        }
        .sub-tab-nav .tab:hover { border-color: var(--primary-color); color: var(--primary-color); }
        .sub-tab-nav .tab.active { background: var(--primary-color); color: #fff; border-color: var(--primary-color); }
        .alert-dot {
            display: inline-block;
            min-width: 18px;
            height: 18px;
            line-height: 18px;
            padding: 0 5px;
            margin-left: 6px;
            background: var(--danger-color);
            color: #fff;
            border-radius: 10px;
            font-size: 0.72rem;
            font-weight: 600;
            text-align: center;
            vertical-align: middle;
        }
        .alert-dot.zero { display: none; }

        /* —— 子页头部条 —— */
        .section-bar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; }
        .section-bar h2 { margin: 0; font-size: 1.1rem; color: #2c3e50; }

        /* —— 操作按钮（§16.8） —— */
        .action-btn {
            padding: 0.3rem 0.7rem;
            margin-right: 0.3rem;
            border: 1px solid #d5dbdb;
            background: #fff;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.82rem;
            color: #2c3e50;
            transition: all 0.15s;
        }
        .action-btn:hover { border-color: var(--primary-color); color: var(--primary-color); }
        .action-btn:disabled { color: #bdc3c7; cursor: not-allowed; border-color: #ecf0f1; background: #fafbfb; }
        .action-btn.danger:hover { border-color: var(--danger-color); color: var(--danger-color); }
        .action-btn.success { border-color: var(--success-color); color: var(--success-color); }
        .action-btn.primary { border-color: var(--primary-color); color: var(--primary-color); }
        .primary-btn {
            padding: 0.5rem 1.1rem;
            background: var(--primary-color);
            color: #fff;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.9rem;
        }
        .primary-btn:hover { background: #2980b9; }
        .primary-btn:disabled { background: #bdc3c7; cursor: not-allowed; }

        /* —— 进度条（§16.4/16.8） —— */
        .progress-bar {
            width: 100%;
            height: 22px;
            background: #ecf0f1;
            border-radius: 11px;
            overflow: hidden;
            margin: 0.5rem 0;
        }
        .progress-bar .fill {
            height: 100%;
            background: linear-gradient(90deg, var(--primary-color), #2ecc71);
            transition: width 0.4s ease;
            border-radius: 11px;
        }
        .progress-label { color: #566573; font-size: 0.85rem; }

        /* —— 节点徽标（组成员） —— */
        .node-chip {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            margin: 0.15rem;
            background: var(--info-bg);
            color: var(--info-color);
            border-radius: 4px;
            font-size: 0.82rem;
        }
        .same-host-warn { background: var(--warning-bg); color: var(--warning-color); }

        /* —— 告警级别徽标 —— */
        .alert-level-critical { background: var(--danger-bg); color: var(--danger-color); }
        .alert-level-warning { background: var(--warning-bg); color: var(--warning-color); }

        /* —— 同步策略表单 —— */
        .policy-form { max-width: 640px; }
        .form-section { margin-bottom: 1.5rem; }
        .form-section h3 { font-size: 0.95rem; color: #2c3e50; margin: 0 0 0.8rem 0; padding-bottom: 0.4rem; border-bottom: 1px solid #eee; }
        .form-row { display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.8rem; flex-wrap: wrap; }
        .form-row label { width: 130px; color: #566573; font-size: 0.9rem; }
        .form-row select, .form-row input {
            padding: 0.45rem 0.6rem;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 0.92rem;
            width: 90px;
            box-sizing: border-box;
        }
        .form-row .hint { color: #95a5a6; font-size: 0.8rem; }

        /* —— 校验提示 —— */
        .validation-msg { font-size: 0.85rem; margin: 0.5rem 0; }
        .validation-msg.error { color: var(--danger-color); }
        .validation-msg.warning { color: var(--warning-color); }

        /* —— 弹窗 —— */
        .modal-overlay {
            display: none;
            position: fixed; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.5);
            justify-content: center; align-items: center;
            z-index: 100;
        }
        .modal-overlay.show { display: flex; }
        .modal {
            background: #fff; padding: 1.5rem; border-radius: 8px;
            width: 100%; max-width: 380px;
            box-shadow: 0 8px 30px rgba(0,0,0,0.2);
        }
        .modal.modal-wide { max-width: 520px; }
        .modal h2 { margin: 0 0 1rem 0; font-size: 1.1rem; color: #2c3e50; }
        .modal p { color: #566573; font-size: 0.92rem; line-height: 1.5; margin: 0 0 1rem 0; }
        .modal input[type="text"], .modal input[type="password"] {
            width: 100%; padding: 0.6rem; margin-bottom: 0.75rem;
            border: 1px solid #ddd; border-radius: 4px; font-size: 0.95rem;
            box-sizing: border-box;
        }
        .modal .node-pick {
            max-height: 220px; overflow-y: auto;
            border: 1px solid #eee; border-radius: 4px; padding: 0.4rem;
            margin-bottom: 0.5rem;
        }
        .modal .node-pick label {
            display: flex; align-items: center; gap: 0.5rem;
            padding: 0.35rem; font-size: 0.9rem; cursor: pointer;
        }
        .modal .node-pick label.disabled { color: #bdc3c7; cursor: not-allowed; }
        .modal-actions { display: flex; gap: 0.5rem; justify-content: flex-end; margin-top: 0.5rem; }
        .modal-actions button { padding: 0.5rem 1rem; border-radius: 4px; cursor: pointer; border: none; font-size: 0.9rem; }
        .btn-cancel { background: #ecf0f1; color: #555; }
        .btn-confirm { background: #3498db; color: #fff; }
        .btn-confirm:disabled { background: #bdc3c7; cursor: not-allowed; }
        .btn-danger { background: var(--danger-color); color: #fff; }

        /* —— Toast —— */
        .toast-container {
            position: fixed; top: 1.2rem; right: 1.2rem; z-index: 200;
            display: flex; flex-direction: column; gap: 0.5rem;
        }
        .toast {
            padding: 0.7rem 1.1rem; border-radius: 6px; color: #fff;
            font-size: 0.9rem; box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            min-width: 220px; opacity: 0; transform: translateX(20px);
            transition: opacity 0.3s, transform 0.3s;
        }
        .toast.show { opacity: 1; transform: translateX(0); }
        .toast.success { background: var(--success-color); }
        .toast.error { background: var(--danger-color); }
        .toast.warning { background: var(--warning-color); }
        .toast.info { background: var(--info-color); }
        .empty-row td { text-align: center; color: #95a5a6; padding: 1.5rem; }
    </style>
</head>
<body>
    <div class="header">
        <h1>JNFS 运维监控中心</h1>
""" + (authEnabled ? """
        <div class="header-actions">
            <button onclick="openChangePasswordModal()">修改密码</button>
            <a href="/logout">登出</a>
        </div>
""" : "") + """
    </div>
    <div class="container">
        <!-- 顶部 Tab（§16.1） -->
        <div class="tab-nav" data-tab-group="top">
            <button class="tab active" data-tab="tab-nodes" onclick="switchTopTab('tab-nodes')">节点监控</button>
            <button class="tab" data-tab="tab-redundancy" onclick="switchTopTab('tab-redundancy')">冗余存储管理</button>
        </div>

        <!-- ========== 节点监控 Tab（§16.2） ========== -->
        <div class="tab-content active" id="tab-nodes" data-tab-group="top">
            <div class="stats-grid">
                <div class="card">
                    <h3>活跃存储节点</h3>
                    <div class="value" id="activeNodes">-</div>
                </div>
                <div class="card">
                    <h3>全网剩余容量</h3>
                    <div class="value" id="totalFreeSpace">-</div>
                </div>
                <div class="card">
                    <h3>安全状态</h3>
                    <div class="value" id="securityStatus"><span style="color:#e67e22">加载中...</span></div>
                </div>
            </div>
            <div class="table-container">
                <table id="nodeTable">
                    <thead>
                        <tr>
                            <th>节点ID</th>
                            <th>节点地址</th>
                            <th>剩余空间</th>
                            <th>最后心跳时间</th>
                            <th>状态</th>
                            <th>操作</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr><td colspan="6" class="empty-row">加载数据中...</td></tr>
                    </tbody>
                </table>
            </div>
            <div class="refresh-info">数据每 2 秒自动刷新</div>
        </div>

        <!-- ========== 冗余存储管理 Tab（§16.3-16.6） ========== -->
        <div class="tab-content" id="tab-redundancy" data-tab-group="top">
            <div class="sub-tab-nav" data-tab-group="redundancy">
                <button class="tab active" data-tab="tab-groups" onclick="switchSubTab('tab-groups')">冗余组管理</button>
                <button class="tab" data-tab="tab-sync" onclick="switchSubTab('tab-sync')">对账同步</button>
                <button class="tab" data-tab="tab-policy" onclick="switchSubTab('tab-policy')">同步策略</button>
                <button class="tab" data-tab="tab-alerts" onclick="switchSubTab('tab-alerts')">告警<span id="alertDot" class="alert-dot zero">0</span></button>
            </div>

            <!-- 冗余组管理（§16.3） -->
            <div class="tab-content active" id="tab-groups" data-tab-group="redundancy">
                <div class="section-bar">
                    <h2>冗余组管理</h2>
                    <button class="primary-btn" onclick="openGroupModal(null)">+ 创建冗余组</button>
                </div>
                <div class="table-container">
                    <table id="groupTable">
                        <thead>
                            <tr>
                                <th>组ID</th>
                                <th>节点成员</th>
                                <th>状态</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr><td colspan="4" class="empty-row">加载数据中...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- 对账同步（§16.4） -->
            <div class="tab-content" id="tab-sync" data-tab-group="redundancy">
                <div class="section-bar">
                    <h2>对账同步</h2>
                    <button class="primary-btn" onclick="triggerSync()">手动触发全量对账</button>
                </div>
                <div class="stats-grid">
                    <div class="card"><h3>待同步任务</h3><div class="value" id="syncPending">-</div></div>
                    <div class="card"><h3>已完成</h3><div class="value" id="syncDone" style="color:var(--success-color)">-</div></div>
                    <div class="card"><h3>同步失败</h3><div class="value" id="syncFailed" style="color:var(--danger-color)">-</div></div>
                    <div class="card"><h3>当前执行中</h3><div class="value" id="syncCurrent" style="color:var(--info-color)">-</div></div>
                </div>
                <div class="card">
                    <h3>同步进度</h3>
                    <div class="progress-bar"><div class="fill" id="syncProgressFill" style="width:0%"></div></div>
                    <div class="progress-label" id="syncProgressLabel">0%</div>
                </div>
                <div class="table-container">
                    <table id="failedJobsTable">
                        <thead>
                            <tr>
                                <th>任务ID</th>
                                <th>文件哈希</th>
                                <th>源节点</th>
                                <th>目标节点</th>
                                <th>失败次数</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr><td colspan="6" class="empty-row">暂无失败任务</td></tr>
                        </tbody>
                    </table>
                </div>
                <div class="refresh-info">数据每 5 秒自动刷新</div>
            </div>

            <!-- 同步策略（§16.5） -->
            <div class="tab-content" id="tab-policy" data-tab-group="redundancy">
                <div class="section-bar">
                    <h2>同步策略配置</h2>
                    <button class="primary-btn" onclick="savePolicy()">保存配置</button>
                </div>
                <div class="card policy-form">
                    <div class="form-section">
                        <h3>核心同步窗口</h3>
                        <div class="form-row">
                            <label>开始时间</label>
                            <select id="winStartH"></select><span>:</span><select id="winStartM"></select>
                        </div>
                        <div class="form-row">
                            <label>结束时间</label>
                            <select id="winEndH"></select><span>:</span><select id="winEndM"></select>
                        </div>
                        <div class="form-row">
                            <label>软截止时间</label>
                            <select id="softDeadlineH"></select><span>:</span><select id="softDeadlineM"></select>
                            <span class="hint">超出软截止仍执行，但标记告警</span>
                        </div>
                    </div>
                    <div class="form-section">
                        <h3>传输限制</h3>
                        <div class="form-row">
                            <label>限速 (MB/s)</label>
                            <input type="text" id="rateLimit" value="50">
                            <span class="hint">0 = 不限速</span>
                        </div>
                        <div class="form-row">
                            <label>最大并发数</label>
                            <input type="text" id="maxConcurrency" value="4">
                            <span class="hint">建议 1~10</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 告警（§16.6） -->
            <div class="tab-content" id="tab-alerts" data-tab-group="redundancy">
                <div class="stats-grid">
                    <div class="card"><h3>活跃告警</h3><div class="value" id="alertActiveCount" style="color:var(--danger-color)">-</div></div>
                    <div class="card"><h3>已恢复告警</h3><div class="value" id="alertResolvedCount" style="color:var(--success-color)">-</div></div>
                </div>
                <div class="table-container">
                    <table id="alertTable">
                        <thead>
                            <tr>
                                <th>级别</th>
                                <th>内容</th>
                                <th>相关节点</th>
                                <th>触发时间</th>
                                <th>恢复时间</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr><td colspan="5" class="empty-row">加载数据中...</td></tr>
                        </tbody>
                    </table>
                </div>
                <div class="refresh-info">数据每 5 秒自动刷新</div>
            </div>
        </div>
    </div>

""" + (authEnabled ? """
    <!-- 修改密码弹窗 -->
    <div class="modal-overlay" id="changePwdModal">
        <div class="modal">
            <h2>修改密码</h2>
            <input type="password" id="oldPassword" placeholder="旧密码" autocomplete="current-password">
            <input type="password" id="newPassword" placeholder="新密码（至少 4 位）" autocomplete="new-password">
            <div class="modal-actions">
                <button class="btn-cancel" onclick="closeChangePasswordModal()">取消</button>
                <button class="btn-confirm" onclick="submitChangePassword()">确认修改</button>
            </div>
        </div>
    </div>
""" : "") + """
    <!-- 通用确认弹窗 -->
    <div class="modal-overlay" id="confirmModal">
        <div class="modal">
            <h2 id="confirmTitle">确认操作</h2>
            <p id="confirmMessage"></p>
            <div class="modal-actions">
                <button class="btn-cancel" onclick="closeConfirm()">取消</button>
                <button class="btn-confirm" id="confirmOkBtn">确定</button>
            </div>
        </div>
    </div>

    <!-- 冗余组创建/编辑弹窗（§16.3） -->
    <div class="modal-overlay" id="groupModal">
        <div class="modal modal-wide">
            <h2 id="groupModalTitle">创建冗余组</h2>
            <input type="text" id="groupNewId" placeholder="组ID（编辑时不可改）">
            <div style="font-size:0.85rem;color:#566573;margin-bottom:0.4rem;">勾选节点（2~3 个，离线节点不可选）：</div>
            <div class="node-pick" id="groupNodePick"></div>
            <div class="validation-msg" id="groupValidation"></div>
            <div class="modal-actions">
                <button class="btn-cancel" onclick="closeGroupModal()">取消</button>
                <button class="btn-confirm" id="groupSubmitBtn" onclick="submitGroup()">确认</button>
            </div>
        </div>
    </div>

    <div class="toast-container" id="toastContainer"></div>

    <script>
    /* ===================== 全局状态 ===================== */
    var allNodes = [];          // 缓存 /api/nodes 最新结果
    var allGroups = [];         // 缓存 /api/replication/groups 最新结果
    var policyLoaded = false;   // 同步策略仅加载一次
    var currentTopTab = 'tab-nodes';
    var currentSubTab = 'tab-groups';
    var editingGroupId = null;  // 冗余组编辑态
    var confirmCallback = null; // 确认弹窗回调

    /* ===================== 工具函数 ===================== */
    function formatBytes(bytes, decimals) {
        decimals = (decimals === undefined) ? 2 : decimals;
        if (bytes === 0) return '0 Bytes';
        var k = 1024;
        var dm = decimals < 0 ? 0 : decimals;
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
        var i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
    }
    function escapeHtml(s) {
        if (s === null || s === undefined) return '';
        return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
            .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    }
    function hostOf(address) { return (address || '').split(':')[0]; }
    /** 判断节点是否属于某个冗余组（R1：非冗余组节点不显示排空按钮） */
    function nodeInAnyGroup(nodeId) {
        for (var i = 0; i < allGroups.length; i++) {
            if ((allGroups[i].nodeIds || []).indexOf(nodeId) >= 0) return true;
        }
        return false;
    }

    /* —— Toast —— */
    function showToast(msg, type) {
        type = type || 'info';
        var c = document.getElementById('toastContainer');
        var t = document.createElement('div');
        t.className = 'toast ' + type;
        t.textContent = msg;
        c.appendChild(t);
        setTimeout(function(){ t.classList.add('show'); }, 10);
        setTimeout(function(){
            t.classList.remove('show');
            setTimeout(function(){ c.removeChild(t); }, 300);
        }, 3000);
    }

    /* —— 确认弹窗 —— */
    function showConfirm(title, message, onConfirm, okClass) {
        document.getElementById('confirmTitle').textContent = title;
        document.getElementById('confirmMessage').innerHTML = message;
        var btn = document.getElementById('confirmOkBtn');
        btn.className = 'btn-confirm ' + (okClass || '');
        confirmCallback = onConfirm;
        document.getElementById('confirmModal').classList.add('show');
    }
    function closeConfirm() {
        document.getElementById('confirmModal').classList.remove('show');
        confirmCallback = null;
    }
    document.getElementById('confirmOkBtn').onclick = function() {
        var cb = confirmCallback;
        closeConfirm();
        if (cb) cb();
    };

    /* —— fetch 封装：统一处理重定向、错误 envelope —— */
    function apiFetch(url, options) {
        options = options || {};
        if (!options.headers) options.headers = {};
        options.credentials = 'same-origin'; // 携带鉴权 cookie（AuthFilter）
        return fetch(url, options).then(function(res) {
            if (res.redirected) { location.href = res.url; return null; }
            var ct = res.headers.get('Content-Type') || '';
            if (ct.indexOf('application/json') === -1) {
                return res.text().then(function(tx){
                    throw new Error('非 JSON 响应（HTTP ' + res.status + '）');
                });
            }
            return res.json().then(function(data) {
                if (!res.ok || data.success === false) {
                    var errs = (data.errors && data.errors.length) ? data.errors.join('; ')
                             : (data.error || ('HTTP ' + res.status));
                    throw new Error(errs);
                }
                return data;
            });
        });
    }
    function apiGet(u) { return apiFetch(u); }
    function apiPost(u, body) {
        return apiFetch(u, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body || {}) });
    }
    function apiPut(u, body) {
        return apiFetch(u, { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body || {}) });
    }
    function apiDelete(u) { return apiFetch(u, { method: 'DELETE' }); }

    /* ===================== Tab 切换（§16.9） ===================== */
    function switchTab(tabGroup, tabId) {
        document.querySelectorAll('[data-tab-group="' + tabGroup + '"] .tab-content')
            .forEach(function(el){ el.classList.remove('active'); });
        document.querySelectorAll('[data-tab-group="' + tabGroup + '"] .tab')
            .forEach(function(el){ el.classList.remove('active'); });
        var content = document.getElementById(tabId);
        if (content) content.classList.add('active');
        var tab = document.querySelector('[data-tab="' + tabId + '"]');
        if (tab) tab.classList.add('active');
    }
    function switchTopTab(tabId) {
        currentTopTab = tabId;
        switchTab('top', tabId);
        restartPolling();
        if (tabId === 'tab-redundancy') {
            // 进入冗余管理时，按当前子 Tab 触发一次加载
            switchSubTab(currentSubTab);
        }
    }
    function switchSubTab(tabId) {
        currentSubTab = tabId;
        switchTab('redundancy', tabId);
        restartPolling();
        // 切到子页时立即拉取一次
        if (tabId === 'tab-groups') loadGroups();
        else if (tabId === 'tab-sync') loadSync();
        else if (tabId === 'tab-policy') loadPolicy();
        else if (tabId === 'tab-alerts') loadAlerts();
    }

    /* ===================== 轮询管理 ===================== */
    var pollingTimers = [];
    function clearPolling() {
        pollingTimers.forEach(function(t){ clearInterval(t); });
        pollingTimers = [];
    }
    function addPoller(fn, interval) {
        fn();
        pollingTimers.push(setInterval(fn, interval));
    }
    function restartPolling() {
        clearPolling();
        if (currentTopTab === 'tab-nodes') {
            addPoller(loadNodes, 2000);
        } else if (currentTopTab === 'tab-redundancy') {
            if (currentSubTab === 'tab-groups') addPoller(loadGroups, 5000);
            else if (currentSubTab === 'tab-sync') addPoller(loadSync, 5000);
            else if (currentSubTab === 'tab-policy') { if (!policyLoaded) loadPolicy(); }
            else if (currentSubTab === 'tab-alerts') addPoller(loadAlerts, 5000);
        }
    }

    /* ===================== 节点监控（§16.2） ===================== */
    function loadNodes() {
        // R1：若 allGroups 未加载，先拉取冗余组再渲染（避免竞态）
        var groupsReady = allGroups.length > 0;
        var groupsPromise = groupsReady ? Promise.resolve() : apiGet('/api/replication/groups').then(function(d) {
            if (d) allGroups = d.groups || [];
        }).catch(function(){});

        apiGet('/api/nodes').then(function(data) {
            if (!data) return;
            allNodes = data;
            // 等冗余组数据就绪后再渲染（若已就绪则 groupsPromise 立即 resolve）
            groupsPromise.then(function() { renderNodes(data); });
        }).catch(function(err) {
            document.querySelector('#nodeTable tbody').innerHTML =
                '<tr><td colspan="6" class="empty-row" style="color:red">无法连接到服务器</td></tr>';
        });
    }
    function renderNodes(data) {
        var tbody = document.querySelector('#nodeTable tbody');
        var activeCount = 0, totalSpace = 0;
        if (data.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-row">暂无节点连接</td></tr>';
        } else {
            var html = '';
            data.forEach(function(node) {
                var isOnline = node.status === 'online';
                var isDraining = node.drainStatus === 1;
                // activeCount/totalSpace：online 且 drainStatus!=1 才计入（drain 节点不算"活跃存储节点"）
                if (isOnline && !isDraining) { activeCount++; totalSpace += node.freeSpace; }
                // 徽标：online+drainStatus=1 → 排空中(橙)；online → 在线(绿)；否则离线(灰)
                var badge;
                if (isOnline && isDraining) {
                    badge = '<span class="status-badge status-draining">排空中</span>';
                } else if (isOnline) {
                    badge = '<span class="status-badge status-online">在线</span>';
                } else {
                    badge = '<span class="status-badge status-offline">离线</span>';
                }
                // R1：非冗余组节点隐藏排空按钮；offline 节点禁用排空按钮
                var inGroup = nodeInAnyGroup(node.nodeId);
                var drainBtn = '';
                if (!inGroup) {
                    // 非冗余组节点：不渲染排空按钮（设计 §4 R1）
                    drainBtn = '';
                } else if (isDraining) {
                    // DRAINING 节点：显示"恢复"按钮
                    drainBtn = '<button class="action-btn success" onclick="recoverNode(\\'' + escapeHtml(node.nodeId) + '\\')">恢复</button>';
                } else if (isOnline) {
                    // ACTIVE+online+在组内：显示"排空"按钮
                    drainBtn = '<button class="action-btn" onclick="drainNode(\\'' + escapeHtml(node.nodeId) + '\\')">排空</button>';
                } else {
                    // offline 节点：禁用排空按钮
                    drainBtn = '<button class="action-btn" disabled>排空</button>';
                }
                html += '<tr>'
                    + '<td>' + escapeHtml(node.nodeId || '-') + '</td>'
                    + '<td>' + escapeHtml(node.address || '-') + '</td>'
                    + '<td>' + formatBytes(node.freeSpace) + '</td>'
                    + '<td>' + new Date(node.lastHeartbeat).toLocaleString() + '</td>'
                    + '<td>' + badge + '</td>'
                    + '<td>'
                    +   drainBtn
                    +   (inGroup ? '<button class="action-btn success" onclick="promoteNode(\\'' + escapeHtml(node.nodeId) + '\\')">晋升</button>' : '')
                    + '</td>'
                    + '</tr>';
            });
            tbody.innerHTML = html;
        }
        document.getElementById('activeNodes').innerText = activeCount;
        document.getElementById('totalFreeSpace').innerText = formatBytes(totalSpace);
    }
    function loadSecurity() {
        apiGet('/api/security').then(function(data) {
            if (!data) return;
            var el = document.getElementById('securityStatus');
            el.innerHTML = data.securityConfigured
                ? '<span style="color:#2e7d32">已配置 (自定义令牌)</span>'
                : '<span style="color:#e67e22">⚠ 使用默认令牌</span>';
        }).catch(function(){});
    }

    /* —— 排空节点（§4 R6 文案 / §13 Q1：仅标记，不迁角色） —— */
    function drainNode(nodeId) {
        var msg = '将节点 <b>' + escapeHtml(nodeId) + '</b> 标记为排空？<br>'
            + '· 后续上传不再选中该节点（新写入选路排除）<br>'
            + '· 已有数据可继续读（读可用性不受影响）<br>'
            + '· 物理数据由同步任务后台搬运，本操作不删除数据';
        showConfirm('确认排空节点', msg,
            function() {
                apiPost('/api/nodes/' + encodeURIComponent(nodeId) + '/drain', { drain: true })
                    .then(function(res) {
                        showToast(res.message || '已标记排空', 'success');
                        loadNodes();
                    })
                    .catch(function(err){ showToast('排空失败: ' + err.message, 'error'); });
            });
    }
    /* —— 恢复节点（POST drain:false，清除 DRAINING 状态） —— */
    function recoverNode(nodeId) {
        showConfirm('确认恢复节点', '取消节点 <b>' + escapeHtml(nodeId) + '</b> 的排空状态？<br>该节点将重新进入新上传的选路候选。',
            function() {
                apiPost('/api/nodes/' + encodeURIComponent(nodeId) + '/drain', { drain: false })
                    .then(function(res) {
                        showToast(res.message || '已恢复', 'success');
                        loadNodes();
                    })
                    .catch(function(err){ showToast('恢复失败: ' + err.message, 'error'); });
            });
    }

    /* —— 晋升节点（§16.10 流程 3，A5：节点级无 primary/replica） ——
       promote API 需要 groupId：从冗余组列表中查找该节点所属组；
       若节点未归属任何组，则提示无法晋升。 */
    function promoteNode(nodeId) {
        apiGet('/api/replication/groups').then(function(data) {
            if (!data) return;
            var groups = data.groups || [];
            allGroups = groups;
            var belongs = null;
            for (var i = 0; i < groups.length; i++) {
                if ((groups[i].nodeIds || []).indexOf(nodeId) >= 0) { belongs = groups[i]; break; }
            }
            if (!belongs) {
                showToast('节点 ' + nodeId + ' 未归属任何冗余组，无法晋升', 'warning');
                return;
            }
            showConfirm('确认晋升副本', '将把冗余组 <b>' + escapeHtml(belongs.groupId) + '</b> 中位于节点 <b>' + escapeHtml(nodeId) + '</b> 上的副本提升为 PRIMARY，<br>该组原有的 PRIMARY 副本将被降级为 SECONDARY。',
                function() {
                    apiPost('/api/nodes/' + encodeURIComponent(nodeId) + '/promote', {})
                        .then(function(res) {
                            showToast(res.message || '晋升完成', 'success');
                        })
                        .catch(function(err){ showToast('晋升失败: ' + err.message, 'error'); });
                });
        }).catch(function(err){ showToast('无法加载冗余组: ' + err.message, 'error'); });
    }

    /* ===================== 冗余组管理（§16.3） ===================== */
    function loadGroups() {
        apiGet('/api/replication/groups').then(function(data) {
            if (!data) return;
            var groups = data.groups || [];
            allGroups = groups;
            var tbody = document.querySelector('#groupTable tbody');
            if (groups.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" class="empty-row">暂无冗余组，点击右上角创建</td></tr>';
                return;
            }
            // 节点 -> 是否在线、host 映射
            var nodeMap = {};
            allNodes.forEach(function(n){ nodeMap[n.nodeId] = n; });
            var html = '';
            groups.forEach(function(g) {
                var ids = g.nodeIds || [];
                // 同 host 检测
                var hostCount = {}, dup = false;
                ids.forEach(function(id){
                    var n = nodeMap[id]; if (!n) return;
                    var h = hostOf(n.address);
                    hostCount[h] = (hostCount[h] || 0) + 1;
                    if (hostCount[h] > 1) dup = true;
                });
                // 冗余度降级检测（§4 R6 / §10.2）：组内 alive(online && !draining) < 组大小
                var aliveCount = 0;
                ids.forEach(function(id){
                    var n = nodeMap[id];
                    if (n && n.status === 'online' && n.drainStatus !== 1) aliveCount++;
                });
                var degraded = aliveCount < ids.length;
                var statusBadge = '';
                if (degraded) {
                    statusBadge += '<span class="status-badge status-draining" title="组内可服务节点少于组大小">⚠ 冗余降级 (' + aliveCount + '/' + ids.length + ')</span> ';
                }
                statusBadge += dup
                    ? '<span class="status-badge status-draining">⚠ 同主机</span>'
                    : '<span class="status-badge status-online">正常</span>';
                var chips = ids.map(function(id){
                    var n = nodeMap[id];
                    var sameHost = n && hostCount[hostOf(n.address)] > 1;
                    return '<span class="node-chip ' + (sameHost ? 'same-host-warn' : '') + '">' + escapeHtml(id) + '</span>';
                }).join('');
                html += '<tr>'
                    + '<td>' + escapeHtml(g.groupId) + '</td>'
                    + '<td>' + chips + '</td>'
                    + '<td>' + statusBadge + '</td>'
                    + '<td>'
                    +   '<button class="action-btn primary" onclick="openGroupModal(\\'' + escapeHtml(g.groupId) + '\\')">编辑</button>'
                    +   '<button class="action-btn danger" onclick="deleteGroup(\\'' + escapeHtml(g.groupId) + '\\')">删除</button>'
                    + '</td>'
                    + '</tr>';
            });
            tbody.innerHTML = html;
        }).catch(function(err) {
            document.querySelector('#groupTable tbody').innerHTML =
                '<tr><td colspan="4" class="empty-row" style="color:red">' + escapeHtml(err.message) + '</td></tr>';
        });
    }

    /* —— 校验函数（§16.9），适配实际 API：group.nodeIds（字符串数组）—— */
    function validateGroupNodeSelection(selectedNodeIds, allNodesList, existingGroups, editingId) {
        var errors = [], warnings = [];
        if (selectedNodeIds.length < 2) errors.push('至少选择 2 个节点');
        if (selectedNodeIds.length > 3) errors.push('最多选择 3 个节点');
        var nodeMap = {};
        allNodesList.forEach(function(n){ nodeMap[n.nodeId] = n; });
        // 重叠检查
        selectedNodeIds.forEach(function(nodeId) {
            existingGroups.forEach(function(group) {
                if (group.groupId === editingId) return;
                var ids = group.nodeIds || [];
                if (ids.indexOf(nodeId) >= 0) {
                    errors.push('节点 ' + nodeId + ' 已属于冗余组 ' + group.groupId + '，不可重复分配');
                }
            });
        });
        // 同 host 检查（仅警告）
        var hosts = {};
        selectedNodeIds.forEach(function(id){
            var n = nodeMap[id]; if (!n) return;
            var h = hostOf(n.address);
            if (!hosts[h]) hosts[h] = [];
            hosts[h].push(id);
        });
        Object.keys(hosts).forEach(function(host) {
            if (hosts[host].length > 1) {
                warnings.push('节点 ' + hosts[host].join(', ') + ' 位于同一主机 (' + host + ')');
            }
        });
        return { errors: errors, warnings: warnings, valid: errors.length === 0 };
    }

    /* —— 创建/编辑弹窗 —— */
    function openGroupModal(groupId) {
        editingGroupId = groupId;
        var title = document.getElementById('groupModalTitle');
        var idInput = document.getElementById('groupNewId');
        var pick = document.getElementById('groupNodePick');
        if (groupId) {
            title.textContent = '编辑冗余组';
            idInput.value = groupId;
            idInput.disabled = true;
        } else {
            title.textContent = '创建冗余组';
            idInput.value = '';
            idInput.disabled = false;
        }
        // 渲染节点勾选（离线 disabled）
        var nodeMap = {};
        allNodes.forEach(function(n){ nodeMap[n.nodeId] = n; });
        var preselect = {};
        if (groupId) {
            for (var i = 0; i < allGroups.length; i++) {
                if (allGroups[i].groupId === groupId) {
                    (allGroups[i].nodeIds || []).forEach(function(id){ preselect[id] = true; });
                    break;
                }
            }
        }
        if (allNodes.length === 0) {
            pick.innerHTML = '<div style="color:#95a5a6;padding:0.5rem;">暂无可用节点</div>';
        } else {
            var html = '';
            allNodes.forEach(function(n) {
                var offline = n.status !== 'online';
                var cls = offline ? 'disabled' : '';
                var dis = offline ? 'disabled' : '';
                var chk = preselect[n.nodeId] ? 'checked' : '';
                html += '<label class="' + cls + '">'
                    + '<input type="checkbox" value="' + escapeHtml(n.nodeId) + '" ' + chk + ' ' + dis
                    + ' onchange="onGroupNodeChange()">'
                    + escapeHtml(n.nodeId) + ' <span style="color:#95a5a6;font-size:0.8rem;">(' + escapeHtml(n.address)
                    + (offline ? ' · 离线' : '') + ')</span></label>';
            });
            pick.innerHTML = html;
        }
        document.getElementById('groupValidation').innerHTML = '';
        updateGroupSubmitState();
        document.getElementById('groupModal').classList.add('show');
        onGroupNodeChange(); // 初始校验（编辑态预选）
    }
    function closeGroupModal() {
        document.getElementById('groupModal').classList.remove('show');
        editingGroupId = null;
    }
    function getSelectedNodeIds() {
        var ids = [];
        document.querySelectorAll('#groupNodePick input[type=checkbox]:checked').forEach(function(cb){ ids.push(cb.value); });
        return ids;
    }
    function onGroupNodeChange() {
        var selected = getSelectedNodeIds();
        var v = validateGroupNodeSelection(selected, allNodes, allGroups, editingGroupId);
        var msg = document.getElementById('groupValidation');
        var html = '';
        v.errors.forEach(function(e){ html += '<div class="validation-msg error">✗ ' + escapeHtml(e) + '</div>'; });
        v.warnings.forEach(function(w){ html += '<div class="validation-msg warning">⚠ ' + escapeHtml(w) + '</div>'; });
        msg.innerHTML = html;
        updateGroupSubmitState(v.valid);
    }
    function updateGroupSubmitState(valid) {
        var selected = getSelectedNodeIds();
        if (valid === undefined) {
            var v = validateGroupNodeSelection(selected, allNodes, allGroups, editingGroupId);
            valid = v.valid;
        }
        document.getElementById('groupSubmitBtn').disabled = !valid || selected.length === 0;
    }
    function submitGroup() {
        var selected = getSelectedNodeIds();
        var v = validateGroupNodeSelection(selected, allNodes, allGroups, editingGroupId);
        if (!v.valid) { showToast('校验未通过', 'error'); return; }
        if (editingGroupId) {
            apiPut('/api/replication/groups/' + encodeURIComponent(editingGroupId), { nodeIds: selected })
                .then(function() {
                    showToast('冗余组已更新', 'success');
                    closeGroupModal();
                    loadGroups();
                })
                .catch(function(err){ showToast('更新失败: ' + err.message, 'error'); });
        } else {
            var newId = document.getElementById('groupNewId').value.trim();
            if (!newId) { showToast('请填写组ID', 'error'); return; }
            apiPost('/api/replication/groups', { groupId: newId, nodeIds: selected })
                .then(function() {
                    showToast('冗余组已创建', 'success');
                    closeGroupModal();
                    loadGroups();
                })
                .catch(function(err){ showToast('创建失败: ' + err.message, 'error'); });
        }
    }
    function deleteGroup(groupId) {
        showConfirm('确认删除冗余组', '删除冗余组 <b>' + escapeHtml(groupId) + '</b>？<br>该操作不会立即迁移已存数据，仅解除节点分组关系。',
            function() {
                apiDelete('/api/replication/groups/' + encodeURIComponent(groupId))
                    .then(function() {
                        showToast('冗余组已删除', 'success');
                        loadGroups();
                    })
                    .catch(function(err){ showToast('删除失败: ' + err.message, 'error'); });
            }, 'btn-danger');
    }

    /* ===================== 对账同步（§16.4） ===================== */
    function loadSync() {
        apiGet('/api/replication/sync').then(function(data) {
            if (!data) return;
            var s = data.summary || {};
            var pending = s.totalPending || 0;
            var done = s.syncedToday || 0;
            var failed = s.failed || 0;
            var current = s.currentJobs || 0;
            document.getElementById('syncPending').innerText = pending;
            document.getElementById('syncDone').innerText = done;
            document.getElementById('syncFailed').innerText = failed;
            document.getElementById('syncCurrent').innerText = current;
            var total = pending + done;
            var pct = total > 0 ? Math.round(done * 100 / total) : 0;
            document.getElementById('syncProgressFill').style.width = pct + '%';
            document.getElementById('syncProgressLabel').innerText = pct + '% (' + done + '/' + total + ')';
            // 失败任务表（§16.10 流程 6：重试）
            var jobs = data.failedJobs || [];
            var tbody = document.querySelector('#failedJobsTable tbody');
            if (jobs.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty-row">暂无失败任务</td></tr>';
            } else {
                var html = '';
                jobs.forEach(function(j) {
                    html += '<tr>'
                        + '<td>' + escapeHtml(j.taskId) + '</td>'
                        + '<td style="font-family:monospace;font-size:0.82rem;">' + escapeHtml(j.fileHash) + '</td>'
                        + '<td>' + escapeHtml(j.sourceNode) + '</td>'
                        + '<td>' + escapeHtml(j.targetNode) + '</td>'
                        + '<td><span class="status-badge status-offline">' + (j.retryCount || 0) + '</span></td>'
                        + '<td><button class="action-btn success" onclick="retryTask(\\'' + escapeHtml(j.taskId) + '\\')">重试</button></td>'
                        + '</tr>';
                });
                tbody.innerHTML = html;
            }
        }).catch(function(err) {
            document.getElementById('syncPending').innerText = '-';
            document.querySelector('#failedJobsTable tbody').innerHTML =
                '<tr><td colspan="6" class="empty-row" style="color:red">' + escapeHtml(err.message) + '</td></tr>';
        });
    }
    function triggerSync() {
        showConfirm('手动触发全量对账', '将立即触发一次全量对账同步？<br>系统会扫描所有待同步任务并在核心窗口内执行。',
            function() {
                apiPost('/api/replication/sync', {})
                    .then(function(res) {
                        showToast(res.message || '已触发全量对账', 'success');
                        loadSync();
                    })
                    .catch(function(err){ showToast('触发失败: ' + err.message, 'error'); });
            });
    }
    function retryTask(taskId) {
        showConfirm('重试失败任务', '重置任务 <b>' + escapeHtml(taskId) + '</b> 的失败计数器，并将其移回等待队列？',
            function() {
                apiPost('/api/replication/sync/retry/' + encodeURIComponent(taskId), {})
                    .then(function(res) {
                        showToast(res.message || '任务已重置', 'success');
                        loadSync();
                    })
                    .catch(function(err){ showToast('重试失败: ' + err.message, 'error'); });
            });
    }

    /* ===================== 同步策略（§16.5） ===================== */
    function fillTimeSelects() {
        var hs = document.getElementById('winStartH'); // 借用任一 select 判断是否已填充
        if (hs.options.length > 0) return;
        var hourSel = ['winStartH','winEndH','softDeadlineH'];
        var minSel = ['winStartM','winEndM','softDeadlineM'];
        hourSel.forEach(function(id){
            var sel = document.getElementById(id);
            for (var h = 0; h < 24; h++) {
                sel.appendChild(new Option((h < 10 ? '0' : '') + h, (h < 10 ? '0' : '') + h));
            }
        });
        var mins = ['00','15','30','45'];
        minSel.forEach(function(id){
            var sel = document.getElementById(id);
            mins.forEach(function(m){ sel.appendChild(new Option(m, m)); });
        });
    }
    function splitTime(t) {
        // 兼容 "01:00" / "01:00:00" / 缺省
        if (!t) return ['00','00'];
        var parts = String(t).split(':');
        return [parts[0] || '00', parts[1] || '00'];
    }
    function loadPolicy() {
        fillTimeSelects();
        apiGet('/api/replication/policy').then(function(p) {
            if (!p) return;
            var ws = splitTime((p.syncWindow && p.syncWindow.start) || '01:00');
            var we = splitTime((p.syncWindow && p.syncWindow.end) || '03:00');
            var sd = splitTime(p.softDeadline || '03:00');
            document.getElementById('winStartH').value = ws[0];
            document.getElementById('winStartM').value = ws[1];
            document.getElementById('winEndH').value = we[0];
            document.getElementById('winEndM').value = we[1];
            document.getElementById('softDeadlineH').value = sd[0];
            document.getElementById('softDeadlineM').value = sd[1];
            document.getElementById('rateLimit').value = (p.rateLimitMbps !== undefined ? p.rateLimitMbps : 50);
            document.getElementById('maxConcurrency').value = (p.maxConcurrency !== undefined ? p.maxConcurrency : 4);
            policyLoaded = true;
        }).catch(function(err) {
            showToast('加载策略失败: ' + err.message, 'error');
        });
    }
    function savePolicy() {
        var body = {
            syncWindow: {
                start: document.getElementById('winStartH').value + ':' + document.getElementById('winStartM').value,
                end: document.getElementById('winEndH').value + ':' + document.getElementById('winEndM').value
            },
            softDeadline: document.getElementById('softDeadlineH').value + ':' + document.getElementById('softDeadlineM').value,
            rateLimitMbps: parseInt(document.getElementById('rateLimit').value, 10),
            maxConcurrency: parseInt(document.getElementById('maxConcurrency').value, 10)
        };
        // 前端校验
        if (isNaN(body.rateLimitMbps) || body.rateLimitMbps < 0) {
            showToast('限速必须为非负整数', 'error'); return;
        }
        if (isNaN(body.maxConcurrency) || body.maxConcurrency < 1 || body.maxConcurrency > 10) {
            showToast('最大并发数须在 1~10 之间', 'error'); return;
        }
        showConfirm('保存同步策略', '确认保存当前同步策略配置？',
            function() {
                apiPut('/api/replication/policy', body)
                    .then(function() { showToast('同步策略已保存', 'success'); })
                    .catch(function(err){ showToast('保存失败: ' + err.message, 'error'); });
            });
    }

    /* ===================== 告警（§16.6） ===================== */
    function loadAlerts() {
        apiGet('/api/replication/alerts').then(function(data) {
            if (!data) return;
            var active = data.active || [];
            var resolved = data.resolved || [];
            // 派生同 host 警告（来自冗余组 + 节点）：组内 ≥2 节点同 host
            // 这里用已缓存的 allGroups / allNodes；若未加载则拉取一次后重入
            deriveAndRenderAlerts(active, resolved);
            // 若冗余组尚未加载，加载后重渲染告警
            if (allGroups.length === 0 && allNodes.length > 0) {
                apiGet('/api/replication/groups').then(function(d){
                    if (!d) return;
                    allGroups = d.groups || [];
                    deriveAndRenderAlerts(active, resolved);
                }).catch(function(){});
            }
        }).catch(function(err) {
            document.getElementById('alertActiveCount').innerText = '-';
            document.querySelector('#alertTable tbody').innerHTML =
                '<tr><td colspan="5" class="empty-row" style="color:red">' + escapeHtml(err.message) + '</td></tr>';
            updateAlertDot(0);
        });
    }
    function deriveAndRenderAlerts(active, resolved) {
        var rows = [];
        // 严重：连续失败（active 任务为 retryCount>=4 的失败任务）
        active.forEach(function(t) {
            rows.push({
                level: 'critical',
                levelText: '严重',
                content: '节点 ' + escapeHtml(t.sourceNode || '-') + ' 连续 ' + (t.retryCount || 0) + ' 次同步失败（任务 ' + escapeHtml(t.taskId || '-') + '）',
                node: escapeHtml(t.sourceNode || '-'),
                triggered: '-',
                resolved: '-'
            });
        });
        // 警告：冗余组内同 host
        var nodeMap = {};
        allNodes.forEach(function(n){ nodeMap[n.nodeId] = n; });
        allGroups.forEach(function(g) {
            var ids = g.nodeIds || [];
            var hosts = {};
            ids.forEach(function(id){
                var n = nodeMap[id]; if (!n) return;
                var h = hostOf(n.address);
                if (!hosts[h]) hosts[h] = [];
                hosts[h].push(id);
            });
            Object.keys(hosts).forEach(function(h) {
                if (hosts[h].length > 1) {
                    rows.push({
                        level: 'warning',
                        levelText: '警告',
                        content: '冗余组 ' + escapeHtml(g.groupId) + ' 内节点 ' + escapeHtml(hosts[h].join(', ')) + ' 位于同一主机 (' + escapeHtml(h) + ')',
                        node: escapeHtml(hosts[h].join(', ')),
                        triggered: '-',
                        resolved: '-'
                    });
                }
            });
        });
        // 渲染
        document.getElementById('alertActiveCount').innerText = rows.length;
        document.getElementById('alertResolvedCount').innerText = resolved.length;
        updateAlertDot(rows.length);
        var tbody = document.querySelector('#alertTable tbody');
        if (rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-row">暂无活跃告警</td></tr>';
        } else {
            var html = '';
            rows.forEach(function(a) {
                var badge = a.level === 'critical'
                    ? '<span class="status-badge alert-level-critical">' + a.levelText + '</span>'
                    : '<span class="status-badge alert-level-warning">' + a.levelText + '</span>';
                html += '<tr>'
                    + '<td>' + badge + '</td>'
                    + '<td>' + escapeHtml(a.content) + '</td>'
                    + '<td>' + a.node + '</td>'
                    + '<td>' + a.triggered + '</td>'
                    + '<td>' + a.resolved + '</td>'
                    + '</tr>';
            });
            tbody.innerHTML = html;
        }
    }
    function updateAlertDot(count) {
        var dot = document.getElementById('alertDot');
        dot.textContent = count;
        dot.className = 'alert-dot ' + (count > 0 ? '' : 'zero');
    }

    /* ===================== 修改密码 ===================== */
    function openChangePasswordModal() {
        document.getElementById('changePwdModal').classList.add('show');
    }
    function closeChangePasswordModal() {
        document.getElementById('changePwdModal').classList.remove('show');
        document.getElementById('oldPassword').value = '';
        document.getElementById('newPassword').value = '';
    }
    function submitChangePassword() {
        var oldPassword = document.getElementById('oldPassword').value;
        var newPassword = document.getElementById('newPassword').value;
        if (!oldPassword || !newPassword) { showToast('请填写完整', 'error'); return; }
        fetch('/api/change-password', {
            method: 'POST',
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            body: 'oldPassword=' + encodeURIComponent(oldPassword) + '&newPassword=' + encodeURIComponent(newPassword)
        })
        .then(function(res) {
            if (res.redirected) { location.href = res.url; return null; }
            return res.json();
        })
        .then(function(data) {
            if (!data) return;
            if (data.success) {
                showToast(data.message || '修改成功', 'success');
                setTimeout(function(){ location.href = '/login'; }, 800);
            } else {
                showToast('修改失败: ' + (data.error || '未知错误'), 'error');
            }
        })
        .catch(function(err){ showToast('请求失败: ' + err.message, 'error'); });
    }

    /* ===================== 启动 ===================== */
    loadSecurity();
    restartPolling();
    </script>
</body>
</html>
""";
            return html;
        }
    }
}
