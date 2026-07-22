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

    private HttpServer server;

    public DashboardServer(int port) {
        this(port, null);
    }

    public DashboardServer(int port, AuthManager authManager) {
        this.port = port;
        this.authManager = authManager;
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
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int i = 0;
        long now = System.currentTimeMillis();
        for (Map.Entry<String, RegistryHandler.NodeInfo> entry : nodes.entrySet()) {
            if (i > 0) sb.append(",");
            RegistryHandler.NodeInfo info = entry.getValue();
            sb.append("{");
            sb.append("\"nodeId\":\"").append(escapeJson(info.nodeId)).append("\",");
            sb.append("\"address\":\"").append(escapeJson(info.address)).append("\",");
            sb.append("\"freeSpace\":").append(info.freeSpace).append(",");
            sb.append("\"lastHeartbeat\":").append(info.lastHeartbeatTime).append(",");
            // 服务端计算状态，避免客户端时间不一致导致误判
            boolean isOnline = (now - info.lastHeartbeatTime) < RegistryHandler.heartbeatTimeout;
            sb.append("\"status\":\"").append(isOnline ? "online" : "offline").append("\"");
            sb.append("}");
            i++;
        }
        sb.append("]");
        return sb.toString();
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
            return "<!DOCTYPE html>\n" +
                    "<html lang=\"zh-CN\">\n" +
                    "<head>\n" +
                    "    <meta charset=\"UTF-8\">\n" +
                    "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                    "    <title>JNFS 运维监控中心</title>\n" +
                    "    <style>\n" +
                    "        :root {\n" +
                    "            --primary-color: #3498db;\n" +
                    "            --bg-color: #f4f7f6;\n" +
                    "            --card-bg: #ffffff;\n" +
                    "            --text-color: #333;\n" +
                    "        }\n" +
                    "        body {\n" +
                    "            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n" +
                    "            background-color: var(--bg-color);\n" +
                    "            color: var(--text-color);\n" +
                    "            margin: 0;\n" +
                    "            padding: 0;\n" +
                    "        }\n" +
                    "        .header {\n" +
                    "            background-color: var(--primary-color);\n" +
                    "            color: white;\n" +
                    "            padding: 1rem 2rem;\n" +
                    "            box-shadow: 0 2px 4px rgba(0,0,0,0.1);\n" +
                    "            display: flex;\n" +
                    "            justify-content: space-between;\n" +
                    "            align-items: center;\n" +
                    "        }\n" +
                    "        .header h1 {\n" +
                    "            margin: 0;\n" +
                    "            font-size: 1.5rem;\n" +
                    "        }\n" +
                    "        .header-actions {\n" +
                    "            display: flex;\n" +
                    "            gap: 0.75rem;\n" +
                    "            align-items: center;\n" +
                    "        }\n" +
                    "        .header-actions button, .header-actions a {\n" +
                    "            background: rgba(255,255,255,0.2);\n" +
                    "            color: white;\n" +
                    "            border: 1px solid rgba(255,255,255,0.4);\n" +
                    "            padding: 0.4rem 0.9rem;\n" +
                    "            border-radius: 4px;\n" +
                    "            cursor: pointer;\n" +
                    "            font-size: 0.85rem;\n" +
                    "            text-decoration: none;\n" +
                    "            transition: background 0.2s;\n" +
                    "        }\n" +
                    "        .header-actions button:hover, .header-actions a:hover {\n" +
                    "            background: rgba(255,255,255,0.35);\n" +
                    "        }\n" +
                    "        .container {\n" +
                    "            max-width: 1200px;\n" +
                    "            margin: 2rem auto;\n" +
                    "            padding: 0 1rem;\n" +
                    "        }\n" +
                    "        .stats-grid {\n" +
                    "            display: grid;\n" +
                    "            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));\n" +
                    "            gap: 1.5rem;\n" +
                    "            margin-bottom: 2rem;\n" +
                    "        }\n" +
                    "        .card {\n" +
                    "            background-color: var(--card-bg);\n" +
                    "            border-radius: 8px;\n" +
                    "            padding: 1.5rem;\n" +
                    "            box-shadow: 0 2px 8px rgba(0,0,0,0.05);\n" +
                    "            transition: transform 0.2s;\n" +
                    "        }\n" +
                    "        .card:hover {\n" +
                    "            transform: translateY(-2px);\n" +
                    "        }\n" +
                    "        .card h3 {\n" +
                    "            margin: 0 0 0.5rem 0;\n" +
                    "            color: #7f8c8d;\n" +
                    "            font-size: 0.9rem;\n" +
                    "            text-transform: uppercase;\n" +
                    "        }\n" +
                    "        .card .value {\n" +
                    "            font-size: 2rem;\n" +
                    "            font-weight: bold;\n" +
                    "            color: #2c3e50;\n" +
                    "        }\n" +
                    "        .table-container {\n" +
                    "            background-color: var(--card-bg);\n" +
                    "            border-radius: 8px;\n" +
                    "            box-shadow: 0 2px 8px rgba(0,0,0,0.05);\n" +
                    "            overflow: hidden;\n" +
                    "        }\n" +
                    "        table {\n" +
                    "            width: 100%;\n" +
                    "            border-collapse: collapse;\n" +
                    "        }\n" +
                    "        th, td {\n" +
                    "            padding: 1rem;\n" +
                    "            text-align: left;\n" +
                    "            border-bottom: 1px solid #eee;\n" +
                    "        }\n" +
                    "        th {\n" +
                    "            background-color: #f8f9fa;\n" +
                    "            font-weight: 600;\n" +
                    "            color: #2c3e50;\n" +
                    "        }\n" +
                    "        tr:last-child td {\n" +
                    "            border-bottom: none;\n" +
                    "        }\n" +
                    "        .status-badge {\n" +
                    "            padding: 0.25rem 0.75rem;\n" +
                    "            border-radius: 50px;\n" +
                    "            font-size: 0.85rem;\n" +
                    "            font-weight: 500;\n" +
                    "        }\n" +
                    "        .status-online {\n" +
                    "            background-color: #e8f5e9;\n" +
                    "            color: #2e7d32;\n" +
                    "        }\n" +
                    "        .status-offline {\n" +
                    "            background-color: #ffebee;\n" +
                    "            color: #c62828;\n" +
                    "        }\n" +
                    "        .refresh-info {\n" +
                    "            text-align: right;\n" +
                    "            color: #95a5a6;\n" +
                    "            font-size: 0.8rem;\n" +
                    "            margin-top: 0.5rem;\n" +
                    "        }\n" +
                    "        /* 修改密码弹窗 */\n" +
                    "        .modal-overlay {\n" +
                    "            display: none;\n" +
                    "            position: fixed; top: 0; left: 0; right: 0; bottom: 0;\n" +
                    "            background: rgba(0,0,0,0.5);\n" +
                    "            justify-content: center; align-items: center;\n" +
                    "            z-index: 100;\n" +
                    "        }\n" +
                    "        .modal-overlay.show { display: flex; }\n" +
                    "        .modal {\n" +
                    "            background: #fff; padding: 1.5rem; border-radius: 8px;\n" +
                    "            width: 100%; max-width: 380px;\n" +
                    "        }\n" +
                    "        .modal h2 { margin: 0 0 1rem 0; font-size: 1.1rem; color: #2c3e50; }\n" +
                    "        .modal input {\n" +
                    "            width: 100%; padding: 0.6rem; margin-bottom: 0.75rem;\n" +
                    "            border: 1px solid #ddd; border-radius: 4px; font-size: 0.95rem;\n" +
                    "            box-sizing: border-box;\n" +
                    "        }\n" +
                    "        .modal-actions { display: flex; gap: 0.5rem; justify-content: flex-end; }\n" +
                    "        .modal-actions button { padding: 0.5rem 1rem; border-radius: 4px; cursor: pointer; border: none; font-size: 0.9rem; }\n" +
                    "        .btn-cancel { background: #ecf0f1; color: #555; }\n" +
                    "        .btn-confirm { background: #3498db; color: #fff; }\n" +
                    "    </style>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "    <div class=\"header\">\n" +
                    "        <h1>JNFS 运维监控中心</h1>\n" +
                    (authEnabled
                            ? "        <div class=\"header-actions\">\n" +
                              "            <button onclick=\"openChangePasswordModal()\">修改密码</button>\n" +
                              "            <a href=\"/logout\">登出</a>\n" +
                              "        </div>\n"
                            : "") +
                    "    </div>\n" +
                    "    <div class=\"container\">\n" +
                    "        <div class=\"stats-grid\">\n" +
                    "            <div class=\"card\">\n" +
                    "                <h3>活跃存储节点</h3>\n" +
                    "                <div class=\"value\" id=\"activeNodes\">-</div>\n" +
                    "            </div>\n" +
                    "            <div class=\"card\">\n" +
                    "                <h3>全网剩余容量</h3>\n" +
                    "                <div class=\"value\" id=\"totalFreeSpace\">-</div>\n" +
                    "            </div>\n" +
                    "            <div class=\"card\">\n" +
                    "                <h3>安全状态</h3>\n" +
                    "                <div class=\"value\" id=\"securityStatus\"><span style='color:#e67e22'>加载中...</span></div>\n" +
                    "            </div>\n" +
                    "        </div>\n" +
                    "\n" +
                    "        <div class=\"table-container\">\n" +
                    "            <table id=\"nodeTable\">\n" +
                    "                <thead>\n" +
                    "                    <tr>\n" +
                    "                        <th>节点ID</th>\n" +
                    "                        <th>节点地址</th>\n" +
                    "                        <th>剩余空间</th>\n" +
                    "                        <th>最后心跳时间</th>\n" +
                    "                        <th>状态</th>\n" +
                    "                    </tr>\n" +
                    "                </thead>\n" +
                    "                <tbody>\n" +
                    "                    <tr><td colspan=\"5\" style=\"text-align:center;color:#999;\">加载数据中...</td></tr>\n" +
                    "                </tbody>\n" +
                    "            </table>\n" +
                    "        </div>\n" +
                    "        <div class=\"refresh-info\">数据每 2 秒自动刷新</div>\n" +
                    "    </div>\n" +
                    "\n" +
                    (authEnabled
                            ? "    <!-- 修改密码弹窗 -->\n" +
                              "    <div class=\"modal-overlay\" id=\"changePwdModal\">\n" +
                              "        <div class=\"modal\">\n" +
                              "            <h2>修改密码</h2>\n" +
                              "            <input type=\"password\" id=\"oldPassword\" placeholder=\"旧密码\" autocomplete=\"current-password\">\n" +
                              "            <input type=\"password\" id=\"newPassword\" placeholder=\"新密码（至少 4 位）\" autocomplete=\"new-password\">\n" +
                              "            <div class=\"modal-actions\">\n" +
                              "                <button class=\"btn-cancel\" onclick=\"closeChangePasswordModal()\">取消</button>\n" +
                              "                <button class=\"btn-confirm\" onclick=\"submitChangePassword()\">确认修改</button>\n" +
                              "            </div>\n" +
                              "        </div>\n" +
                              "    </div>\n"
                            : "") +
                    "\n" +
                    "    <script>\n" +
                    "        function formatBytes(bytes, decimals = 2) {\n" +
                    "            if (bytes === 0) return '0 Bytes';\n" +
                    "            const k = 1024;\n" +
                    "            const dm = decimals < 0 ? 0 : decimals;\n" +
                    "            const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];\n" +
                    "            const i = Math.floor(Math.log(bytes) / Math.log(k));\n" +
                    "            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];\n" +
                    "        }\n" +
                    "\n" +
                    "        function loadData() {\n" +
                    "            fetch('/api/nodes')\n" +
                    "                .then(res => {\n" +
                    "                    if (res.redirected) { location.href = res.url; return null; }\n" +
                    "                    return res.json();\n" +
                    "                })\n" +
                    "                .then(data => {\n" +
                    "                    if (!data) return;\n" +
                    "                    const tbody = document.querySelector('#nodeTable tbody');\n" +
                    "                    \n" +
                    "                    // 统计数据\n" +
                    "                    let activeCount = 0;\n" +
                    "                    let totalSpace = 0;\n" +
                    "                    const now = new Date().getTime();\n" +
                    "\n" +
                    "                    if (data.length === 0) {\n" +
                    "                         tbody.innerHTML = '<tr><td colspan=\"5\" style=\"text-align:center;color:#999;\">暂无节点连接</td></tr>';\n" +
                    "                    } else {\n" +
                    "                        tbody.innerHTML = '';\n" +
                    "                    }\n" +
                    "\n" +
                    "                    data.forEach(node => {\n" +
                    "                        // 使用服务端返回的状态，避免客户端时间不一致问题\n" +
                    "                        const isOnline = node.status === 'online';\n" +
                    "                        \n" +
                    "                        if (isOnline) {\n" +
                    "                            activeCount++;\n" +
                    "                            totalSpace += node.freeSpace;\n" +
                    "                        }\n" +
                    "\n" +
                    "                        const tr = document.createElement('tr');\n" +
                    "                        const statusHtml = isOnline \n" +
                    "                            ? '<span class=\"status-badge status-online\">在线</span>' \n" +
                    "                            : '<span class=\"status-badge status-offline\">离线</span>';\n" +
                    "                        \n" +
                    "                        tr.innerHTML = `\n" +
                    "                            <td>${node.nodeId || '-'}</td>\n" +
                    "                            <td>${node.address}</td>\n" +
                    "                            <td>${formatBytes(node.freeSpace)}</td>\n" +
                    "                            <td>${new Date(node.lastHeartbeat).toLocaleString()}</td>\n" +
                    "                            <td>${statusHtml}</td>\n" +
                    "                        `;\n" +
                    "                        tbody.appendChild(tr);\n" +
                    "                    });\n" +
                    "\n" +
                    "                    // 更新统计卡片\n" +
                    "                    document.getElementById('activeNodes').innerText = activeCount;\n" +
                    "                    document.getElementById('totalFreeSpace').innerText = formatBytes(totalSpace);\n" +
                    "                })\n" +
                    "                .catch(err => {\n" +
                    "                    console.error('Fetch error:', err);\n" +
                    "                    document.querySelector('#nodeTable tbody').innerHTML = '<tr><td colspan=\"5\" style=\"text-align:center;color:red;\">无法连接到服务器</td></tr>';\n" +
                    "                });\n" +
                    "\n" +
                    "            // 加载安全状态\n" +
                    "            fetch('/api/security')\n" +
                    "                .then(res => {\n" +
                    "                    if (res.redirected) { location.href = res.url; return null; }\n" +
                    "                    return res.json();\n" +
                    "                })\n" +
                    "                .then(data => {\n" +
                    "                    if (!data) return;\n" +
                    "                    const el = document.getElementById('securityStatus');\n" +
                    "                    if (data.securityConfigured) {\n" +
                    "                        el.innerHTML = '<span style=\\'color:#2e7d32\\'>已配置 (自定义令牌)</span>';\n" +
                    "                    } else {\n" +
                    "                        el.innerHTML = '<span style=\\'color:#e67e22\\'>⚠ 使用默认令牌</span>';\n" +
                    "                    }\n" +
                    "                });\n" +
                    "        }\n" +
                    "\n" +
                    (authEnabled
                            ? "        function openChangePasswordModal() {\n" +
                              "            document.getElementById('changePwdModal').classList.add('show');\n" +
                              "        }\n" +
                              "        function closeChangePasswordModal() {\n" +
                              "            document.getElementById('changePwdModal').classList.remove('show');\n" +
                              "            document.getElementById('oldPassword').value = '';\n" +
                              "            document.getElementById('newPassword').value = '';\n" +
                              "        }\n" +
                              "        function submitChangePassword() {\n" +
                              "            const oldPassword = document.getElementById('oldPassword').value;\n" +
                              "            const newPassword = document.getElementById('newPassword').value;\n" +
                              "            if (!oldPassword || !newPassword) { alert('请填写完整'); return; }\n" +
                              "            fetch('/api/change-password', {\n" +
                              "                method: 'POST',\n" +
                              "                headers: {'Content-Type': 'application/x-www-form-urlencoded'},\n" +
                              "                body: 'oldPassword=' + encodeURIComponent(oldPassword) + '&newPassword=' + encodeURIComponent(newPassword)\n" +
                              "            })\n" +
                              "            .then(res => {\n" +
                              "                if (res.redirected) { location.href = res.url; return null; }\n" +
                              "                return res.json();\n" +
                              "            })\n" +
                              "            .then(data => {\n" +
                              "                if (!data) return;\n" +
                              "                if (data.success) {\n" +
                              "                    alert(data.message);\n" +
                              "                    location.href = '/login';\n" +
                              "                } else {\n" +
                              "                    alert('修改失败: ' + (data.error || '未知错误'));\n" +
                              "                }\n" +
                              "            })\n" +
                              "            .catch(err => alert('请求失败: ' + err));\n" +
                              "        }\n"
                            : "") +
                    "\n" +
                    "        setInterval(loadData, 2000);\n" +
                    "        loadData();\n" +
                    "    </script>\n" +
                    "</body>\n" +
                    "</html>";
        }
    }
}
