package org.jnfs.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 仪表盘服务 (HTTP Server)
 * 提供系统状态的 Web 界面和 JSON API
 */
public class DashboardServer {

    private static final Logger LOG = LoggerFactory.getLogger(DashboardServer.class);

    private final int port;

    public DashboardServer(int port) {
        this.port = port;
    }

    public void start() {
        try {
            com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress(port), 0);

            server.createContext("/", new DashboardHttpHandler());

            server.createContext("/api/nodes", exchange -> {
                String json = getNodesJson();
                byte[] response = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length);
                try (java.io.OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }
            });

            server.setExecutor(null);
            server.start();
            LOG.info("JNFS Dashboard 启动成功，访问地址: http://localhost:{}", port);
        } catch (Exception e) {
            LOG.error("Dashboard启动失败", e);
        }
    }

    private String getNodesJson() {
        Map<String, RegistryHandler.NodeInfo> nodes = RegistryHandler.getDataNodes();
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int i = 0;
        for (Map.Entry<String, RegistryHandler.NodeInfo> entry : nodes.entrySet()) {
            if (i > 0) sb.append(",");
            sb.append("{");
            sb.append("\"address\":\"").append(entry.getKey()).append("\",");
            sb.append("\"freeSpace\":").append(entry.getValue().freeSpace).append(",");
            sb.append("\"lastHeartbeat\":").append(entry.getValue().lastHeartbeatTime);
            sb.append("}");
            i++;
        }
        sb.append("]");
        return sb.toString();
    }

    static class DashboardHttpHandler implements com.sun.net.httpserver.HttpHandler {
        @Override
        public void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
            // 美化后的 HTML 模板
            String html = "<!DOCTYPE html>\n" +
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
                    "        }\n" +
                    "        .header h1 {\n" +
                    "            margin: 0;\n" +
                    "            font-size: 1.5rem;\n" +
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
                    "    </style>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "    <div class=\"header\">\n" +
                    "        <h1>JNFS 运维监控中心</h1>\n" +
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
                    "        </div>\n" +
                    "\n" +
                    "        <div class=\"table-container\">\n" +
                    "            <table id=\"nodeTable\">\n" +
                    "                <thead>\n" +
                    "                    <tr>\n" +
                    "                        <th>节点地址</th>\n" +
                    "                        <th>剩余空间</th>\n" +
                    "                        <th>最后心跳时间</th>\n" +
                    "                        <th>状态</th>\n" +
                    "                    </tr>\n" +
                    "                </thead>\n" +
                    "                <tbody>\n" +
                    "                    <tr><td colspan=\"4\" style=\"text-align:center;color:#999;\">加载数据中...</td></tr>\n" +
                    "                </tbody>\n" +
                    "            </table>\n" +
                    "        </div>\n" +
                    "        <div class=\"refresh-info\">数据每 2 秒自动刷新</div>\n" +
                    "    </div>\n" +
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
                    "                .then(res => res.json())\n" +
                    "                .then(data => {\n" +
                    "                    const tbody = document.querySelector('#nodeTable tbody');\n" +
                    "                    \n" +
                    "                    // 统计数据\n" +
                    "                    let activeCount = 0;\n" +
                    "                    let totalSpace = 0;\n" +
                    "                    const now = new Date().getTime();\n" +
                    "\n" +
                    "                    if (data.length === 0) {\n" +
                    "                         tbody.innerHTML = '<tr><td colspan=\"4\" style=\"text-align:center;color:#999;\">暂无节点连接</td></tr>';\n" +
                    "                    } else {\n" +
                    "                        tbody.innerHTML = '';\n" +
                    "                    }\n" +
                    "\n" +
                    "                    data.forEach(node => {\n" +
                    "                        const diff = now - node.lastHeartbeat;\n" +
                    "                        const isOnline = diff < 30000;\n" +
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
                    "                    document.querySelector('#nodeTable tbody').innerHTML = '<tr><td colspan=\"4\" style=\"text-align:center;color:red;\">无法连接到服务器</td></tr>';\n" +
                    "                });\n" +
                    "        }\n" +
                    "\n" +
                    "        setInterval(loadData, 2000);\n" +
                    "        loadData();\n" +
                    "    </script>\n" +
                    "</body>\n" +
                    "</html>";

            byte[] response = html.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length);
            try (java.io.OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        }
    }
}
