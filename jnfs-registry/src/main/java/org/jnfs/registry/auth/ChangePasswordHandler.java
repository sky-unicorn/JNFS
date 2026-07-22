package org.jnfs.registry.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 修改密码接口（受保护路由）
 * <p>
 * POST /api/change-password → 修改当前登录用户的密码
 * 请求体：application/x-www-form-urlencoded，参数 oldPassword / newPassword
 * 响应：JSON
 */
public class ChangePasswordHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ChangePasswordHandler.class);

    private final AuthManager authManager;

    public ChangePasswordHandler(AuthManager authManager) {
        this.authManager = authManager;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendJson(exchange, 405, "{\"error\":\"Method Not Allowed\"}");
            return;
        }

        // 从 AuthFilter 注入的 attribute 获取当前用户名
        String username = (String) exchange.getAttribute(AuthFilter.ATTR_USERNAME);
        if (username == null) {
            sendJson(exchange, 401, "{\"error\":\"未登录\"}");
            return;
        }

        Map<String, String> params = LoginHandler.parseFormData(exchange.getRequestBody());
        String oldPassword = params.get("oldPassword");
        String newPassword = params.get("newPassword");

        if (oldPassword == null || newPassword == null || newPassword.isEmpty()) {
            sendJson(exchange, 400, "{\"error\":\"参数不完整：需要 oldPassword 和 newPassword\"}");
            return;
        }

        if (newPassword.length() < 4) {
            sendJson(exchange, 400, "{\"error\":\"新密码长度不能少于 4 位\"}");
            return;
        }

        boolean ok = authManager.changePassword(username, oldPassword, newPassword);
        if (ok) {
            // 改密成功后使 session 失效，提示重新登录
            LOG.info("用户 '{}' 密码修改成功", username);
            sendJson(exchange, 200, "{\"success\":true,\"message\":\"密码已修改，请重新登录\"}");
        } else {
            sendJson(exchange, 400, "{\"error\":\"旧密码错误\"}");
        }
    }

    private void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] response = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }
}