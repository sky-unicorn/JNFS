package org.jnfs.registry.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 登录页面和登录接口
 * <p>
 * GET  /login → 返回登录页 HTML
 * POST /login → 处理登录表单（application/x-www-form-urlencoded），成功设置 Cookie 并 302 跳 /
 */
public class LoginHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LoginHandler.class);

    private final AuthManager authManager;

    public LoginHandler(AuthManager authManager) {
        this.authManager = authManager;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if ("GET".equalsIgnoreCase(method)) {
            handleGet(exchange);
        } else if ("POST".equalsIgnoreCase(method)) {
            handlePost(exchange);
        } else {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
        }
    }

    private void handleGet(HttpExchange exchange) throws IOException {
        String html = generateLoginPage(null);
        byte[] response = html.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(200, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private void handlePost(HttpExchange exchange) throws IOException {
        Map<String, String> params = parseFormData(exchange.getRequestBody());
        String username = params.get("username");
        String password = params.get("password");

        if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
            sendLoginPageWithError(exchange, "用户名和密码不能为空");
            return;
        }

        String token = authManager.login(username, password);
        if (token != null) {
            // 登录成功：设置 Cookie（HttpOnly），302 跳转首页
            String cookie = AuthFilter.buildLoginCookie(token, authManager.getSessionTimeoutSeconds());
            exchange.getResponseHeaders().set("Set-Cookie", cookie);
            exchange.getResponseHeaders().set("Location", "/");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
            LOG.info("用户 '{}' 登录成功，已设置 session cookie", username);
        } else {
            sendLoginPageWithError(exchange, "用户名或密码错误");
        }
    }

    private void sendLoginPageWithError(HttpExchange exchange, String error) throws IOException {
        String html = generateLoginPage(error);
        byte[] response = html.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.sendResponseHeaders(401, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    /**
     * 解析 application/x-www-form-urlencoded 表单
     */
    static Map<String, String> parseFormData(InputStream is) throws IOException {
        String body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        Map<String, String> params = new HashMap<>();
        for (String pair : body.split("&")) {
            if (pair.isEmpty()) continue;
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                params.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                           URLDecoder.decode(kv[1], StandardCharsets.UTF_8));
            }
        }
        return params;
    }

    /**
     * 生成登录页 HTML
     */
    private String generateLoginPage(String error) {
        StringBuilder sb = new StringBuilder();
        sb.append("<!DOCTYPE html>\n");
        sb.append("<html lang=\"zh-CN\">\n");
        sb.append("<head>\n");
        sb.append("    <meta charset=\"UTF-8\">\n");
        sb.append("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
        sb.append("    <title>JNFS 运维监控中心 - 登录</title>\n");
        sb.append("    <style>\n");
        sb.append("        * { box-sizing: border-box; margin: 0; padding: 0; }\n");
        sb.append("        body {\n");
        sb.append("            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;\n");
        sb.append("            background: linear-gradient(135deg, #3498db 0%, #2c3e50 100%);\n");
        sb.append("            display: flex; justify-content: center; align-items: center;\n");
        sb.append("            height: 100vh; margin: 0;\n");
        sb.append("        }\n");
        sb.append("        .login-container {\n");
        sb.append("            background: #fff; padding: 2.5rem; border-radius: 10px;\n");
        sb.append("            box-shadow: 0 8px 24px rgba(0,0,0,0.2); width: 100%; max-width: 400px;\n");
        sb.append("        }\n");
        sb.append("        .login-container h1 {\n");
        sb.append("            margin: 0 0 0.5rem 0; font-size: 1.4rem; color: #2c3e50; text-align: center;\n");
        sb.append("        }\n");
        sb.append("        .login-container .subtitle {\n");
        sb.append("            text-align: center; color: #95a5a6; font-size: 0.85rem;\n");
        sb.append("            margin-bottom: 1.5rem;\n");
        sb.append("        }\n");
        sb.append("        .error {\n");
        sb.append("            background: #fdf2f2; color: #c0392b; padding: 0.75rem;\n");
        sb.append("            border-radius: 6px; margin-bottom: 1rem; font-size: 0.9rem;\n");
        sb.append("            border: 1px solid #f5c6cb;\n");
        sb.append("        }\n");
        sb.append("        .form-group { margin-bottom: 1rem; }\n");
        sb.append("        .form-group label {\n");
        sb.append("            display: block; margin-bottom: 0.4rem;\n");
        sb.append("            font-size: 0.85rem; color: #555; font-weight: 500;\n");
        sb.append("        }\n");
        sb.append("        .form-group input {\n");
        sb.append("            width: 100%; padding: 0.7rem 0.8rem; border: 1px solid #ddd;\n");
        sb.append("            border-radius: 6px; font-size: 1rem;\n");
        sb.append("            transition: border-color 0.2s;\n");
        sb.append("        }\n");
        sb.append("        .form-group input:focus {\n");
        sb.append("            outline: none; border-color: #3498db; box-shadow: 0 0 0 3px rgba(52,152,219,0.15);\n");
        sb.append("        }\n");
        sb.append("        button {\n");
        sb.append("            width: 100%; padding: 0.75rem; background: #3498db; color: #fff;\n");
        sb.append("            border: none; border-radius: 6px; font-size: 1rem;\n");
        sb.append("            font-weight: 600; cursor: pointer; transition: background 0.2s;\n");
        sb.append("            margin-top: 0.5rem;\n");
        sb.append("        }\n");
        sb.append("        button:hover { background: #2980b9; }\n");
        sb.append("    </style>\n");
        sb.append("</head>\n");
        sb.append("<body>\n");
        sb.append("    <div class=\"login-container\">\n");
        sb.append("        <h1>JNFS 运维监控中心</h1>\n");
        sb.append("        <p class=\"subtitle\">请登录以访问 Dashboard</p>\n");

        if (error != null && !error.isEmpty()) {
            sb.append("        <div class=\"error\">").append(escapeHtml(error)).append("</div>\n");
        }

        sb.append("        <form method=\"POST\" action=\"/login\">\n");
        sb.append("            <div class=\"form-group\">\n");
        sb.append("                <label for=\"username\">用户名</label>\n");
        sb.append("                <input type=\"text\" id=\"username\" name=\"username\" placeholder=\"请输入用户名\" required autofocus>\n");
        sb.append("            </div>\n");
        sb.append("            <div class=\"form-group\">\n");
        sb.append("                <label for=\"password\">密码</label>\n");
        sb.append("                <input type=\"password\" id=\"password\" name=\"password\" placeholder=\"请输入密码\" required>\n");
        sb.append("            </div>\n");
        sb.append("            <button type=\"submit\">登录</button>\n");
        sb.append("        </form>\n");
        sb.append("    </div>\n");
        sb.append("</body>\n");
        sb.append("</html>");
        return sb.toString();
    }

    private static String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}