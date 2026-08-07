package org.jnfs.registry.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jnfs.registry.StaticFileHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 登录页面和登录接口
 * <p>
 * GET  /login → 返回 SPA index.html（由 StaticFileHandler 提供，含 __JNFS_CONFIG__ 注入），
 *               pathname=/login → SPA 内渲染 LoginView
 * POST /login → 处理登录表单（application/x-www-form-urlencoded），成功设置 Cookie 并 302 跳 /；
 *               失败 302 重定向到 /login?error=...（SPA 读取查询参数展示错误，见 {@link #sendLoginPageWithError}）
 */
public class LoginHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LoginHandler.class);

    private final AuthManager authManager;
    private final StaticFileHandler staticHandler;

    /**
     * @param authManager    鉴权管理器
     * @param staticHandler  SPA 静态资源 handler（提供 index.html 渲染 + __JNFS_CONFIG__ 注入）
     */
    public LoginHandler(AuthManager authManager, StaticFileHandler staticHandler) {
        this.authManager = authManager;
        this.staticHandler = staticHandler;
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

    /**
     * GET /login → 返回 SPA index.html。
     * SPA 根据 pathname==='/login' && authEnabled 渲染 LoginView。
     */
    private void handleGet(HttpExchange exchange) throws IOException {
        staticHandler.serveIndex(exchange);
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

    /**
     * 登录失败：302 重定向回 {@code /login?error=...}，由 SPA 渲染 index.html 并展示错误。
     * <p>
     * 设计要点（见架构师设计 §3.3，保留表单提交方案）：
     * <ul>
     *   <li>POST /login 仍为表单刷新式（成功 302 → /，浏览器导航进 Dashboard）</li>
     *   <li>失败时浏览器收到 302 → GET /login?error=xxx → StaticFileHandler 返回 SPA index.html</li>
     *   <li>SPA LoginView 通过 {@code URLSearchParams(window.location.search).get('error')} 读取错误串，
     *       用 {@code message.error} 展示，无需后端在 index.html 中内嵌错误文案</li>
     * </ul>
     * error 入参为后端内部明文（非用户输入），URLEncoder 编码保证查询串合法。
     */
    private void sendLoginPageWithError(HttpExchange exchange, String error) throws IOException {
        String redirectUrl = "/login?error=" + java.net.URLEncoder.encode(error, StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Location", redirectUrl);
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
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
}
