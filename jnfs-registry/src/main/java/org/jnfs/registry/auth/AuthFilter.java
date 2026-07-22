package org.jnfs.registry.auth;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Dashboard 鉴权过滤器
 * <p>
 * 对受保护路由校验 Cookie 中的 session token。
 * 鉴权通过 → 将 username 存入 exchange attribute 并继续处理链；
 * 未登录 → 302 跳转 /login 并关闭连接。
 * <p>
 * 使用方式：在 HttpContext 上 addFilter：
 * {@code ctx.getFilters().add(new AuthFilter(authManager));}
 */
public class AuthFilter extends Filter {

    private static final Logger LOG = LoggerFactory.getLogger(AuthFilter.class);

    public static final String ATTR_USERNAME = "username";
    private static final String LOGIN_PATH = "/login";

    private final AuthManager authManager;

    public AuthFilter(AuthManager authManager) {
        this.authManager = authManager;
    }

    @Override
    public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
        String token = extractTokenFromCookie(exchange);
        String username = authManager.validateSession(token);

        if (username != null) {
            // 鉴权通过，将用户名存入 attribute 供 handler 使用
            exchange.setAttribute(ATTR_USERNAME, username);
            chain.doFilter(exchange);
        } else {
            // 未登录 → 302 跳转登录页
            String path = exchange.getRequestURI().getPath();
            LOG.debug("未登录访问 {}，跳转 /login", path);
            exchange.getResponseHeaders().set("Location", LOGIN_PATH);
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        }
    }

    @Override
    public String description() {
        return "Dashboard Auth Filter";
    }

    // ==================== Cookie 工具方法（供其他 handler 复用） ====================

    /**
     * 从请求 Cookie 中提取 session token
     */
    public static String extractTokenFromCookie(HttpExchange exchange) {
        return extractCookieValue(exchange, AuthManager.getSessionCookieName());
    }

    /**
     * 按名称提取 Cookie 值
     */
    static String extractCookieValue(HttpExchange exchange, String cookieName) {
        List<String> cookies = exchange.getRequestHeaders().get("Cookie");
        if (cookies == null) {
            return null;
        }
        for (String cookieHeader : cookies) {
            String[] pairs = cookieHeader.split("; ");
            for (String pair : pairs) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2 && cookieName.equals(kv[0].trim())) {
                    return kv[1];
                }
            }
        }
        return null;
    }

    /**
     * 构建 Set-Cookie 响应头（用于登录）
     */
    public static String buildLoginCookie(String token, long maxAgeSeconds) {
        return String.format("%s=%s; Path=/; HttpOnly; Max-Age=%d",
                AuthManager.getSessionCookieName(), token, maxAgeSeconds);
    }

    /**
     * 构建清空 Cookie 的响应头（用于登出）
     */
    public static String buildLogoutCookie() {
        return AuthManager.getSessionCookieName() + "=; Path=/; HttpOnly; Max-Age=0";
    }
}