package org.jnfs.registry.auth;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 登出接口
 * <p>
 * GET /logout → 清除 session 并 302 跳转 /login
 */
public class LogoutHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LogoutHandler.class);

    private final AuthManager authManager;

    public LogoutHandler(AuthManager authManager) {
        this.authManager = authManager;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String token = AuthFilter.extractTokenFromCookie(exchange);
        authManager.logout(token);

        // 清除 Cookie
        exchange.getResponseHeaders().set("Set-Cookie", AuthFilter.buildLogoutCookie());
        exchange.getResponseHeaders().set("Location", "/login");
        exchange.sendResponseHeaders(302, -1);
        exchange.close();
    }
}