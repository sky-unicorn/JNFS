package org.jnfs.registry.api;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * HTTP JSON 响应工具类（§10.1 复用，避免 12 个端点重复样板代码）。
 * <p>
 * 统一 UTF-8 编码 + {@code application/json; charset=UTF-8} Content-Type。
 */
public final class JsonHttpUtils {

    private JsonHttpUtils() {
    }

    /**
     * 发送 JSON 响应。
     *
     * @param exchange HTTP 交换
     * @param status   HTTP 状态码
     * @param json     JSON 字符串
     */
    public static void sendJson(HttpExchange exchange, int status, String json) throws IOException {
        byte[] response = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, response.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    /** 发送成功响应：{@code {"success":true,...}} 由调用方拼好 json */
    public static void sendSuccess(HttpExchange exchange, String json) throws IOException {
        sendJson(exchange, 200, json);
    }

    /** 发送错误响应：{@code {"success":false,"errors":[...]}} */
    public static void sendErrors(HttpExchange exchange, int status, String[] errors) throws IOException {
        StringBuilder sb = new StringBuilder("{\"success\":false,\"errors\":[");
        for (int i = 0; i < errors.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(escapeJson(errors[i])).append("\"");
        }
        sb.append("]}");
        sendJson(exchange, status, sb.toString());
    }

    /** 单条错误响应 */
    public static void sendError(HttpExchange exchange, int status, String error) throws IOException {
        sendErrors(exchange, status, new String[]{error});
    }

    /** JSON 字符串转义（\ 、"、控制字符 \n \r \t） */
    public static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * 读取请求体并返回字符串（UTF-8）。
     */
    public static String readBody(HttpExchange exchange) throws IOException {
        byte[] bytes = exchange.getRequestBody().readAllBytes();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 从路径中提取最后一段（路径参数）。
     * <p>
     * com.sun.net.httpserver 路径匹配是前缀式，需手动解析。
     * 例：{@code /api/replication/groups/grp-1} → {@code grp-1}
     */
    public static String lastSegment(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        if (path == null || path.isEmpty()) {
            return "";
        }
        // 去掉末尾斜杠
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        int idx = path.lastIndexOf('/');
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    /**
     * 解析查询串为 Map（重复参数取首个值；值做 URL 解码，null/空串归一为 null）。
     * 供分页/筛选类 GET 端点使用（com.sun.net.httpserver 无内建参数解析）。
     */
    public static java.util.Map<String, String> parseQuery(HttpExchange exchange) {
        java.util.Map<String, String> result = new java.util.HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return result;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                result.putIfAbsent(dec(pair), null);
                continue;
            }
            String key = dec(pair.substring(0, eq));
            String value = eq == pair.length() - 1 ? null : dec(pair.substring(eq + 1));
            if (value != null && value.isEmpty()) {
                value = null;
            }
            result.putIfAbsent(key, value);
        }
        return result;
    }

    private static String dec(String s) {
        try {
            return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }
}
