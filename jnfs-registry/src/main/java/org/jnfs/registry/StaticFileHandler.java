package org.jnfs.registry;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 从 classpath 'static/' 提供 SPA 静态资源（Vue 构建产物）。
 * <p>
 * 部署形态：前端 {@code jnfs-dashboard-ui} 模块构建后，{@code dist/} 内容经
 * maven-resources-plugin 复制到 {@code target/classes/static/}，最终并入 fat-jar
 * 顶部的 {@code static/}。运行期由本 handler 通过
 * {@code getResourceAsStream("/static/...")} 读取。
 * <p>
 * 职责：
 * <ul>
 *   <li>静态资源（js/css/png/字体等）：从 classpath 'static/{path}' 读取，区分大小写</li>
 *   <li>index.html：从 'static/index.html' 读取，注入 {@code __JNFS_CONFIG__} 配置 JSON 后返回</li>
 *   <li>缓存：/assets/* 下的 vite 产物带 hash → immutable 长缓存；index.html → no-cache</li>
 *   <li>路径穿越防御：拒绝 {@code ..}、反斜杠、NUL；规范化后须仍位于 static/ 前缀内</li>
 * </ul>
 * <p>
 * 线程安全：仅持有两个 final 字段（storageMode、authEnabled），MIME 表为不可变 Map，
 * 可被多个 HttpContext 共享。
 */
public class StaticFileHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(StaticFileHandler.class);

    /** classpath 静态资源根前缀（jar 内顶部 static/） */
    private static final String STATIC_PREFIX = "static/";
    /**
     * index.html 占位符容错正则：前端预置 {@code window.__JNFS_CONFIG__ = null;}，返回前替换 null 为配置 JSON 字面量。
     * 兼容 vite 改写引入的空白/分号差异（精确字符串替换会静默失效）。
     */
    private static final Pattern CONFIG_PLACEHOLDER =
            Pattern.compile("window\\.__JNFS_CONFIG__\\s*=\\s*null\\s*;");
    /** /assets/* 下 vite 产物（含 hash）→ 长缓存不可变 */
    private static final String CACHE_CONTROL_ASSETS = "public, max-age=31536000, immutable";
    /** index.html 每次需重新注入配置，禁止缓存 */
    private static final String CACHE_CONTROL_INDEX = "no-cache";
    /** 默认 MIME（未知后缀） */
    private static final String DEFAULT_MIME = "application/octet-stream";

    /** MIME 表（key 为小写后缀，含点号）。未知后缀回退到 {@link #DEFAULT_MIME} */
    private static final Map<String, String> MIME_TYPES = Map.ofEntries(
            Map.entry(".html", "text/html; charset=UTF-8"),
            Map.entry(".htm", "text/html; charset=UTF-8"),
            Map.entry(".js", "text/javascript; charset=UTF-8"),
            Map.entry(".mjs", "text/javascript; charset=UTF-8"),
            Map.entry(".css", "text/css; charset=UTF-8"),
            Map.entry(".json", "application/json; charset=UTF-8"),
            Map.entry(".svg", "image/svg+xml"),
            Map.entry(".png", "image/png"),
            Map.entry(".jpg", "image/jpeg"),
            Map.entry(".jpeg", "image/jpeg"),
            Map.entry(".gif", "image/gif"),
            Map.entry(".ico", "image/x-icon"),
            Map.entry(".webp", "image/webp"),
            Map.entry(".woff", "font/woff"),
            Map.entry(".woff2", "font/woff2"),
            Map.entry(".ttf", "font/ttf"),
            Map.entry(".otf", "font/otf"),
            Map.entry(".eot", "application/vnd.ms-fontobject"),
            Map.entry(".map", "application/json; charset=UTF-8"),
            Map.entry(".txt", "text/plain; charset=UTF-8"),
            Map.entry(".webmanifest", "application/manifest+json; charset=UTF-8")
    );

    private final String storageMode;
    private final boolean authEnabled;

    /**
     * 构造一个可复用的静态资源 handler。
     *
     * @param storageMode 顶层 storage.mode（file | h2 | mysql），用于注入 SPA 配置；null/未知值视为 file
     * @param authEnabled 是否启用鉴权（由是否传入 AuthManager 决定），注入到 SPA 配置
     */
    public StaticFileHandler(String storageMode, boolean authEnabled) {
        this.storageMode = normalizeStorageMode(storageMode);
        this.authEnabled = authEnabled;
    }

    /**
     * 通用入口：服务 /assets/* 下的任意静态资源。
     * 兜底：请求路径为空或根时回退到 index.html（SPA 入口）。
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String rawPath = exchange.getRequestURI().getPath();
        if (rawPath == null || rawPath.isEmpty() || rawPath.equals("/")) {
            serveIndex(exchange);
            return;
        }
        // 第一道：粗暴拒绝明显的穿越标记（URL 已被 HttpServer 解码）
        if (!isSafeRelativePath(rawPath)) {
            LOG.debug("StaticFileHandler: 拒绝可疑路径 {}", rawPath);
            sendStatusWithoutBody(exchange, 403);
            return;
        }
        // 去掉前导斜杠，拼到 static/ 下
        String resourcePath = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
        // 第二道：规范化后须仍位于 static/ 前缀内（双重保险）
        if (!isUnderStaticPrefix(resourcePath)) {
            LOG.debug("StaticFileHandler: 规范化后越界 {}", rawPath);
            sendStatusWithoutBody(exchange, 403);
            return;
        }
        serveStaticResource(exchange, resourcePath, rawPath);
    }

    /**
     * 渲染 SPA 入口 index.html（注入 {@code __JNFS_CONFIG__} 后返回）。
     * <p>
     * 用于 {@code /} 与 {@code /login} 两个路由（是否受 AuthFilter 保护由调用方决定）。
     * Cache-Control 固定为 no-cache，因为每次都需根据 storageMode/authEnabled 重新注入配置。
     * <p>
     * 容错：若 classpath 缺失 index.html（前端未构建），返回 503 + 提示，避免空响应导致 SPA 白屏。
     */
    public void serveIndex(HttpExchange exchange) throws IOException {
        String indexHtml = readClasspathText(STATIC_PREFIX + "index.html");
        if (indexHtml == null) {
            String msg = "SPA index.html not found on classpath '/static/index.html'. "
                    + "Run 'mvn generate-resources' (jnfs-dashboard-ui) to build the frontend.";
            LOG.warn("serveIndex: {}", msg);
            byte[] body = msg.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.getResponseHeaders().set("Cache-Control", CACHE_CONTROL_INDEX);
            exchange.sendResponseHeaders(503, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
            return;
        }
        // 注入配置（占位符由前端预置）。用容错正则匹配，避免 vite 改写缩进/分号后精确替换静默失效。
        String rendered = injectConfig(indexHtml);
        byte[] body = rendered.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        exchange.getResponseHeaders().set("Cache-Control", CACHE_CONTROL_INDEX);
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    /**
     * 服务一个普通静态资源（js/css/png/字体 等）。
     * 找不到返回 404（无 body）。
     */
    private void serveStaticResource(HttpExchange exchange, String resourcePath, String requestPath) throws IOException {
        byte[] body = readClasspathBytes(STATIC_PREFIX + resourcePath);
        if (body == null) {
            LOG.debug("StaticFileHandler: 资源不存在 /{}", resourcePath);
            sendStatusWithoutBody(exchange, 404);
            return;
        }
        String mime = lookupMime(resourcePath);
        exchange.getResponseHeaders().set("Content-Type", mime);
        // /assets/* 下 vite 产物带 hash → immutable 长缓存；其他静态资源保守走 no-cache
        if (isAssetsPath(requestPath)) {
            exchange.getResponseHeaders().set("Cache-Control", CACHE_CONTROL_ASSETS);
        } else {
            exchange.getResponseHeaders().set("Cache-Control", CACHE_CONTROL_INDEX);
        }
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    // ==================== 路径穿越防御 ====================

    /**
     * 第一道校验：拒绝明显的穿越字符。
     * HttpExchange.getRequestURI().getPath() 已做 URL 解码，因此 {@code ..} 已展开为字面值。
     */
    private static boolean isSafeRelativePath(String path) {
        if (path == null) return false;
        if (path.contains("..")) return false;        // 父目录引用
        if (path.indexOf('\\') >= 0) return false;    // Windows 路径分隔符
        if (path.indexOf('\0') >= 0) return false;    // NUL 注入
        return true;
    }

    /**
     * 第二道校验：去掉前导 '/' 后，规范化路径须仍位于 static/ 前缀内（非绝对、非空、无穿越）。
     */
    private static boolean isUnderStaticPrefix(String resourcePath) {
        if (resourcePath == null || resourcePath.isEmpty()) return false;
        if (resourcePath.startsWith("/") || resourcePath.startsWith("\\")) return false;
        if (resourcePath.contains("..") || resourcePath.indexOf('\\') >= 0) return false;
        // 折叠多个连续斜杠后须保持等价（不引入新越界）
        String normalized = resourcePath.replaceAll("/+", "/");
        return normalized.equals(resourcePath);
    }

    private static boolean isAssetsPath(String requestPath) {
        return requestPath != null
                && (requestPath.startsWith("/assets/") || requestPath.equals("/assets"));
    }

    // ==================== MIME / 缓存 / 配置 ====================

    private static String lookupMime(String resourcePath) {
        int slash = resourcePath.lastIndexOf('/');
        String file = slash >= 0 ? resourcePath.substring(slash + 1) : resourcePath;
        int dot = file.lastIndexOf('.');
        if (dot < 0 || dot == file.length() - 1) return DEFAULT_MIME;
        String ext = file.substring(dot).toLowerCase(Locale.ROOT);
        return MIME_TYPES.getOrDefault(ext, DEFAULT_MIME);
    }

    /**
     * 将运行期配置 JSON 注入 index.html 的 {@code __JNFS_CONFIG__} 占位符。
     * <p>
     * 容错策略（防止 vite 改写 index.html 后精确替换静默失效，导致 SPA 读到 null -> client.js
     * fallback 误判为无鉴权、隐藏登录入口）：
     * <ol>
     *   <li>用正则 {@link #CONFIG_PLACEHOLDER} 容错匹配空白/分号差异后替换为配置字面量；</li>
     *   <li>替换后再次检测：若结果既仍含 {@code = null;} 占位（未命中），又不含已注入的配置对象
     *       （{@code __JNFS_CONFIG__ = \{}），说明占位符格式已漂移 -> 记 warn 并在 <head> 起始处
     *       追加一段 {@code <script>} 注入默认配置，确保 SPA 不会读到 null。</li>
     * </ol>
     * 注：默认配置仍按当前 storageMode/authEnabled 计算（与正常路径一致），并非安全降级值，
     * 因此即便走兜底分支也不会引入鉴权误判。
     *
     * @param indexHtml classpath 读取的原始 index.html 文本
     * @return 已注入配置的 index.html 文本（至少保证 {@code __JNFS_CONFIG__} 非 null）
     */
    private String injectConfig(String indexHtml) {
        String configLiteral = "window.__JNFS_CONFIG__ = " + buildConfigJson() + ";";
        String rendered = CONFIG_PLACEHOLDER.matcher(indexHtml).replaceAll(
                java.util.regex.Matcher.quoteReplacement(configLiteral));
        // 命中检测：占位符仍在 且 未出现已注入的对象字面量 -> 占位符漂移，走兜底
        if (CONFIG_PLACEHOLDER.matcher(rendered).find()
                && !rendered.contains("window.__JNFS_CONFIG__ = {")) {
            LOG.warn("injectConfig: index.html 占位符 'window.__JNFS_CONFIG__ = null;' 未命中"
                    + "（疑似 vite 改写格式漂移），已在 <head> 追加默认配置兜底，请核对前端模板。");
            String fallbackScript = "<script>window.__JNFS_CONFIG__ = "
                    + buildConfigJson() + ";</script>";
            int headIdx = rendered.indexOf("<head>");
            if (headIdx >= 0) {
                rendered = rendered.substring(0, headIdx + "<head>".length())
                        + fallbackScript + rendered.substring(headIdx + "<head>".length());
            } else {
                rendered = fallbackScript + rendered;
            }
        }
        return rendered;
    }

    /**
     * 构造注入到 SPA 的配置 JSON 字面量（不含外层括号）。
     * <ul>
     *   <li>storageMode：白名单 file/h2/mysql，null/其他 → file</li>
     *   <li>noRedundancy：仅 file 模式为 true（file 已退役，防御性保留）；h2/mysql 均 false（冗余可用）</li>
     *   <li>authEnabled：由是否传入 AuthManager 决定</li>
     * </ul>
     */
    private String buildConfigJson() {
        boolean noRedundancy = "file".equals(storageMode);
        return "{\"storageMode\":\"" + storageMode + "\","
                + "\"noRedundancy\":" + noRedundancy + ","
                + "\"authEnabled\":" + authEnabled + "}";
    }

    /** 规范化 storageMode，保证只输出白名单值（避免 XSS / 注入） */
    private static String normalizeStorageMode(String mode) {
        if (mode == null) return "file";
        if ("mysql".equalsIgnoreCase(mode)) return "mysql";
        if ("h2".equalsIgnoreCase(mode)) return "h2";
        if ("file".equalsIgnoreCase(mode)) return "file";
        return "file";
    }

    // ==================== classpath 读写工具 ====================

    private static String readClasspathText(String path) {
        try (InputStream in = StaticFileHandler.class.getResourceAsStream("/" + path)) {
            if (in == null) return null;
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.warn("读取 classpath 文本资源失败: {}", path, e);
            return null;
        }
    }

    private static byte[] readClasspathBytes(String path) {
        try (InputStream in = StaticFileHandler.class.getResourceAsStream("/" + path)) {
            if (in == null) return null;
            return in.readAllBytes();
        } catch (IOException e) {
            LOG.warn("读取 classpath 二进制资源失败: {}", path, e);
            return null;
        }
    }

    private static void sendStatusWithoutBody(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }
}
