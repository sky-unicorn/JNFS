package org.jnfs.registry.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jnfs.common.FileTypeDetector;
import org.jnfs.registry.RegistryHandler;
import org.jnfs.registry.api.dao.FileMetadataDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * 文件管理 API 处理器（Dashboard「文件管理」页）。
 * <p>
 * 路由（仅 GET）：
 * <ul>
 *   <li>{@code /api/files}：分页查询已上传文件，支持
 *       {@code page / pageSize / nodeId(存储节点) / fileType(文件类型) / storageId(存储编号)} 筛选；</li>
 *   <li>{@code /api/files/types}：类型下拉候选（元数据库 distinct file_type ∪ 内置扩展名目录）。</li>
 * </ul>
 * 数据源为与 NameNode 共享的元数据库（h2 / mysql 同库）；查询仅走 JDBC，
 * 不经过 NameNode RPC，不影响存储/下载链路。
 */
public class FilesApiHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FilesApiHandler.class);

    /** 分页默认值与上下界 */
    private static final int DEFAULT_PAGE = 1;
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 200;

    private final FileMetadataDao dao;

    public FilesApiHandler(javax.sql.DataSource dataSource) {
        this.dao = new FileMetadataDao(dataSource);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        try {
            if ("GET".equalsIgnoreCase(method) && path.equals("/api/files/types")) {
                handleTypes(exchange);
            } else if ("GET".equalsIgnoreCase(method)
                    && (path.equals("/api/files") || path.equals("/api/files/"))) {
                handleList(exchange);
            } else if (path.startsWith("/api/files")) {
                JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
            } else {
                JsonHttpUtils.sendError(exchange, 404, "Not Found");
            }
        } catch (SQLException e) {
            LOG.error("FilesApiHandler SQL 异常: path={}", path, e);
            JsonHttpUtils.sendError(exchange, 500, "Database error: " + e.getMessage());
        }
    }

    // ---- GET /api/files ----

    private void handleList(HttpExchange exchange) throws IOException, SQLException {
        Map<String, String> q = JsonHttpUtils.parseQuery(exchange);

        int page = parsePositiveInt(q.get("page"), DEFAULT_PAGE);
        int pageSize = Math.min(Math.max(parsePositiveInt(q.get("pageSize"), DEFAULT_PAGE_SIZE), 1), MAX_PAGE_SIZE);
        String nodeId = trimToNull(q.get("nodeId"));
        String fileType = trimToNull(q.get("fileType"));
        String storageId = trimToNull(q.get("storageId"));

        // 节点筛选的 host:port 兜底：datanode_id 精确匹配 + datanode_addr 同值匹配旧数据
        String nodeAddr = null;
        if (nodeId != null) {
            RegistryHandler.NodeInfo info = RegistryHandler.getDataNodes().get(nodeId);
            if (info != null && info.address != null && info.address.contains(":")) {
                nodeAddr = info.address;
            }
        }

        FileMetadataDao.Page pageResult = dao.queryFiles(
                new FileMetadataDao.Filter(nodeId, nodeAddr, fileType, storageId), page, pageSize);

        // 页面内 hash 批量查副本位置
        List<String> hashes = new java.util.ArrayList<>(pageResult.rows.size());
        for (FileMetadataDao.FileRow row : pageResult.rows) {
            hashes.add(row.fileHash);
        }
        Map<String, List<FileMetadataDao.Replica>> replicas = dao.queryReplicas(hashes);

        StringBuilder sb = new StringBuilder("{\"success\":true,")
                .append("\"total\":").append(pageResult.total).append(",")
                .append("\"page\":").append(page).append(",")
                .append("\"pageSize\":").append(pageSize).append(",")
                .append("\"files\":[");
        for (int i = 0; i < pageResult.rows.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            appendFileJson(sb, pageResult.rows.get(i), replicas.get(pageResult.rows.get(i).fileHash));
        }
        sb.append("]}");
        JsonHttpUtils.sendSuccess(exchange, sb.toString());
    }

    private void appendFileJson(StringBuilder sb, FileMetadataDao.FileRow row,
                                List<FileMetadataDao.Replica> reps) {
        // 展示类型：存储值优先；NULL 兜底扩展名推导（迁移回填后 NULL 仅剩无扩展名文件）
        String displayType = row.fileType != null ? row.fileType : FileTypeDetector.fromFilename(row.filename);

        sb.append("{\"storageId\":\"").append(JsonHttpUtils.escapeJson(row.storageId)).append("\",");
        sb.append("\"filename\":\"").append(JsonHttpUtils.escapeJson(row.filename)).append("\",");
        sb.append("\"fileHash\":\"").append(JsonHttpUtils.escapeJson(row.fileHash)).append("\",");
        sb.append("\"fileSize\":").append(row.fileSize != null ? row.fileSize : "null").append(",");
        sb.append("\"fileType\":").append(displayType != null ? "\"" + JsonHttpUtils.escapeJson(displayType) + "\"" : "null").append(",");
        sb.append("\"createTime\":").append(row.createTime).append(",");
        sb.append("\"replicationFactor\":").append(row.replicationFactor).append(",");
        sb.append("\"nodes\":[");
        if (reps != null) {
            for (int j = 0; j < reps.size(); j++) {
                if (j > 0) {
                    sb.append(",");
                }
                FileMetadataDao.Replica r = reps.get(j);
                sb.append("{\"nodeId\":\"").append(JsonHttpUtils.escapeJson(r.nodeId)).append("\",");
                sb.append("\"addr\":").append(r.addr != null ? "\"" + JsonHttpUtils.escapeJson(r.addr) + "\"" : "null").append(",");
                sb.append("\"role\":").append(r.role).append(",");
                sb.append("\"status\":").append(r.status).append("}");
            }
        }
        sb.append("]}");
    }

    // ---- GET /api/files/types ----

    private void handleTypes(HttpExchange exchange) throws IOException, SQLException {
        // 元数据库实际存值 ∪ 内置扩展名目录，去重升序；'unknown' 恒存在（NULL / unknown 存值行均属未知）
        TreeSet<String> types = new TreeSet<>(dao.distinctStoredTypes());
        types.addAll(FileTypeDetector.knownTypes());
        types.add("unknown");

        StringBuilder sb = new StringBuilder("{\"success\":true,\"types\":[");
        int i = 0;
        for (String type : types) {
            if (i++ > 0) {
                sb.append(",");
            }
            sb.append("\"").append(JsonHttpUtils.escapeJson(type)).append("\"");
        }
        sb.append("]}");
        JsonHttpUtils.sendSuccess(exchange, sb.toString());
    }

    // ---- 辅助 ----

    private static int parsePositiveInt(String s, int defaultValue) {
        if (s == null) {
            return defaultValue;
        }
        try {
            int v = Integer.parseInt(s.trim());
            return v > 0 ? v : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String trimmed = s.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
