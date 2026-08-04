package org.jnfs.registry.api;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.jnfs.common.replication.ReplicationGroup;
import org.jnfs.registry.api.dao.ReplicationControlDao;
import org.jnfs.registry.api.dao.ReplicationGroupDao;
import org.jnfs.registry.api.dao.ReplicationPolicyDao;
import org.jnfs.registry.api.dao.ReplicaTaskDao;
import org.jnfs.registry.auth.AuthFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 冗余存储管理 API 处理器（§10.1，12 个端点）。
 * <p>
 * 路由分发基于 {@code exchange.getRequestURI().getPath()} 前缀匹配。
 * 所有写接口记录审计日志（§10.2）：{@code time|user|action|target|result}。
 * <p>
 * drain 状态内存维护（MVP 简化）：{@code drainedNodes} ConcurrentHashMap。
 * promote 直接 SQL（跨进程可用，NameNode cache 自然反映新 primary）。
 */
public class ReplicationApiHandler implements HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationApiHandler.class);

    private final ReplicationGroupDao groupDao;
    private final ReplicationPolicyDao policyDao;
    private final ReplicaTaskDao taskDao;
    private final ReplicationControlDao controlDao;
    private final DataSource metadataDataSource;

    /** drain 状态（MVP 内存维护，二期持久化） */
    private final ConcurrentHashMap<String, Boolean> drainedNodes = new ConcurrentHashMap<>();

    public ReplicationApiHandler(DataSource metadataDataSource) {
        this.metadataDataSource = metadataDataSource;
        this.groupDao = new ReplicationGroupDao(metadataDataSource);
        this.policyDao = new ReplicationPolicyDao(metadataDataSource);
        this.taskDao = new ReplicaTaskDao(metadataDataSource);
        this.controlDao = new ReplicationControlDao(metadataDataSource);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();
        String username = (String) exchange.getAttribute(AuthFilter.ATTR_USERNAME);

        try {
            // 路由分发
            if (path.startsWith("/api/replication/groups/")) {
                // /api/replication/groups/{id}
                String groupId = path.substring("/api/replication/groups/".length());
                if (groupId.endsWith("/")) groupId = groupId.substring(0, groupId.length() - 1);
                if ("PUT".equalsIgnoreCase(method)) {
                    handleUpdateGroup(exchange, groupId, username);
                } else if ("DELETE".equalsIgnoreCase(method)) {
                    handleDeleteGroup(exchange, groupId, username);
                } else {
                    JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
                }
            } else if (path.equals("/api/replication/groups")) {
                if ("GET".equalsIgnoreCase(method)) {
                    handleListGroups(exchange);
                } else if ("POST".equalsIgnoreCase(method)) {
                    handleCreateGroup(exchange, username);
                } else {
                    JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
                }
            } else if (path.startsWith("/api/nodes/") && path.contains("/drain")) {
                String nodeId = extractNodeId(path, "/api/nodes/", "/drain");
                handleDrain(exchange, nodeId, username);
            } else if (path.startsWith("/api/nodes/") && path.contains("/promote")) {
                String nodeId = extractNodeId(path, "/api/nodes/", "/promote");
                handlePromote(exchange, nodeId, username);
            } else if (path.equals("/api/replication/policy")) {
                if ("GET".equalsIgnoreCase(method)) {
                    handleGetPolicy(exchange);
                } else if ("PUT".equalsIgnoreCase(method)) {
                    handlePutPolicy(exchange, username);
                } else {
                    JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
                }
            } else if (path.startsWith("/api/replication/sync/retry/")) {
                String taskId = path.substring("/api/replication/sync/retry/".length());
                if (taskId.endsWith("/")) taskId = taskId.substring(0, taskId.length() - 1);
                handleRetryTask(exchange, taskId, username);
            } else if (path.equals("/api/replication/sync")) {
                if ("GET".equalsIgnoreCase(method)) {
                    handleGetSync(exchange);
                } else if ("POST".equalsIgnoreCase(method)) {
                    handleTriggerSync(exchange, username);
                } else {
                    JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
                }
            } else if (path.equals("/api/replication/alerts")) {
                handleGetAlerts(exchange);
            } else {
                JsonHttpUtils.sendError(exchange, 404, "Not Found");
            }
        } catch (SQLException e) {
            LOG.error("ReplicationApiHandler SQL 异常: path={}", path, e);
            JsonHttpUtils.sendError(exchange, 500, "Database error: " + e.getMessage());
        }
    }

    // ---- /api/replication/groups GET ----
    private void handleListGroups(HttpExchange exchange) throws IOException, SQLException {
        List<ReplicationGroup> groups = groupDao.listAll();
        StringBuilder sb = new StringBuilder("{\"groups\":[");
        for (int i = 0; i < groups.size(); i++) {
            if (i > 0) sb.append(",");
            ReplicationGroup g = groups.get(i);
            sb.append("{\"groupId\":\"").append(JsonHttpUtils.escapeJson(g.getGroupId())).append("\",");
            sb.append("\"groupName\":\"").append(JsonHttpUtils.escapeJson(g.getGroupName())).append("\",");
            sb.append("\"nodeIds\":[");
            List<String> ids = g.getNodeIds();
            for (int j = 0; j < ids.size(); j++) {
                if (j > 0) sb.append(",");
                sb.append("\"").append(JsonHttpUtils.escapeJson(ids.get(j))).append("\"");
            }
            sb.append("],\"createTime\":").append(g.getCreateTime()).append(",\"updateTime\":").append(g.getUpdateTime());
            sb.append("}");
        }
        sb.append("]}");
        JsonHttpUtils.sendSuccess(exchange, sb.toString());
    }

    // ---- /api/replication/groups POST ----
    private void handleCreateGroup(HttpExchange exchange, String username) throws IOException, SQLException {
        String body = JsonHttpUtils.readBody(exchange);
        // 简易 JSON 解析：提取 groupId 和 nodeIds
        String groupId = extractJsonString(body, "groupId");
        String groupName = extractJsonString(body, "groupName");
        List<String> nodeIds = extractJsonArray(body, "nodeIds");

        List<String> errors = validateGroup(groupId, nodeIds, null);
        if (!errors.isEmpty()) {
            JsonHttpUtils.sendErrors(exchange, 400, errors.toArray(new String[0]));
            return;
        }

        ReplicationGroup g = new ReplicationGroup();
        g.setGroupId(groupId);
        g.setGroupName(groupName != null ? groupName : "");
        g.setNodeIds(nodeIds);
        groupDao.insert(g);
        audit(username, "CREATE_GROUP", groupId, "OK");
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true,\"group\":{\"groupId\":\"" + JsonHttpUtils.escapeJson(groupId) + "\"}}");
    }

    // ---- /api/replication/groups/{id} PUT ----
    private void handleUpdateGroup(HttpExchange exchange, String groupId, String username) throws IOException, SQLException {
        String body = JsonHttpUtils.readBody(exchange);
        List<String> nodeIds = extractJsonArray(body, "nodeIds");
        String groupName = extractJsonString(body, "groupName");

        List<String> errors = validateGroup(groupId, nodeIds, groupId);
        if (!errors.isEmpty()) {
            JsonHttpUtils.sendErrors(exchange, 400, errors.toArray(new String[0]));
            return;
        }

        groupDao.update(groupId, nodeIds, groupName != null ? groupName : "");
        audit(username, "UPDATE_GROUP", groupId, "OK");
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true}");
    }

    // ---- /api/replication/groups/{id} DELETE ----
    private void handleDeleteGroup(HttpExchange exchange, String groupId, String username) throws IOException, SQLException {
        boolean deleted = groupDao.delete(groupId);
        if (!deleted) {
            JsonHttpUtils.sendError(exchange, 404, "Group not found: " + groupId);
            return;
        }
        audit(username, "DELETE_GROUP", groupId, "OK");
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true}");
    }

    // ---- /api/nodes/{id}/drain POST ----
    private void handleDrain(HttpExchange exchange, String nodeId, String username) throws IOException {
        // S3：仅接受 POST，防 GET 误触发清空 drain 状态
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
            return;
        }
        String body = JsonHttpUtils.readBody(exchange);
        // S7：用正则容忍键值间空白，避免 {"drain":  true} 变体失效误判为 clear
        boolean drain = extractJsonBool(body, "drain", false);
        if (drain) {
            drainedNodes.put(nodeId, true);
        } else {
            drainedNodes.remove(nodeId);
        }
        audit(username, "DRAIN", nodeId, drain ? "SET" : "CLEAR");
        // S4：drain 仅 Registry 内存（NameNode selectReplicaTargets 不排除 drained 节点）
        //    响应 message 注明 "marked, not yet enforced"，二期持久化到 node_registry + discovery 排除
        String msg = drain ? "Node marked as drained (marked, not yet enforced at upload selection)"
                           : "Node drain cleared";
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true,\"message\":\"" + msg + "\"}");
    }

    // ---- /api/nodes/{id}/promote POST ----
    private void handlePromote(HttpExchange exchange, String nodeId, String username) throws IOException, SQLException {
        // S3：仅接受 POST，防 GET 误触发
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            JsonHttpUtils.sendError(exchange, 405, "Method Not Allowed");
            return;
        }
        // I1 修复：promote 必须 demote 旧 primary，否则同 file_hash 出现多 primary
        // → reconcile findPrimaryNode 可能选到死节点 → markFailed → 告警卡死。
        // 2 步事务（picky 方案 A）：
        //   1) 提升目标节点的 secondary(role=1) → primary(role=0)
        //   2) 把目标节点作为 primary 的 file_hash 在其他节点上的旧 primary 降为 secondary（保证单 primary 不变式）
        String promoteSql = "UPDATE file_location SET replica_role = 0 WHERE datanode_id = ? AND replica_role = 1";
        String demoteSql = "UPDATE file_location SET replica_role = 1" +
                " WHERE file_hash IN (SELECT file_hash FROM (SELECT file_hash FROM file_location WHERE datanode_id = ? AND replica_role = 0) t)" +
                " AND datanode_id <> ? AND replica_role = 0";

        int promoted;
        int demoted;
        try (java.sql.Connection conn = metadataDataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (java.sql.PreparedStatement promote = conn.prepareStatement(promoteSql)) {
                    promote.setString(1, nodeId);
                    promoted = promote.executeUpdate();
                }
                try (java.sql.PreparedStatement demote = conn.prepareStatement(demoteSql)) {
                    demote.setString(1, nodeId);
                    demote.setString(2, nodeId);
                    demoted = demote.executeUpdate();
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                // 还原 autoCommit，避免归还连接池后污染后续借用方
                conn.setAutoCommit(true);
            }
        }
        audit(username, "PROMOTE", nodeId, "promoted=" + promoted + ",demoted=" + demoted);
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true,\"message\":\"Promoted " + promoted
                + " replicas to PRIMARY, demoted " + demoted + " old primaries to SECONDARY\"}");
    }

    // ---- /api/replication/policy GET ----
    private void handleGetPolicy(HttpExchange exchange) throws IOException, SQLException {
        ReplicationPolicyDao.Policy p = policyDao.get();
        String json = "{\"syncWindow\":{\"start\":\"" + JsonHttpUtils.escapeJson(p.syncWindowStart) + "\"," +
                "\"end\":\"" + JsonHttpUtils.escapeJson(p.syncWindowEnd) + "\"}," +
                "\"softDeadline\":\"" + JsonHttpUtils.escapeJson(p.softDeadline) + "\"," +
                "\"rateLimitMbps\":" + p.rateLimitMbps + "," +
                "\"maxConcurrency\":" + p.maxConcurrency + "}";
        JsonHttpUtils.sendSuccess(exchange, json);
    }

    // ---- /api/replication/policy PUT ----
    private void handlePutPolicy(HttpExchange exchange, String username) throws IOException, SQLException {
        String body = JsonHttpUtils.readBody(exchange);
        ReplicationPolicyDao.Policy p = new ReplicationPolicyDao.Policy();
        p.syncWindowStart = extractJsonString(body, "start");
        p.syncWindowEnd = extractJsonString(body, "end");
        p.softDeadline = extractJsonString(body, "softDeadline");
        p.rateLimitMbps = extractJsonInt(body, "rateLimitMbps", 50);
        p.maxConcurrency = extractJsonInt(body, "maxConcurrency", 4);
        // 默认值兜底
        if (p.syncWindowStart == null) p.syncWindowStart = "01:00";
        if (p.syncWindowEnd == null) p.syncWindowEnd = "03:00";
        if (p.softDeadline == null) p.softDeadline = "03:00";
        policyDao.update(p);
        audit(username, "PUT_POLICY", "", "OK");
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true}");
    }

    // ---- /api/replication/sync GET ----
    private void handleGetSync(HttpExchange exchange) throws IOException, SQLException {
        ReplicaTaskDao.Summary s = taskDao.summary();
        List<org.jnfs.common.replication.ReplicaSyncTask> failed = taskDao.listFailed();
        List<org.jnfs.common.replication.ReplicaSyncTask> alerts = taskDao.listAlerts();

        StringBuilder sb = new StringBuilder("{\"summary\":{\"totalPending\":").append(s.totalPending)
                .append(",\"syncedToday\":").append(s.syncedToday)
                .append(",\"failed\":").append(s.failed)
                .append(",\"currentJobs\":").append(s.currentJobs).append("},")
                .append("\"failedJobs\":").append(tasksToJson(failed))
                .append(",\"alerts\":").append(tasksToJson(alerts))
                .append("}");
        JsonHttpUtils.sendSuccess(exchange, sb.toString());
    }

    // ---- /api/replication/sync POST (手动触发) ----
    private void handleTriggerSync(HttpExchange exchange, String username) throws IOException, SQLException {
        controlDao.requestManualSync();
        audit(username, "SYNC_TRIGGER", "", "OK");
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true,\"message\":\"Manual sync requested\"}");
    }

    // ---- /api/replication/sync/retry/{taskId} POST ----
    private void handleRetryTask(HttpExchange exchange, String taskId, String username) throws IOException, SQLException {
        boolean ok = taskDao.resetRetryCount(taskId);
        if (!ok) {
            JsonHttpUtils.sendError(exchange, 404, "Task not found: " + taskId);
            return;
        }
        audit(username, "RETRY", taskId, "OK");
        JsonHttpUtils.sendSuccess(exchange, "{\"success\":true}");
    }

    // ---- /api/replication/alerts GET ----
    private void handleGetAlerts(HttpExchange exchange) throws IOException, SQLException {
        List<org.jnfs.common.replication.ReplicaSyncTask> active = taskDao.listAlerts();
        StringBuilder sb = new StringBuilder("{\"active\":").append(tasksToJson(active)).append(",\"resolved\":[]}");
        JsonHttpUtils.sendSuccess(exchange, sb.toString());
    }

    // ---- 辅助方法 ----

    private List<String> validateGroup(String groupId, List<String> nodeIds, String excludeGroupId) throws SQLException {
        List<String> errors = new ArrayList<>();
        if (groupId == null || groupId.isEmpty()) {
            errors.add("groupId is required");
        }
        if (nodeIds == null || nodeIds.size() < 2) {
            errors.add("At least 2 nodes required");
        }
        if (nodeIds != null && nodeIds.size() > 3) {
            errors.add("At most 3 nodes allowed");
        }
        // 重叠校验：节点不能属于其他组
        if (nodeIds != null && !nodeIds.isEmpty()) {
            List<ReplicationGroup> allGroups = groupDao.listAll();
            for (ReplicationGroup g : allGroups) {
                if (excludeGroupId != null && excludeGroupId.equals(g.getGroupId())) {
                    continue; // 排除自身
                }
                for (String nid : nodeIds) {
                    if (g.getNodeIds().contains(nid)) {
                        errors.add("Node " + nid + " already in group " + g.getGroupId());
                    }
                }
            }
        }
        // 同主机告警（§16.6 校验规则：同 host 仅警告不阻止）
        if (nodeIds != null && nodeIds.size() >= 2) {
            Map<String, org.jnfs.registry.RegistryHandler.NodeInfo> allNodes =
                    org.jnfs.registry.RegistryHandler.getDataNodes();
            Map<String, List<String>> hostToNodes = new java.util.HashMap<>();
            for (String nid : nodeIds) {
                org.jnfs.registry.RegistryHandler.NodeInfo info = allNodes.get(nid);
                if (info != null && info.address != null) {
                    String host = info.address.split(":")[0];
                    hostToNodes.computeIfAbsent(host, k -> new ArrayList<>()).add(nid);
                }
            }
            for (Map.Entry<String, List<String>> entry : hostToNodes.entrySet()) {
                if (entry.getValue().size() > 1) {
                    LOG.warn("ReplicationApiHandler: 同主机告警 - 节点 {} 位于同一主机 ({})",
                            String.join(", ", entry.getValue()), entry.getKey());
                }
            }
        }
        return errors;
    }

    private String extractNodeId(String path, String prefix, String suffix) {
        int start = path.indexOf(prefix) + prefix.length();
        int end = path.indexOf(suffix, start);
        if (end < 0) end = path.length();
        return path.substring(start, end);
    }

    /** 简易 JSON 字符串提取（无 JSON 库，手写最小解析） */
    private String extractJsonString(String json, String key) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) return null;
        // 找冒号后的值
        int colon = json.indexOf(':', idx + pattern.length());
        if (colon < 0) return null;
        // 跳过空白
        int i = colon + 1;
        while (i < json.length() && json.charAt(i) == ' ') i++;
        if (i >= json.length()) return null;
        if (json.charAt(i) == '"') {
            // 字符串值
            int end = json.indexOf('"', i + 1);
            if (end < 0) return null;
            return json.substring(i + 1, end);
        }
        return null;
    }

    /** 简易 JSON 数组提取 */
    private List<String> extractJsonArray(String json, String key) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) return Collections.emptyList();
        int colon = json.indexOf(':', idx + pattern.length());
        if (colon < 0) return Collections.emptyList();
        int i = colon + 1;
        while (i < json.length() && json.charAt(i) == ' ') i++;
        if (i >= json.length() || json.charAt(i) != '[') return Collections.emptyList();
        int end = json.indexOf(']', i);
        if (end < 0) return Collections.emptyList();
        String arrStr = json.substring(i + 1, end);
        List<String> result = new ArrayList<>();
        for (String part : arrStr.split(",")) {
            part = part.trim();
            if (part.startsWith("\"") && part.endsWith("\"") && part.length() >= 2) {
                result.add(part.substring(1, part.length() - 1));
            }
        }
        return result;
    }

    /** 简易 JSON int 提取（S2：NFE 防御，畸形/溢出数字返回默认值，不抛 500） */
    private int extractJsonInt(String json, String key, int defaultVal) {
        String pattern = "\"" + key + "\"";
        int idx = json.indexOf(pattern);
        if (idx < 0) return defaultVal;
        int colon = json.indexOf(':', idx + pattern.length());
        if (colon < 0) return defaultVal;
        int i = colon + 1;
        while (i < json.length() && json.charAt(i) == ' ') i++;
        int end = i;
        while (end < json.length() && Character.isDigit(json.charAt(end))) end++;
        if (end == i) return defaultVal;
        try {
            return Integer.parseInt(json.substring(i, end));
        } catch (NumberFormatException e) {
            return defaultVal;
        }
    }

    /** 简易 JSON 布尔提取（S7：正则容忍键值间空白，匹配 "key"\s*:\s*(true|false)） */
    private boolean extractJsonBool(String json, String key, boolean defaultVal) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                "\"" + key + "\"\\s*:\\s*(true|false)").matcher(json);
        if (m.find()) {
            return "true".equals(m.group(1));
        }
        return defaultVal;
    }

    /** 任务列表 → JSON 数组 */
    private String tasksToJson(List<org.jnfs.common.replication.ReplicaSyncTask> tasks) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < tasks.size(); i++) {
            if (i > 0) sb.append(",");
            org.jnfs.common.replication.ReplicaSyncTask t = tasks.get(i);
            sb.append("{\"taskId\":\"").append(JsonHttpUtils.escapeJson(t.getTaskId())).append("\",");
            sb.append("\"fileHash\":\"").append(JsonHttpUtils.escapeJson(t.getFileHash())).append("\",");
            sb.append("\"sourceNode\":\"").append(JsonHttpUtils.escapeJson(t.getSourceNode())).append("\",");
            sb.append("\"targetNode\":\"").append(JsonHttpUtils.escapeJson(t.getTargetNode())).append("\",");
            sb.append("\"retryCount\":").append(t.getRetryCount()).append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    /** 审计日志（§10.2：结构化日志 time|user|action|target|result） */
    private void audit(String user, String action, String target, String result) {
        LOG.info("{}|{}|{}|{}|{}", System.currentTimeMillis(), user, action, target, result);
    }
}
