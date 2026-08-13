package org.jnfs.registry.api.dao;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 文件元数据查询 DAO（Dashboard「文件管理」页数据源）。
 * <p>
 * 直查与 NameNode 共享的元数据库（h2 / mysql 同库）：file_metadata / file_location。
 * 全部 SQL 仅用两方言共同支持的 ANSI 子集（LIMIT/OFFSET、EXISTS、LIKE ... ESCAPE '!'），
 * 无方言分支（见 `.rules/storage-compatibility.md`）。
 */
public class FileMetadataDao {

    private final DataSource dataSource;

    public FileMetadataDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // ==================== 分页查询 ====================

    /** 查询筛选条件（null = 不筛选） */
    public static final class Filter {
        public final String nodeId;     // datanode_id 精确匹配（兼容旧数据：datanode_addr 同值兜底由 handler 传入 nodeAddr）
        public final String nodeAddr;   // 与 nodeId 同批传入的 host:port，双列 OR 匹配旧数据
        public final String fileType;   // 类型标签（'unknown' 额外匹配 NULL，见 buildWhere）
        public final String storageId;  // 存储编号（storage_id 包含匹配，通配符转义）

        public Filter(String nodeId, String nodeAddr, String fileType, String storageId) {
            this.nodeId = nodeId;
            this.nodeAddr = nodeAddr;
            this.fileType = fileType;
            this.storageId = storageId;
        }
    }

    /** file_metadata 行（file_size 可 null；createTime 为 epoch 毫秒） */
    public static final class FileRow {
        public final String storageId;
        public final String filename;
        public final String fileHash;
        public final Long fileSize;
        public final String fileType;
        public final long createTime;
        public final int replicationFactor;

        FileRow(String storageId, String filename, String fileHash, Long fileSize,
                String fileType, long createTime, int replicationFactor) {
            this.storageId = storageId;
            this.filename = filename;
            this.fileHash = fileHash;
            this.fileSize = fileSize;
            this.fileType = fileType;
            this.createTime = createTime;
            this.replicationFactor = replicationFactor;
        }
    }

    /** 单条副本位置（nodeId 取 COALESCE(datanode_id, datanode_addr)；addr 为 datanode_addr 原值） */
    public static final class Replica {
        public final String fileHash;
        public final String nodeId;
        public final String addr;
        public final int role;
        public final int status;

        Replica(String fileHash, String nodeId, String addr, int role, int status) {
            this.fileHash = fileHash;
            this.nodeId = nodeId;
            this.addr = addr;
            this.role = role;
            this.status = status;
        }
    }

    public static final class Page {
        public final long total;
        public final List<FileRow> rows;

        Page(long total, List<FileRow> rows) {
            this.total = total;
            this.rows = rows;
        }
    }

    /**
     * 分页查询：总数 + 当前页行（create_time 倒序）。
     *
     * @param page     页码（1 起，调用方已钳制）
     * @param pageSize 页大小（1..200，调用方已钳制）
     */
    public Page queryFiles(Filter filter, int page, int pageSize) throws SQLException {
        Where where = buildWhere(filter);
        long total = queryTotal(where);
        List<FileRow> rows = new ArrayList<>(pageSize);
        String sql = "SELECT storage_id, filename, file_hash, file_size, file_type, "
                + "replication_factor, create_time FROM file_metadata m"
                + where.sql
                + " ORDER BY create_time DESC, storage_id ASC LIMIT " + pageSize
                + " OFFSET " + ((page - 1) * pageSize);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            bindWhere(stmt, where);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    rows.add(mapFileRow(rs));
                }
            }
        }
        return new Page(total, rows);
    }

    private long queryTotal(Where where) throws SQLException {
        String sql = "SELECT COUNT(*) FROM file_metadata m" + where.sql;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            bindWhere(stmt, where);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private static FileRow mapFileRow(ResultSet rs) throws SQLException {
        long size = rs.getLong("file_size");
        boolean sizeNull = rs.wasNull(); // 必须紧跟 getLong 取 null 状态（后续 getter 会覆盖）
        Timestamp createTime = rs.getTimestamp("create_time");
        return new FileRow(
                rs.getString("storage_id"),
                rs.getString("filename"),
                rs.getString("file_hash"),
                sizeNull ? null : size,
                rs.getString("file_type"),
                createTime != null ? createTime.getTime() : 0L,
                rs.getInt("replication_factor"));
    }

    /**
     * 批量查副本位置：file_hash → 副本列表（role ASC，PRIMARY 在前）。
     * 逐页 hash 二次 IN 查询组装，避免 GROUP_CONCAT 的方言差异。
     */
    public Map<String, List<Replica>> queryReplicas(List<String> hashes) throws SQLException {
        Map<String, List<Replica>> result = new HashMap<>();
        if (hashes.isEmpty()) {
            return result;
        }
        StringBuilder in = new StringBuilder();
        for (int i = 0; i < hashes.size(); i++) {
            if (i > 0) {
                in.append(",");
            }
            in.append("?");
        }
        String sql = "SELECT file_hash, datanode_id, datanode_addr, replica_role, status "
                + "FROM file_location WHERE file_hash IN (" + in + ") ORDER BY replica_role ASC";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < hashes.size(); i++) {
                stmt.setString(i + 1, hashes.get(i));
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String hash = rs.getString("file_hash");
                    String datanodeId = rs.getString("datanode_id");
                    String datanodeAddr = rs.getString("datanode_addr");
                    String nodeId = datanodeId != null ? datanodeId : datanodeAddr;
                    result.computeIfAbsent(hash, k -> new ArrayList<>())
                            .add(new Replica(hash, nodeId, datanodeAddr,
                                    rs.getInt("replica_role"), rs.getInt("status")));
                }
            }
        }
        return result;
    }

    /** 元数据库中实际存储过的类型标签（去重升序） */
    public List<String> distinctStoredTypes() throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = "SELECT DISTINCT file_type FROM file_metadata WHERE file_type IS NOT NULL ORDER BY file_type";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        }
        return result;
    }

    // ==================== WHERE 构建 ====================

    /**
     * WHERE 条件 + 参数双列表（两方言均支持的标准 SQL；LIKE 用 ESCAPE '!' 避免反斜杠方言差异）。
     */
    private Where buildWhere(Filter filter) {
        StringBuilder sql = new StringBuilder(" WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (filter.nodeId != null || filter.nodeAddr != null) {
            // 节点筛选：datanode_id 精确匹配，或 datanode_addr 等于节点当前 host:port（兼容旧数据未回填行）
            sql.append(" AND EXISTS (SELECT 1 FROM file_location l WHERE l.file_hash = m.file_hash");
            if (filter.nodeId != null && filter.nodeAddr != null) {
                sql.append(" AND (l.datanode_id = ? OR l.datanode_addr = ?)");
                params.add(filter.nodeId);
                params.add(filter.nodeAddr);
            } else if (filter.nodeId != null) {
                sql.append(" AND l.datanode_id = ?");
                params.add(filter.nodeId);
            } else {
                sql.append(" AND l.datanode_addr = ?");
                params.add(filter.nodeAddr);
            }
            sql.append(")");
        }

        if (filter.fileType != null) {
            if ("unknown".equalsIgnoreCase(filter.fileType)) {
                // 未知 = 显式存值 'unknown' ∪ 尚未回填的 NULL 行（NULL 多为无扩展名文件，
                // 可识别扩展名的旧行已由 V7 迁移或后台调度器回填）
                sql.append(" AND (m.file_type = 'unknown' OR m.file_type IS NULL)");
            } else {
                List<String> exts = org.jnfs.common.FileTypeDetector.extensionsOfType(filter.fileType);
                if (exts.isEmpty()) {
                    // 目录外类型（自定义存值）：仅精确匹配存储值
                    sql.append(" AND m.file_type = ?");
                    params.add(filter.fileType);
                } else {
                    // 存储值精确匹配 + 旧数据 NULL 行按扩展名兜底（大小写不敏感，扩展名来自内置目录、无注入风险）
                    sql.append(" AND (m.file_type = ? OR (m.file_type IS NULL AND (");
                    params.add(filter.fileType);
                    for (int i = 0; i < exts.size(); i++) {
                        if (i > 0) {
                            sql.append(" OR ");
                        }
                        sql.append("LOWER(m.filename) LIKE ?");
                        params.add("%." + exts.get(i));
                    }
                    sql.append(")))");
                }
            }
        }

        if (filter.storageId != null) {
            // 存储编号包含匹配（storage_id 可能含通配符，按字面转义）
            sql.append(" AND m.storage_id LIKE ? ESCAPE '!'");
            params.add(escapeLike(filter.storageId));
        }
        return new Where(sql.toString(), params);
    }

    /** LIKE 模式转义：! → !!，% → !%，_ → !_（配合 ESCAPE '!'） */
    static String escapeLike(String keyword) {
        StringBuilder sb = new StringBuilder(keyword.length() + 8);
        sb.append("%");
        for (int i = 0; i < keyword.length(); i++) {
            char c = keyword.charAt(i);
            if (c == '!' || c == '%' || c == '_') {
                sb.append('!');
            }
            sb.append(c);
        }
        sb.append("%");
        return sb.toString();
    }

    private void bindWhere(PreparedStatement stmt, Where where) throws SQLException {
        for (int i = 0; i < where.params.size(); i++) {
            stmt.setString(i + 1, (String) where.params.get(i));
        }
    }

    /** WHERE 子句 + 参数列表 */
    private static final class Where {
        final String sql;
        final List<Object> params;

        Where(String sql, List<Object> params) {
            this.sql = sql;
            this.params = params;
        }
    }
}
