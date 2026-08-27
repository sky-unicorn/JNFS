package org.jnfs.namenode;

import com.zaxxer.hikari.HikariDataSource;
import org.jnfs.common.NodeAddressResolver;
import org.jnfs.common.migration.JdbcDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JDBC 模式元数据管理器抽象基类（mysql / h2 共享 JDBC 实现）
 * <p>
 * 持有 {@link HikariDataSource} + {@link JdbcDialect}（构造注入），上提了原
 * {@code MySQLMetadataManager} 的全部 JDBC 逻辑：
 * queryByHash（多副本 JOIN）、queryHashByStorageId、isFileExist、tryAcquireUploadLock、
 * releaseUploadLock、logAddFile（事务）、backfillDataNodeIds、recover（懒加载 no-op）。
 * <p>
 * 子类只负责 createDataSource（各自 JDBC URL）+ 方言实例：
 * <ul>
 *   <li>{@link MySQLMetadataManager}：jdbc:mysql:...，MysqlDialect</li>
 *   <li>{@link H2MetadataManager}：jdbc:h2:file:...，H2Dialect</li>
 * </ul>
 * <p>
 * 锚点表 DDL：探针验证 H2 MariaDB 模式直接支持 mysql 全部 DDL（ENGINE/CHARSET/反引号/KEY/ON UPDATE
 * AUTO_INCREMENT），故 {@link #buildDdl()} 为具体方法产出共享的 file_metadata / file_location /
 * file_upload_lock 三张表 CREATE TABLE IF NOT EXISTS，构造时执行，零分支。
 */
public abstract class JdbcMetadataManager extends MetadataManager {

    /** 动态 logger：日志名前缀为具体子类名（MySQLMetadataManager / H2MetadataManager） */
    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final HikariDataSource dataSource;
    protected final JdbcDialect dialect;

    /**
     * 构造：注入数据源与方言，并执行锚点表 DDL（CREATE TABLE IF NOT EXISTS，幂等）。
     * <p>
     * 全新部署场景由迁移链建表；此处是兜底（对已存在的表/列无副作用）。
     * 建表失败（INV-4 精神）抛 IllegalStateException 拒绝启动。
     */
    protected JdbcMetadataManager(HikariDataSource dataSource, JdbcDialect dialect) {
        this.dataSource = dataSource;
        this.dialect = dialect;

        try (Connection conn = dataSource.getConnection()) {
            buildDdl(conn);
        } catch (SQLException e) {
            log.error("创建数据库表失败，拒绝启动", e);
            throw new IllegalStateException("Failed to create database tables: " + e.getMessage(), e);
        }
    }

    // ==================== 锚点表 DDL ====================

    /**
     * 锚点业务表 DDL（file_metadata / file_location / file_upload_lock）。
     * <p>
     * 单一来源：{@link #anchorTableDdl()}。构造时执行（CREATE TABLE IF NOT EXISTS，幂等），
     * 与迁移链 {@code MysqlV0ToV1#ensureBusinessAnchorTables} 共用同一份 DDL，避免 schema 漂移。
     * 探针验证 H2 MariaDB 模式零分支兼容（ENGINE/CHARSET/反引号/KEY/AFTER/ON UPDATE/AUTO_INCREMENT/COMMENT），
     * 故不按模式分支。
     */
    protected void buildDdl(Connection conn) throws SQLException {
        for (String ddl : anchorTableDdl()) {
            conn.createStatement().execute(ddl);
        }
    }

    /**
     * 三张锚点业务表的权威 DDL（单一来源）。
     * <p>
     * 被 {@link #buildDdl()}（NameNode 侧兜底建表）与 {@code MysqlV0ToV1#ensureBusinessAnchorTables}
     * （迁移链建表）共同引用，保证 fresh 部署与迁移升级产出完全一致的 schema。与
     * {@code mysql/jnfs.sql} 终态保持一致。
     */
    public static List<String> anchorTableDdl() {
        return List.of(
                // file_metadata（含 V2 新增 replication_factor 列、V7 新增 file_type 列）
                "CREATE TABLE IF NOT EXISTS `file_metadata` ("
                        + "`storage_id` CHAR(36) NOT NULL COMMENT '存储ID (UUID), 主键', "
                        + "`filename` VARCHAR(255) NOT NULL COMMENT '原始文件名', "
                        + "`file_hash` CHAR(64) NOT NULL COMMENT '文件哈希 (SHA-256)', "
                        + "`file_size` BIGINT DEFAULT NULL COMMENT '文件大小 (字节, NULL=未知)', "
                        + "`replication_factor` TINYINT NOT NULL DEFAULT 1 COMMENT '目标副本数；1=单副本，2/3=组内节点数', "
                        + "`file_type` VARCHAR(32) DEFAULT NULL COMMENT '文件类型标签(扩展名识别/Tika内容嗅探), NULL=未知', "
                        + "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间', "
                        + "`update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`storage_id`), "
                        + "KEY `idx_hash` (`file_hash`), "
                        + "KEY `idx_filename` (`filename`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文件元数据表'",
                // file_location（含 V1 status + V2 replica_role 列 + idx_node / idx_hash_status 索引）
                "CREATE TABLE IF NOT EXISTS `file_location` ("
                        + "`id` BIGINT NOT NULL AUTO_INCREMENT, "
                        + "`file_hash` CHAR(64) NOT NULL COMMENT '关联 file_metadata.file_hash', "
                        + "`datanode_id` VARCHAR(128) DEFAULT NULL COMMENT 'DataNode节点ID (关联 node_registry.node_id)', "
                        + "`datanode_addr` VARCHAR(100) DEFAULT NULL COMMENT 'DataNode地址 (host:port, 兼容旧数据)', "
                        + "`status` TINYINT NOT NULL DEFAULT 1 COMMENT '状态: 1-正常(ACTIVE), 0-损坏(CORRUPT)', "
                        + "`replica_role` TINYINT NOT NULL DEFAULT 0 COMMENT '0=PRIMARY,1=SECONDARY', "
                        + "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`id`), "
                        + "UNIQUE KEY `uk_hash_node` (`file_hash`, `datanode_id`), "
                        + "INDEX `idx_node` (`datanode_id`), "
                        + "INDEX `idx_hash_status` (`file_hash`, `status`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文件存储位置映射表'",
                // file_upload_lock
                "CREATE TABLE IF NOT EXISTS `file_upload_lock` ("
                        + "`file_hash` CHAR(64) NOT NULL COMMENT '锁Key：文件的Hash值', "
                        + "`namenode_id` VARCHAR(64) NOT NULL COMMENT '持有锁的服务节点标识', "
                        + "`expire_time` DATETIME NOT NULL COMMENT '锁过期时间(防止死锁)', "
                        + "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`file_hash`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文件上传分布式锁表'"
        );
    }

    // ==================== 能力探测 ====================

    @Override
    public boolean isJdbcBacked() {
        return true;
    }

    @Override
    public HikariDataSource getDataSource() {
        return dataSource;
    }

    // ==================== 查询 ====================

    /**
     * 按 hash 查询文件元数据（多副本 JOIN）。
     * <p>
     * 查询该 hash 全部 file_location 行（多副本），按 replica_role ASC, status DESC 排序，
     * 组装成一个 MetadataEntry（per-file 字段取任一行，locations 为全部副本行）。
     * <p>
     * <b>返回 null 的两种情况</b>：
     * <ul>
     *   <li>file_metadata 不存在该 hash（真正的不存在）</li>
     *   <li>file_metadata 存在但无 file_location 副本行（孤儿 metadata）——使用 INNER JOIN，
     *       无副本行即视为不可读，返回 null（§8.3 副本未就绪场景）</li>
     * </ul>
     */
    @Override
    public MetadataCacheManager.MetadataEntry queryByHash(String hash) {
        String sql = "SELECT m.filename, m.file_hash, m.storage_id, m.file_size, m.file_type, " +
                     "COALESCE(l.datanode_id, l.datanode_addr) AS node_id, " +
                     "l.replica_role, l.status " +
                     "FROM file_metadata m " +
                     "JOIN file_location l ON m.file_hash = l.file_hash " +
                     "WHERE m.file_hash = ? " +
                     "ORDER BY l.replica_role ASC, l.status DESC";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                String filename = null;
                String fileHash = null;
                String storageId = null;
                Long fileSize = null;
                String fileType = null;
                List<ReplicaLocation> locations = new ArrayList<>();

                while (rs.next()) {
                    // per-file 字段：所有行相同，取第一行即可
                    if (filename == null) {
                        filename = rs.getString("filename");
                        fileHash = rs.getString("file_hash");
                        storageId = rs.getString("storage_id");
                        long size = rs.getLong("file_size");
                        fileSize = rs.wasNull() ? null : size;
                        fileType = rs.getString("file_type");
                    }
                    locations.add(new ReplicaLocation(
                            rs.getString("node_id"),
                            rs.getInt("replica_role"),
                            rs.getInt("status")
                    ));
                }

                if (filename != null) {
                    return new MetadataCacheManager.MetadataEntry(
                            filename, fileHash, storageId, fileSize, fileType, locations);
                }
            }
        } catch (SQLException e) {
            log.error("[{}] 按Hash查询失败", getClass().getSimpleName(), e);
        }
        return null;
    }

    @Override
    public String queryHashByStorageId(String storageId) {
        String sql = "SELECT file_hash FROM file_metadata WHERE storage_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, storageId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("file_hash");
                }
            }
        } catch (SQLException e) {
            log.error("[{}] 按StorageId查询Hash失败", getClass().getSimpleName(), e);
        }
        return null;
    }

    @Override
    public boolean isFileExist(String hash) {
        String sql = "SELECT 1 FROM file_metadata WHERE file_hash = ? LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            log.error("[{}] 检查文件存在失败", getClass().getSimpleName(), e);
            return false;
        }
    }

    // ==================== 上传锁 ====================

    /**
     * 尝试获取文件上传锁（30 分钟过期）。
     * <p>
     * 重复键判定经 {@link JdbcDialect#isDuplicateKeyError}：
     * mysql=errorCode 1062 / h2=SQLState 23505，零分支。
     */
    @Override
    public boolean tryAcquireUploadLock(String hash, String nodeId) {
        String deleteSql = "DELETE FROM file_upload_lock WHERE file_hash = ? AND expire_time < "
                + dialect.nowLiteral();
        String insertSql = "INSERT INTO file_upload_lock (file_hash, namenode_id, expire_time) VALUES (?, ?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            // 1. 清理过期锁
            try (PreparedStatement stmt = conn.prepareStatement(deleteSql)) {
                stmt.setString(1, hash);
                stmt.executeUpdate();
            }

            // 2. 尝试获取锁 (30分钟过期)
            try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                stmt.setString(1, hash);
                stmt.setString(2, nodeId);
                stmt.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis() + 30 * 60 * 1000));
                stmt.executeUpdate();
                return true;
            }
        } catch (SQLException e) {
            // 唯一键冲突：mysql=1062 / h2=SQLState 23505
            if (dialect.isDuplicateKeyError(e)) {
                return false;
            }
            log.error("[{}] 获取锁失败", getClass().getSimpleName(), e);
            return false;
        }
    }

    @Override
    public void releaseUploadLock(String hash) {
        String sql = "DELETE FROM file_upload_lock WHERE file_hash = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("[{}] 释放锁失败", getClass().getSimpleName(), e);
        }
    }

    // ==================== 恢复 ====================

    /**
     * JDBC 模式懒加载 no-op：不批量灌内存，查询走 cache miss → {@link #queryByHash}。
     * 保留以满足基类契约；运行时不会被调用（{@link NameNodeHandler#initMetadataManager}
     * 对 isJdbcBacked()=true 走懒加载跳过 recover）。
     */
    @Override
    public void recover(Map<String, String> filenameToHash,
                        Map<String, String> hashToStorage,
                        Map<String, String> hashToId,
                        Set<String> persistedHashes) {
        log.info("[{}] JDBC 模式: 跳过全量内存恢复，启用懒加载（cache miss → queryByHash）",
                getClass().getSimpleName());
    }

    // ==================== 持久化 ====================

    /**
     * 持久化文件元数据 + 全部副本位置（事务）。
     * <p>
     * 事务内：
     * 1. INSERT file_metadata（含 replication_factor 列）
     * 2. 为每个 location INSERT IGNORE 一行 file_location（file_hash, datanode_id, datanode_addr, status, replica_role）
     * 3. DELETE file_upload_lock
     * <p>
     * <b>契约约束</b>：本方法仅用于文件首次写入（INSERT IGNORE 去重已 ACTIVE 行）；
     * <b>不更新已存在行的 status/replica_role</b>。
     * 对账补齐（追加缺失副本行）或副本状态翻转（如 CORRUPT→ACTIVE）必须用
     * {@code INSERT ... ON DUPLICATE KEY UPDATE}，由对账器实现，不要复用本方法。
     *
     * @throws IOException 数据库写入失败时包装抛出
     */
    @Override
    public void logAddFile(String filename, String hash, String storageId,
                           int replicationFactor, Long fileSize, String fileType,
                           List<ReplicaLocation> locations) throws IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 1. 插入 metadata（含 replication_factor / file_size / file_type 列）
                String sqlMeta = "INSERT INTO file_metadata (storage_id, filename, file_hash, "
                        + "file_size, file_type, replication_factor) VALUES (?, ?, ?, ?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sqlMeta)) {
                    stmt.setString(1, storageId);
                    stmt.setString(2, filename);
                    stmt.setString(3, hash);
                    if (fileSize == null) {
                        stmt.setNull(4, java.sql.Types.BIGINT);
                    } else {
                        stmt.setLong(4, fileSize);
                    }
                    if (fileType == null) {
                        stmt.setNull(5, java.sql.Types.VARCHAR);
                    } else {
                        stmt.setString(5, fileType);
                    }
                    stmt.setInt(6, replicationFactor);
                    stmt.executeUpdate();
                }

                // 2. 为每个副本位置插入 file_location 行
                String sqlLoc = "INSERT IGNORE INTO file_location " +
                        "(file_hash, datanode_id, datanode_addr, status, replica_role) VALUES (?, ?, ?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sqlLoc)) {
                    for (ReplicaLocation loc : locations) {
                        stmt.setString(1, hash);
                        stmt.setString(2, loc.getNodeId());
                        // datanode_addr 通过 NodeAddressResolver 解析为 host:port（兼容过渡）
                        String hostPort = NodeAddressResolver.resolve(loc.getNodeId());
                        stmt.setString(3, hostPort);
                        stmt.setInt(4, loc.getStatus());
                        stmt.setInt(5, loc.getRole());
                        stmt.executeUpdate();
                    }
                }

                // 3. 删除锁（确保原子性）
                String sqlUnlock = "DELETE FROM file_upload_lock WHERE file_hash = ?";
                try (PreparedStatement stmt = conn.prepareStatement(sqlUnlock)) {
                    stmt.setString(1, hash);
                    stmt.executeUpdate();
                }

                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("[{}] 写入数据库失败", getClass().getSimpleName(), e);
            throw new IOException("Database persistence failed", e);
        }
    }

    // ==================== 在线回填 ====================

    /**
     * JDBC 模式: 在线补全 file_location.datanode_id（设计文档 §4.9.2）。
     * <p>
     * 语义正确性：NameNode 从 Registry 拉取的 DataNode 列表 (node_id|host:port|freeSpace)
     * 证明了"该 node_id 现在就是这个 host:port"。因此 file_location 中所有
     * datanode_addr = 这个 host:port 且 datanode_id IS NULL 的记录,补上 node_id 是正确的。
     * <p>
     * IP 变更场景：
     * - DataNode 换 IP 后重启(同 node_id) → 新 IP 心跳补齐 datanode_addr=新IP 的记录
     * - 老节点永久下线,新机器接管同 IP → 新机器首次心跳用自己的新 node_id 补齐历史记录
     *
     * @return 被补全的记录数, -1 表示出错
     */
    @Override
    public int backfillDataNodeIds() {
        // 从 NodeAddressResolver 拿当前 host:port -> node_id 映射
        Map<String, String> addrToId = NodeAddressResolver.getAddressToNodeIdSnapshot();
        if (addrToId.isEmpty()) {
            log.info("[{}] 当前无 host:port→node_id 映射,跳过在线补全", getClass().getSimpleName());
            return 0;
        }

        String sql = "UPDATE file_location SET datanode_id = ? "
                + "WHERE datanode_addr = ? AND datanode_id IS NULL";
        int totalUpdated = 0;

        try (Connection conn = dataSource.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (Map.Entry<String, String> entry : addrToId.entrySet()) {
                    String address = entry.getKey();
                    String nodeId = entry.getValue();
                    // 跳过 address == nodeId 的情况(老数据 fallback)
                    if (address.equals(nodeId)) {
                        continue;
                    }
                    stmt.setString(1, nodeId);
                    stmt.setString(2, address);
                    int updated = stmt.executeUpdate();
                    if (updated > 0) {
                        log.info("[{}] 补全 datanode_addr={} → datanode_id={} ({} 条)",
                                getClass().getSimpleName(), address, nodeId, updated);
                        totalUpdated += updated;
                    }
                }
            }

            // 补全进度监控:剩余未补全的记录数
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT COUNT(*) FROM file_location WHERE datanode_id IS NULL");
                 ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int remaining = rs.getInt(1);
                    log.info("[{}] 在线补全完成: 本次补全 {} 条,剩余未补全 {} 条",
                            getClass().getSimpleName(), totalUpdated, remaining);
                }
            }
        } catch (SQLException e) {
            log.error("[{}] 在线补全 datanode_id 失败", getClass().getSimpleName(), e);
            return -1;
        }

        return totalUpdated;
    }
}