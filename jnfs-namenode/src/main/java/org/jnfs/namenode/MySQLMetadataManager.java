package org.jnfs.namenode;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * MySQL 元数据管理器
 * 使用 JDBC 替换本地文件日志
 */
public class MySQLMetadataManager extends MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLMetadataManager.class);

    private final HikariDataSource dataSource;

    public MySQLMetadataManager(String host, int port, String dbName, String user, String password) {
        this(createDataSource(host, port, dbName, user, password));
    }

    /**
     * 使用已有的 DataSource 构造（迁移流程中先创建 DataSource，再传入）
     * <p>
     * 构造时执行 CREATE TABLE IF NOT EXISTS 确保全新部署时表结构完整。
     * 所有列定义与 {@code mysql/jnfs.sql} V2 schema 保持一致。
     * 存量升级场景由迁移框架（MysqlV0ToV1 + MysqlV1ToV2）负责补列/建表，
     * 此处 CREATE TABLE IF NOT EXISTS 对已存在的表/列无副作用（幂等）。
     */
    public MySQLMetadataManager(HikariDataSource dataSource) {
        this.dataSource = dataSource;

        // 确保表存在 (全新部署场景；存量升级由迁移框架负责)
        try (Connection conn = dataSource.getConnection()) {
            // file_metadata（含 V2 新增 replication_factor 列）
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `file_metadata` (" +
                "`storage_id` CHAR(36) NOT NULL," +
                "`filename` VARCHAR(255) NOT NULL," +
                "`file_hash` CHAR(64) NOT NULL," +
                "`file_size` BIGINT DEFAULT 0," +
                "`replication_factor` TINYINT NOT NULL DEFAULT 1 COMMENT '目标副本数；1=单副本，2/3=组内节点数'," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "`update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`storage_id`)," +
                "KEY `idx_hash` (`file_hash`)," +
                "KEY `idx_filename` (`filename`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            // node_registry
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `node_registry` (" +
                "`node_id` VARCHAR(128) NOT NULL," +
                "`node_type` VARCHAR(20) NOT NULL COMMENT 'DATANODE / NAMENODE'," +
                "`host` VARCHAR(100) NOT NULL," +
                "`port` INT NOT NULL," +
                "`last_heartbeat` DATETIME NOT NULL," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`node_id`)," +
                "KEY `idx_type` (`node_type`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            // file_location（含 V1 status 列 + V2 replica_role 列 + idx_hash_status 索引）
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `file_location` (" +
                "`id` BIGINT AUTO_INCREMENT PRIMARY KEY," +
                "`file_hash` CHAR(64) NOT NULL," +
                "`datanode_id` VARCHAR(128) DEFAULT NULL," +
                "`datanode_addr` VARCHAR(100) DEFAULT NULL," +
                "`status` TINYINT NOT NULL DEFAULT 1 COMMENT '状态: 1-正常(ACTIVE), 0-损坏(CORRUPT)'," +
                "`replica_role` TINYINT NOT NULL DEFAULT 0 COMMENT '0=PRIMARY,1=SECONDARY'," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "UNIQUE KEY `uk_hash_node` (`file_hash`, `datanode_id`)," +
                "INDEX `idx_node` (`datanode_id`)," +
                "INDEX `idx_hash_status` (`file_hash`, `status`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            // file_upload_lock
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `file_upload_lock` (" +
                "`file_hash` CHAR(64) NOT NULL," +
                "`namenode_id` VARCHAR(64) NOT NULL," +
                "`expire_time` DATETIME NOT NULL," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`file_hash`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            // replication_group（V2 新增）
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `replication_group` (" +
                "`group_id` VARCHAR(64) NOT NULL COMMENT '组ID'," +
                "`group_name` VARCHAR(128) NOT NULL COMMENT '组名'," +
                "`node_ids` VARCHAR(512) NOT NULL COMMENT '组成员node_id列表,逗号分隔(2~3个)'," +
                "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`group_id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冗余组配置表'"
            );
            // replica_sync_task（V2 新增）
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `replica_sync_task` (" +
                "`task_id` VARCHAR(64) NOT NULL COMMENT '任务ID'," +
                "`file_hash` CHAR(64) NOT NULL COMMENT '文件hash'," +
                "`source_node` VARCHAR(128) NOT NULL COMMENT '源节点(primary)'," +
                "`target_node` VARCHAR(128) NOT NULL COMMENT '目标节点'," +
                "`status` TINYINT NOT NULL DEFAULT 0 COMMENT '0=PENDING,1=IN_FLIGHT,2=DONE,3=FAILED'," +
                "`retry_count` TINYINT NOT NULL DEFAULT 0 COMMENT '累计失败次数(达4告警)'," +
                "`file_size` BIGINT NOT NULL DEFAULT 0 COMMENT '文件大小(字节,用于限速与超时)'," +
                "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`task_id`)," +
                "UNIQUE KEY `uk_hash_target` (`file_hash`, `target_node`)," +
                "INDEX `idx_status` (`status`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账同步任务表'"
            );
            // replication_policy（V3 新增，同步策略配置）
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `replication_policy` (" +
                "`id` TINYINT NOT NULL DEFAULT 1," +
                "`sync_window_start` VARCHAR(5) NOT NULL DEFAULT '01:00'," +
                "`sync_window_end` VARCHAR(5) NOT NULL DEFAULT '03:00'," +
                "`soft_deadline` VARCHAR(5) NOT NULL DEFAULT '03:00'," +
                "`rate_limit_mbps` INT NOT NULL DEFAULT 50," +
                "`max_concurrency` INT NOT NULL DEFAULT 4," +
                "`updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步策略配置'"
            );
            // replication_control（V3 新增，对账控制信号）
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `replication_control` (" +
                "`id` TINYINT NOT NULL DEFAULT 1," +
                "`manual_sync_requested` TINYINT NOT NULL DEFAULT 0," +
                "`requested_at` DATETIME NULL," +
                "PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账控制信号'"
            );
            // 种子行（INSERT IGNORE 幂等）
            conn.createStatement().execute("INSERT IGNORE INTO replication_policy (id) VALUES (1)");
            conn.createStatement().execute("INSERT IGNORE INTO replication_control (id) VALUES (1)");
        } catch (SQLException e) {
            // INV-4 精神：建表失败拒绝启动，避免带着不完整表继续运行
            LOG.error("创建数据库表失败，拒绝启动", e);
            throw new IllegalStateException("Failed to create database tables: " + e.getMessage(), e);
        }
    }

    private static HikariDataSource createDataSource(String host, int port, String dbName, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        config.setUsername(user);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        return new HikariDataSource(config);
    }

    /**
     * 获取内部 DataSource（供迁移流程复用）
     */
    public HikariDataSource getDataSource() {
        return dataSource;
    }

    /**
     * 按 hash 查询文件元数据（多副本）。
     * <p>
     * 查询该 hash 全部 file_location 行（多副本），按 replica_role ASC, status DESC 排序，
     * 组装成一个 MetadataEntry（per-file 字段取任一行，locations 为全部副本行）。
     * <p>
     * <b>返回 null 的两种情况</b>（调用方需注意，避免误用 null 判秒传）：
     * <ul>
     *   <li>file_metadata 不存在该 hash（真正的不存在）</li>
     *   <li>file_metadata 存在但无 file_location 副本行（孤儿 metadata）——使用 INNER JOIN，
     *       无副本行即视为不可读，返回 null（§8.3 副本未就绪场景）</li>
     * </ul>
     */
    @Override
    public MetadataCacheManager.MetadataEntry queryByHash(String hash) {
        String sql = "SELECT m.filename, m.file_hash, m.storage_id, " +
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
                List<ReplicaLocation> locations = new ArrayList<>();

                while (rs.next()) {
                    // per-file 字段：所有行相同，取第一行即可
                    if (filename == null) {
                        filename = rs.getString("filename");
                        fileHash = rs.getString("file_hash");
                        storageId = rs.getString("storage_id");
                    }
                    locations.add(new ReplicaLocation(
                            rs.getString("node_id"),
                            rs.getInt("replica_role"),
                            rs.getInt("status")
                    ));
                }

                if (filename != null) {
                    return new MetadataCacheManager.MetadataEntry(
                            filename, fileHash, storageId, locations);
                }
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 按Hash查询失败", e);
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
            LOG.error("[MySQLMetadataManager] 按StorageId查询Hash失败", e);
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
            LOG.error("[MySQLMetadataManager] 检查文件存在失败", e);
            return false;
        }
    }

    @Override
    public boolean tryAcquireUploadLock(String hash, String nodeId) {
        String deleteSql = "DELETE FROM file_upload_lock WHERE file_hash = ? AND expire_time < NOW()";
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
            // Duplicate entry error code for MySQL is 1062
            if (e.getErrorCode() == 1062) { 
                return false;
            }
            LOG.error("[MySQLMetadataManager] 获取锁失败", e);
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
            LOG.error("[MySQLMetadataManager] 释放锁失败", e);
        }
    }

    /**
     * 全量恢复到内存 maps。
     * <p>
     * <b>MySQL 模式实际不调用此方法</b>：{@link NameNodeHandler#initMetadataManager} 对 MySQL 模式
     * 走懒加载（cache miss → queryByHash），跳过全量 recover。此实现保留以满足基类契约，
     * 但若被调用，{@code hashToStorage} 单值 map 在多副本场景只会保留最后一条副本行（视为 primary）。
     * 多副本查询请走 {@link #queryByHash}（返回含全部副本的 MetadataEntry）。
     */
    @Override
    public void recover(Map<String, String> filenameToHash,
                        Map<String, String> hashToStorage,
                        Map<String, String> hashToId,
                        Set<String> persistedHashes) {
        LOG.info("[MySQLMetadataManager] 正在从数据库恢复元数据...");
        int count = 0;

        String sql = "SELECT m.filename, m.file_hash, m.storage_id, " +
                     "COALESCE(l.datanode_id, l.datanode_addr) AS address " +
                     "FROM file_metadata m " +
                     "JOIN file_location l ON m.file_hash = l.file_hash " +
                     "ORDER BY l.replica_role ASC, l.status DESC";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                String filename = rs.getString("filename");
                String hash = rs.getString("file_hash");
                String storageId = rs.getString("storage_id");
                String address = rs.getString("address");

                filenameToHash.put(filename, hash);
                // 多副本：ORDER BY 保证 primary 在前，putIfAbsent 保留 primary 行（避免被 secondary 覆盖）
                hashToStorage.putIfAbsent(hash, address);
                hashToId.putIfAbsent(hash, storageId);
                persistedHashes.add(hash);

                count++;
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 恢复失败", e);
        }

        LOG.info("[MySQLMetadataManager] 恢复完成，共加载 {} 条记录", count);
    }

    /**
     * 持久化文件元数据 + 全部副本位置（MySQL 模式多副本）。
     * <p>
     * 事务内：
     * 1. INSERT file_metadata（含 replication_factor 列）
     * 2. 为每个 location INSERT IGNORE 一行 file_location（file_hash, datanode_id, datanode_addr, status, replica_role）
     * 3. DELETE file_upload_lock
     * <p>
     * <b>契约约束</b>：本方法仅用于文件首次写入（INSERT IGNORE 去重已 ACTIVE 行）；
     * <b>不更新已存在行的 status/replica_role</b>。
     * 对账补齐（追加缺失副本行）或副本状态翻转（如 CORRUPT→ACTIVE）必须用
     * {@code INSERT ... ON DUPLICATE KEY UPDATE}，由 Phase 4/5 对账器实现，不要复用本方法。
     *
     * @param filename         文件名
     * @param hash             文件哈希
     * @param storageId        存储ID
     * @param replicationFactor 目标副本数
     * @param locations        全部副本位置（PRIMARY 行 role=0/status=1，成功 SECONDARY 行 role=1/status=1）
     */
    @Override
    public void logAddFile(String filename, String hash, String storageId,
                           int replicationFactor, List<ReplicaLocation> locations) throws java.io.IOException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 1. 插入 metadata（含 replication_factor）
                String sqlMeta = "INSERT INTO file_metadata (storage_id, filename, file_hash, replication_factor) VALUES (?, ?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sqlMeta)) {
                    stmt.setString(1, storageId);
                    stmt.setString(2, filename);
                    stmt.setString(3, hash);
                    stmt.setInt(4, replicationFactor);
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
                        String hostPort = org.jnfs.common.NodeAddressResolver.resolve(loc.getNodeId());
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
            LOG.error("[MySQLMetadataManager] 写入数据库失败", e);
            throw new java.io.IOException("Database persistence failed", e);
        }
    }

    /**
     * MySQL 模式: 在线补全 file_location.datanode_id
     * 对应设计文档 §4.9.2
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
    public int backfillDataNodeIds() {
        // 从 NodeAddressResolver 拿当前 host:port -> node_id 映射
        java.util.Map<String, String> addrToId = org.jnfs.common.NodeAddressResolver.getAddressToNodeIdSnapshot();
        if (addrToId.isEmpty()) {
            LOG.info("[MySQLMetadataManager] 当前无 host:port→node_id 映射,跳过在线补全");
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
                        LOG.info("[MySQLMetadataManager] 补全 datanode_addr={} → datanode_id={} ({} 条)",
                                address, nodeId, updated);
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
                    LOG.info("[MySQLMetadataManager] 在线补全完成: 本次补全 {} 条,剩余未补全 {} 条",
                            totalUpdated, remaining);
                }
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 在线补全 datanode_id 失败", e);
            return -1;
        }

        return totalUpdated;
    }
}
