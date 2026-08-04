package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * MySQL 模式 V0 → V1 迁移步骤
 * <p>
 * 动作：
 * 1. CREATE TABLE IF NOT EXISTS node_registry（如果不存在）
 * 2. ALTER TABLE file_location 确保 datanode_id 字段存在（允许 NULL，过渡期）
 * 3. CREATE TABLE IF NOT EXISTS schema_version 并 INSERT 当前版本 1
 * <p>
 * 注意：
 * - 不执行 UPDATE ... SET datanode_id = ... 反查补全（反查不到，见设计文档 §4.9）
 * - datanode_id 的在线补全由 DataNode 心跳注册时触发（§4.9.2）
 * - 迁移 DDL 与 INSERT schema_version 在同一事务内（原子性）
 * <p>
 * 幂等性保证：
 * - CREATE TABLE IF NOT EXISTS 天然幂等
 * - ALTER TABLE ADD COLUMN IF NOT EXISTS（MySQL 不直接支持，用 information_schema 检查）
 * - INSERT schema_version 使用 INSERT IGNORE 避免重复
 */
public class MysqlV0ToV1 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlV0ToV1.class);

    @Override
    public int fromVersion() {
        return 0;
    }

    @Override
    public int toVersion() {
        return 1;
    }

    @Override
    public boolean supports(StorageMode mode) {
        return mode == StorageMode.MYSQL;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return true; // MySQL 模式下迁移 DDL 与版本号写入在同一事务内，保证原子性 (§4.6)
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        DataSource ds = ctx.dataSource();
        if (ds == null) {
            return "MySQL mode requires a DataSource";
        }

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 1. 确保 node_registry 表存在
                createNodeRegistryIfNotExists(conn);

                // 2. 确保 file_location.datanode_id 字段存在
                ensureDatanoIdColumn(conn);

                // 3. 确保 file_location.status 字段存在 (C3 根因修复：
                //    历史 file_location 由旧构造函数建表时漏建 status 列，与 jnfs.sql 不一致)
                ensureStatusColumn(conn);

                // 4. 创建 schema_version 表并写入版本号
                createSchemaVersionAndInsert(conn, 1);

                conn.commit();
                LOG.info("MysqlV0ToV1: 迁移完成");
                return null;
            } catch (SQLException e) {
                conn.rollback();
                LOG.error("MysqlV0ToV1: 迁移失败，已回滚", e);
                return "MySQL migration failed: " + e.getMessage();
            }
        }
    }

    private void createNodeRegistryIfNotExists(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `node_registry` ("
                        + "`node_id` VARCHAR(128) NOT NULL COMMENT '节点唯一标识', "
                        + "`node_type` VARCHAR(20) NOT NULL COMMENT '节点类型: DATANODE / NAMENODE', "
                        + "`host` VARCHAR(100) NOT NULL COMMENT '节点IP地址', "
                        + "`port` INT NOT NULL COMMENT '节点端口', "
                        + "`last_heartbeat` DATETIME NOT NULL COMMENT '最后心跳时间', "
                        + "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`node_id`), "
                        + "KEY `idx_type` (`node_type`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='节点注册表'"
        );
        LOG.info("MysqlV0ToV1: node_registry 表已确保存在");
    }

    private void ensureDatanoIdColumn(Connection conn) throws SQLException {
        // 检查 datanode_id 列是否已存在
        String checkSql = "SELECT COUNT(*) FROM information_schema.columns "
                + "WHERE table_schema = DATABASE() "
                + "AND table_name = 'file_location' "
                + "AND column_name = 'datanode_id'";

        try (PreparedStatement stmt = conn.prepareStatement(checkSql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("MysqlV0ToV1: file_location.datanode_id 列已存在，跳过 ALTER");
                return;
            }
        }

        // 添加 datanode_id 列（允许 NULL，过渡期）
        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `datanode_id` VARCHAR(128) DEFAULT NULL "
                        + "COMMENT 'DataNode节点ID (关联 node_registry.node_id)' "
                        + "AFTER `file_hash`"
        );

        // 添加索引（如果不存在）
        try {
            conn.createStatement().executeUpdate(
                    "ALTER TABLE `file_location` ADD INDEX `idx_node` (`datanode_id`)"
            );
        } catch (SQLException e) {
            // 索引可能已存在，忽略
            if (e.getErrorCode() != 1061) { // 1061 = Duplicate key name
                throw e;
            }
        }

        LOG.info("MysqlV0ToV1: file_location.datanode_id 列已添加");
    }

    private void ensureStatusColumn(Connection conn) throws SQLException {
        // 检查 status 列是否已存在
        String checkSql = "SELECT COUNT(*) FROM information_schema.columns "
                + "WHERE table_schema = DATABASE() "
                + "AND table_name = 'file_location' "
                + "AND column_name = 'status'";

        try (PreparedStatement stmt = conn.prepareStatement(checkSql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("MysqlV0ToV1: file_location.status 列已存在，跳过 ALTER");
                return;
            }
        }

        // 添加 status 列（NOT NULL DEFAULT 1，与 jnfs.sql 一致，避免 NULL 被 WHERE status=1 漏掉）
        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `status` TINYINT NOT NULL DEFAULT 1 COMMENT '状态: 1-正常, 0-损坏' "
                        + "AFTER `datanode_addr`"
        );

        LOG.info("MysqlV0ToV1: file_location.status 列已添加");
    }

    private void createSchemaVersionAndInsert(Connection conn, int version) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `schema_version` ("
                        + "`version` INT NOT NULL COMMENT '当前 schema 版本', "
                        + "`upgraded_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`version`)"
                        + ") ENGINE=InnoDB CHARACTER SET=utf8mb4 COMMENT='schema 版本记录'"
        );

        // INSERT IGNORE 保证幂等
        conn.createStatement().executeUpdate(
                "INSERT IGNORE INTO `schema_version` (version) VALUES (" + version + ")"
        );

        LOG.info("MysqlV0ToV1: schema_version 表已创建，版本号 {} 已写入", version);
    }
}
