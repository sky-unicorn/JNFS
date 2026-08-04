package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * MySQL 模式 V2 → V3 迁移步骤
 * <p>
 * 动作：
 * 1. CREATE TABLE IF NOT EXISTS replication_policy（同步策略配置）
 * 2. CREATE TABLE IF NOT EXISTS replication_control（对账控制信号，手动触发跨进程）
 * 3. INSERT IGNORE 种子行（id=1）
 * <p>
 * handlesOwnVersionWrite() 返回 false（与 MysqlV1ToV2 一致，DDL 隐式提交）。
 * 重入安全依赖幂等性（INV-3）：CREATE TABLE IF NOT EXISTS + INSERT IGNORE。
 * <p>
 * 失败拒绝启动（INV-4）：抛异常让 MigrationRunner 触发 System.exit(2)
 */
public class MysqlV2ToV3 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlV2ToV3.class);

    @Override
    public int fromVersion() {
        return 2;
    }

    @Override
    public int toVersion() {
        return 3;
    }

    @Override
    public boolean supports(StorageMode mode) {
        return mode == StorageMode.MYSQL;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        DataSource ds = ctx.dataSource();
        if (ds == null) {
            return "MySQL mode requires a DataSource";
        }

        try (Connection conn = ds.getConnection()) {
            // 1. 创建 replication_policy 表
            createReplicationPolicyTable(conn);

            // 2. 创建 replication_control 表
            createReplicationControlTable(conn);

            // 3. 插入种子行
            insertSeedRows(conn);

            LOG.info("MysqlV2ToV3: 迁移完成");
            return null;
        } catch (SQLException e) {
            LOG.error("MysqlV2ToV3: 迁移失败", e);
            return "MySQL migration V2→V3 failed: " + e.getMessage();
        }
    }

    private void createReplicationPolicyTable(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `replication_policy` ("
                        + "`id` TINYINT NOT NULL DEFAULT 1, "
                        + "`sync_window_start` VARCHAR(5) NOT NULL DEFAULT '01:00', "
                        + "`sync_window_end` VARCHAR(5) NOT NULL DEFAULT '03:00', "
                        + "`soft_deadline` VARCHAR(5) NOT NULL DEFAULT '03:00', "
                        + "`rate_limit_mbps` INT NOT NULL DEFAULT 50, "
                        + "`max_concurrency` INT NOT NULL DEFAULT 4, "
                        + "`updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`id`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步策略配置'"
        );
        LOG.info("MysqlV2ToV3: replication_policy 表已确保存在");
    }

    private void createReplicationControlTable(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `replication_control` ("
                        + "`id` TINYINT NOT NULL DEFAULT 1, "
                        + "`manual_sync_requested` TINYINT NOT NULL DEFAULT 0, "
                        + "`requested_at` DATETIME NULL, "
                        + "PRIMARY KEY (`id`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账控制信号'"
        );
        LOG.info("MysqlV2ToV3: replication_control 表已确保存在");
    }

    private void insertSeedRows(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "INSERT IGNORE INTO replication_policy (id) VALUES (1)");
        conn.createStatement().executeUpdate(
                "INSERT IGNORE INTO replication_control (id) VALUES (1)");
        LOG.info("MysqlV2ToV3: 种子行已插入");
    }
}
