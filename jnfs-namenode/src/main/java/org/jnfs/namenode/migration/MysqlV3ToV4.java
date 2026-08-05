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
 * MySQL 模式 V3 → V4 迁移步骤
 * <p>
 * 动作：CREATE TABLE IF NOT EXISTS node_drain（节点排空状态表，§6.1）
 * <p>
 * handlesOwnVersionWrite() 返回 false（与 MysqlV2ToV3 一致，DDL 隐式提交）。
 * 重入安全依赖幂等性（INV-3）：CREATE TABLE IF NOT EXISTS。
 * <p>
 * 失败拒绝启动（INV-4）：抛异常让 MigrationRunner 触发 System.exit(2)
 */
public class MysqlV3ToV4 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlV3ToV4.class);

    @Override
    public int fromVersion() {
        return 3;
    }

    @Override
    public int toVersion() {
        return 4;
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
            createNodeDrainTable(conn);
            LOG.info("MysqlV3ToV4: 迁移完成");
            return null;
        } catch (SQLException e) {
            LOG.error("MysqlV3ToV4: 迁移失败", e);
            return "MySQL migration V3→V4 failed: " + e.getMessage();
        }
    }

    private void createNodeDrainTable(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `node_drain` ("
                        + "`node_id` VARCHAR(128) NOT NULL COMMENT '节点ID（关联运行时节点，非外键）', "
                        + "`drain_status` TINYINT NOT NULL DEFAULT 0 COMMENT '0=ACTIVE, 1=DRAINING', "
                        + "`drain_at` DATETIME NULL DEFAULT NULL COMMENT 'DRAINING 置位时间（清除时置 NULL）', "
                        + "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`node_id`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='节点排空状态表'"
        );
        LOG.info("MysqlV3ToV4: node_drain 表已确保存在");
    }
}
