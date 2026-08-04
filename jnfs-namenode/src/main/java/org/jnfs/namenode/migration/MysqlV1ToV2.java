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
 * MySQL 模式 V1 → V2 迁移步骤
 * <p>
 * 动作：
 * 1. ALTER TABLE file_location ADD COLUMN replica_role（幂等，information_schema 检查）
 * 2. ALTER TABLE file_metadata ADD COLUMN replication_factor（幂等）
 * 3. ALTER TABLE file_location ADD INDEX idx_hash_status（幂等）
 * 4. CREATE TABLE IF NOT EXISTS replication_group
 * 5. CREATE TABLE IF NOT EXISTS replica_sync_task
 * 6. ALTER TABLE file_location 确保 status 列存在（幂等，修复部分 V1 库缺列）
 * <p>
 * 版本号写入：{@code handlesOwnVersionWrite()} 返回 false，由 {@link org.jnfs.common.migration.MigrationRunner}
 * 在本步骤成功后用其单行 UPDATE/INSERT 路径（writeMysqlVersion）写入版本号 2。
 * <p>
 * 原子性说明：MySQL DDL（ALTER/CREATE TABLE）触发隐式提交，{@code conn.setAutoCommit(false)}
 * 对 DDL 无效，DDL 无法与版本号写入置于同一事务。重入安全依赖幂等性（INV-3）：
 * 每条 DDL 先查 information_schema 判断列/索引/表是否存在，重复执行无副作用；
 * 版本号由 Runner 单点写入，DDL 已全部应用后才写版本，崩溃重入会重新执行幂等 DDL 再写版本。
 * <p>
 * 存量行语义：
 * - replica_role 默认 0（PRIMARY），replication_factor 默认 1（单副本）
 * - 不强制回填 datanode_id NULL 行（V0→V1 node_id 回填是在线异步的，升级瞬间仍有 NULL）
 * <p>
 * 失败拒绝启动（INV-4）：抛异常让 MigrationRunner 触发 System.exit(2)
 */
public class MysqlV1ToV2 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlV1ToV2.class);

    @Override
    public int fromVersion() {
        return 1;
    }

    @Override
    public int toVersion() {
        return 2;
    }

    @Override
    public boolean supports(StorageMode mode) {
        return mode == StorageMode.MYSQL;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        // MySQL DDL 触发隐式提交，无法与版本号写入置于同一事务（原子性声明不成立）；
        // 改由 MigrationRunner 在步骤成功后单点写版本号（writeMysqlVersion），
        // 与 FileV1ToV2 模式一致，统一版本号写入模式。重入安全依赖幂等性（INV-3）。
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        DataSource ds = ctx.dataSource();
        if (ds == null) {
            return "MySQL mode requires a DataSource";
        }

        try (Connection conn = ds.getConnection()) {
            // 注意：MySQL DDL 隐式提交，setAutoCommit(false) 对 DDL 无效。
            // 不再使用事务包装，避免误导。重入安全依赖幂等性（INV-3）。

            // 0. 前置依赖：确保 file_location.status 列存在。
            //    修复历史部分 V1 库（schema_version=1 但 file_location 缺 status 列，
            //    由早期建表漏建 + V0→V1 版本号已推进导致 ensureStatusColumn 不再执行）。
            //    本步骤 AFTER status 与 idx_hash_status 索引均依赖该列，故在此幂等补齐。
            ensureStatusColumn(conn);

            // 1. file_location 增加 replica_role 列
            addReplicaRoleColumn(conn);

            // 2. file_metadata 增加 replication_factor 列
            addReplicationFactorColumn(conn);

            // 3. file_location 增加 idx_hash_status 索引
            addHashStatusIndex(conn);

            // 4. 创建 replication_group 表
            createReplicationGroupTable(conn);

            // 5. 创建 replica_sync_task 表
            createReplicaSyncTaskTable(conn);

            // 版本号写入由 MigrationRunner 在本步骤返回成功后执行（writeMysqlVersion）

            LOG.info("MysqlV1ToV2: 迁移完成");
            return null;
        } catch (SQLException e) {
            LOG.error("MysqlV1ToV2: 迁移失败", e);
            return "MySQL migration V1→V2 failed: " + e.getMessage();
        }
    }

    /**
     * 幂等确保 file_location.status 列存在。
     * <p>
     * 修复历史部分 V1 库：早期建表漏建 status 列，schema_version 已推进到 1，
     * 导致 V0->V1 的 ensureStatusColumn 不再执行。本步骤的 AFTER status 与
     * idx_hash_status 索引依赖该列，故在此幂等补齐（information_schema 检查）。
     */
    private void ensureStatusColumn(Connection conn) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM information_schema.columns "
                + "WHERE table_schema = DATABASE() "
                + "AND table_name = 'file_location' "
                + "AND column_name = 'status'";

        try (PreparedStatement stmt = conn.prepareStatement(checkSql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("MysqlV1ToV2: file_location.status 列已存在，跳过 ALTER");
                return;
            }
        }

        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `status` TINYINT NOT NULL DEFAULT 1 "
                        + "COMMENT '状态: 1-正常, 0-损坏' "
                        + "AFTER `datanode_addr`"
        );
        LOG.info("MysqlV1ToV2: file_location.status 列已补齐");
    }

    private void addReplicaRoleColumn(Connection conn) throws SQLException {
        // 检查 replica_role 列是否已存在
        String checkSql = "SELECT COUNT(*) FROM information_schema.columns "
                + "WHERE table_schema = DATABASE() "
                + "AND table_name = 'file_location' "
                + "AND column_name = 'replica_role'";

        try (PreparedStatement stmt = conn.prepareStatement(checkSql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("MysqlV1ToV2: file_location.replica_role 列已存在，跳过 ALTER");
                return;
            }
        }

        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `replica_role` TINYINT NOT NULL DEFAULT 0 "
                        + "COMMENT '0=PRIMARY,1=SECONDARY' "
                        + "AFTER `status`"
        );
        LOG.info("MysqlV1ToV2: file_location.replica_role 列已添加");
    }

    private void addReplicationFactorColumn(Connection conn) throws SQLException {
        // 检查 replication_factor 列是否已存在
        String checkSql = "SELECT COUNT(*) FROM information_schema.columns "
                + "WHERE table_schema = DATABASE() "
                + "AND table_name = 'file_metadata' "
                + "AND column_name = 'replication_factor'";

        try (PreparedStatement stmt = conn.prepareStatement(checkSql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("MysqlV1ToV2: file_metadata.replication_factor 列已存在，跳过 ALTER");
                return;
            }
        }

        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_metadata` "
                        + "ADD COLUMN `replication_factor` TINYINT NOT NULL DEFAULT 1 "
                        + "COMMENT '目标副本数；1=单副本，2/3=组内节点数' "
                        + "AFTER `file_size`"
        );
        LOG.info("MysqlV1ToV2: file_metadata.replication_factor 列已添加");
    }

    private void addHashStatusIndex(Connection conn) throws SQLException {
        // 检查 idx_hash_status 索引是否已存在
        String checkSql = "SELECT COUNT(*) FROM information_schema.statistics "
                + "WHERE table_schema = DATABASE() "
                + "AND table_name = 'file_location' "
                + "AND index_name = 'idx_hash_status'";

        try (PreparedStatement stmt = conn.prepareStatement(checkSql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
                LOG.info("MysqlV1ToV2: file_location.idx_hash_status 索引已存在，跳过 ALTER");
                return;
            }
        }

        try {
            conn.createStatement().executeUpdate(
                    "ALTER TABLE `file_location` ADD INDEX `idx_hash_status` (`file_hash`, `status`)"
            );
            LOG.info("MysqlV1ToV2: file_location.idx_hash_status 索引已添加");
        } catch (SQLException e) {
            // 1061 = Duplicate key name，幂等忽略
            if (e.getErrorCode() != 1061) {
                throw e;
            }
            LOG.info("MysqlV1ToV2: file_location.idx_hash_status 索引已存在（1061），跳过");
        }
    }

    private void createReplicationGroupTable(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `replication_group` ("
                        + "`group_id` VARCHAR(64) NOT NULL COMMENT '组ID', "
                        + "`group_name` VARCHAR(128) NOT NULL COMMENT '组名', "
                        + "`node_ids` VARCHAR(512) NOT NULL COMMENT '组成员node_id列表,逗号分隔(2~3个)', "
                        + "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                        + "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`group_id`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冗余组配置表'"
        );
        LOG.info("MysqlV1ToV2: replication_group 表已确保存在");
    }

    private void createReplicaSyncTaskTable(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `replica_sync_task` ("
                        + "`task_id` VARCHAR(64) NOT NULL COMMENT '任务ID', "
                        + "`file_hash` CHAR(64) NOT NULL COMMENT '文件hash', "
                        + "`source_node` VARCHAR(128) NOT NULL COMMENT '源节点(primary)', "
                        + "`target_node` VARCHAR(128) NOT NULL COMMENT '目标节点', "
                        + "`status` TINYINT NOT NULL DEFAULT 0 COMMENT '0=PENDING,1=IN_FLIGHT,2=DONE,3=FAILED', "
                        + "`retry_count` TINYINT NOT NULL DEFAULT 0 COMMENT '累计失败次数(达4告警)', "
                        + "`file_size` BIGINT NOT NULL DEFAULT 0 COMMENT '文件大小(字节,用于限速与超时)', "
                        + "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                        + "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`task_id`), "
                        + "UNIQUE KEY `uk_hash_target` (`file_hash`, `target_node`), "
                        + "INDEX `idx_status` (`status`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账同步任务表'"
        );
        LOG.info("MysqlV1ToV2: replica_sync_task 表已确保存在");
    }
}
