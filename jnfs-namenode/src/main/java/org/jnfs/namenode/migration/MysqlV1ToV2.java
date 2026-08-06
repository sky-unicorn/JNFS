package org.jnfs.namenode.migration;

import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 模式（mysql / h2）V1 → V2 迁移步骤
 * <p>
 * 动作：
 * 1. ALTER TABLE file_location ADD COLUMN replica_role（幂等，dialect.columnExists 检查）
 * 2. ALTER TABLE file_metadata ADD COLUMN replication_factor（幂等）
 * 3. ALTER TABLE file_location ADD INDEX idx_hash_status（幂等，dialect.isDuplicateIndexError 捕获重复）
 * 4. CREATE TABLE IF NOT EXISTS replication_group
 * 5. CREATE TABLE IF NOT EXISTS replica_sync_task
 * 6. ALTER TABLE file_location 确保 status 列存在（幂等，修复部分 V1 库缺列）
 * <p>
 * 版本号写入：{@code handlesOwnVersionWrite()} 返回 false，由 {@link org.jnfs.common.migration.MigrationRunner}
 * 在本步骤成功后用其单行 UPDATE/INSERT 路径（writeJdbcVersion）写入版本号 2。
 * <p>
 * 原子性说明：mysql DDL（ALTER/CREATE TABLE）触发隐式提交，{@code conn.setAutoCommit(false)}
 * 对 DDL 无效，DDL 无法与版本号写入置于同一事务。重入安全依赖幂等性（INV-3）：
 * 每条 DDL 先查列是否存在（dialect.columnExists）判断，重复执行无副作用；
 * 版本号由 Runner 单点写入，DDL 已全部应用后才写版本，崩溃重入会重新执行幂等 DDL 再写版本。
 * <p>
 * 方言路由：
 * - 列存在性：mysql=DATABASE()/h2=CURRENT_SCHEMA，经 {@link JdbcDialect#columnExists} 零分支
 * - 索引重复：mysql=1061/h2=SQLState 42xxx+"already exists"，经 {@link JdbcDialect#isDuplicateIndexError}
 *   （H2 无 information_schema.statistics 视图，旧的 statistics 预检查已移除，统一靠 try/catch 幂等）
 * - mysql 行为与旧实现等价（columnExists 用 DATABASE()、isDuplicateIndexError 用 1061）
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
        return mode == StorageMode.MYSQL || mode == StorageMode.H2;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        // mysql DDL 触发隐式提交，无法与版本号写入置于同一事务（原子性声明不成立）；
        // 改由 MigrationRunner 在步骤成功后单点写版本号（writeJdbcVersion），
        // 与 FileV1ToV2 模式一致，统一版本号写入模式。重入安全依赖幂等性（INV-3）。
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        DataSource ds = ctx.dataSource();
        if (ds == null) {
            return ctx.mode() + " mode requires a DataSource";
        }
        JdbcDialect dialect = JdbcDialect.dialectFor(ctx.mode());

        try (Connection conn = ds.getConnection()) {
            // 注意：mysql DDL 隐式提交，setAutoCommit(false) 对 DDL 无效。
            // 不再使用事务包装，避免误导。重入安全依赖幂等性（INV-3）。

            // 0. 前置依赖：确保 file_location.status 列存在。
            //    修复历史部分 V1 库（schema_version=1 但 file_location 缺 status 列，
            //    由早期建表漏建 + V0→V1 版本号已推进导致 ensureStatusColumn 不再执行）。
            //    本步骤 AFTER status 与 idx_hash_status 索引均依赖该列，故在此幂等补齐。
            ensureStatusColumn(conn, dialect);

            // 1. file_location 增加 replica_role 列
            addReplicaRoleColumn(conn, dialect);

            // 2. file_metadata 增加 replication_factor 列
            addReplicationFactorColumn(conn, dialect);

            // 3. file_location 增加 idx_hash_status 索引
            addHashStatusIndex(conn, dialect);

            // 4. 创建 replication_group 表
            createReplicationGroupTable(conn);

            // 5. 创建 replica_sync_task 表
            createReplicaSyncTaskTable(conn);

            // 版本号写入由 MigrationRunner 在本步骤返回成功后执行（writeJdbcVersion）

            LOG.info("MysqlV1ToV2: 迁移完成");
            return null;
        } catch (SQLException e) {
            LOG.error("MysqlV1ToV2: 迁移失败", e);
            return ctx.mode() + " migration V1→V2 failed: " + e.getMessage();
        }
    }

    /**
     * 幂等确保 file_location.status 列存在。
     * <p>
     * 修复历史部分 V1 库：早期建表漏建 status 列，schema_version 已推进到 1，
     * 导致 V0->V1 的 ensureStatusColumn 不再执行。本步骤的 AFTER status 与
     * idx_hash_status 索引依赖该列，故在此幂等补齐（dialect.columnExists）。
     */
    private void ensureStatusColumn(Connection conn, JdbcDialect dialect) throws SQLException {
        if (dialect.columnExists(conn, "file_location", "status")) {
            LOG.info("MysqlV1ToV2: file_location.status 列已存在，跳过 ALTER");
            return;
        }

        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `status` TINYINT NOT NULL DEFAULT 1 "
                        + "COMMENT '状态: 1-正常, 0-损坏' "
                        + "AFTER `datanode_addr`"
        );
        LOG.info("MysqlV1ToV2: file_location.status 列已补齐");
    }

    private void addReplicaRoleColumn(Connection conn, JdbcDialect dialect) throws SQLException {
        if (dialect.columnExists(conn, "file_location", "replica_role")) {
            LOG.info("MysqlV1ToV2: file_location.replica_role 列已存在，跳过 ALTER");
            return;
        }

        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `replica_role` TINYINT NOT NULL DEFAULT 0 "
                        + "COMMENT '0=PRIMARY,1=SECONDARY' "
                        + "AFTER `status`"
        );
        LOG.info("MysqlV1ToV2: file_location.replica_role 列已添加");
    }

    private void addReplicationFactorColumn(Connection conn, JdbcDialect dialect) throws SQLException {
        if (dialect.columnExists(conn, "file_metadata", "replication_factor")) {
            LOG.info("MysqlV1ToV2: file_metadata.replication_factor 列已存在，跳过 ALTER");
            return;
        }

        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_metadata` "
                        + "ADD COLUMN `replication_factor` TINYINT NOT NULL DEFAULT 1 "
                        + "COMMENT '目标副本数；1=单副本，2/3=组内节点数' "
                        + "AFTER `file_size`"
        );
        LOG.info("MysqlV1ToV2: file_metadata.replication_factor 列已添加");
    }

    /**
     * 幂等添加 idx_hash_status 索引。
     * <p>
     * 旧的 information_schema.statistics 预检查在 H2 上不可用（H2 无 statistics 视图），
     * 故移除预检查，统一靠 ALTER + try/catch 经 {@link JdbcDialect#isDuplicateIndexError}
     * 幂等（mysql=1061 / h2=SQLState 42xxx+"already exists"）。mysql 行为与旧实现等价。
     */
    private void addHashStatusIndex(Connection conn, JdbcDialect dialect) throws SQLException {
        try {
            conn.createStatement().executeUpdate(
                    "ALTER TABLE `file_location` ADD INDEX `idx_hash_status` (`file_hash`, `status`)"
            );
            LOG.info("MysqlV1ToV2: file_location.idx_hash_status 索引已添加");
        } catch (SQLException e) {
            // mysql=1061 / h2=SQLState 42xxx+"already exists"，幂等忽略
            if (!dialect.isDuplicateIndexError(e)) {
                throw e;
            }
            LOG.info("MysqlV1ToV2: file_location.idx_hash_status 索引已存在（{}），跳过", e.getErrorCode());
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
