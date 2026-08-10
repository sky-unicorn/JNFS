package org.jnfs.namenode.migration;

import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.MigrationContext;
import org.jnfs.namenode.JdbcMetadataManager;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBC 模式（mysql / h2）V0 → V1 迁移步骤
 * <p>
 * 动作：
 * 1. CREATE TABLE IF NOT EXISTS 三张锚点业务表（file_metadata / file_location / file_upload_lock）
 *    —— mysql 存量部署这三张表必然已存在（detectVersion 判定 V0 的前提就是锚点表存在），此处为 no-op；
 *    H2 全新部署首启走全链建表（detectH2Version 不走 CURRENT_VERSION 捷径），三张表在此首次创建。
 * 2. CREATE TABLE IF NOT EXISTS node_registry（如果不存在）
 * 3. ALTER TABLE file_location 确保 datanode_id 字段存在（允许 NULL，过渡期）
 * 4. ALTER TABLE file_location 确保 status 字段存在（C3 根因修复）
 * 5. CREATE TABLE IF NOT EXISTS schema_version 并 INSERT 当前版本 1
 * <p>
 * 注意：
 * - 不执行 UPDATE ... SET datanode_id = ... 反查补全（反查不到，见设计文档 §4.9）
 * - datanode_id 的在线补全由 DataNode 心跳注册时触发（§4.9.2）
 * - 迁移 DDL 与 INSERT schema_version 在同一事务内（原子性；H2 DDL 事务原生支持，mysql DDL 隐式提交但语义不回退）
 * <p>
 * 幂等性保证（INV-3）：
 * - CREATE TABLE IF NOT EXISTS 天然幂等
 * - ALTER TABLE ADD COLUMN 通过 {@link JdbcDialect#columnExists} 检查（mysql=DATABASE()/h2=CURRENT_SCHEMA，零分支）
 * - ALTER TABLE ADD INDEX 通过 {@link JdbcDialect#isDuplicateIndexError} 捕获重复（mysql=1061/h2=SQLState 42xxx+"already exists"）
 * - INSERT schema_version 使用 INSERT IGNORE 避免重复
 * <p>
 * 方言路由：mysql 行为与旧实现逐字节等价（DATABASE()=DATABASE()、1061=1061、锚点表已存在 no-op）。
 * DDL（ENGINE/CHARSET/反引号/KEY/AFTER/COMMENT）经探针验证 H2 MariaDB 模式直接支持，保留 mysql 原样。
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
        return mode == StorageMode.MYSQL || mode == StorageMode.H2;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return true; // JDBC 模式下迁移 DDL 与版本号写入在同一事务内，保证原子性 (§4.6)
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        DataSource ds = ctx.dataSource();
        if (ds == null) {
            return ctx.mode() + " mode requires a DataSource";
        }
        JdbcDialect dialect = JdbcDialect.dialectFor(ctx.mode());

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 0. 锚点业务表（H2 全链首启在此创建；mysql 存量为 no-op）
                ensureBusinessAnchorTables(conn);

                // 1. 确保 node_registry 表存在
                createNodeRegistryIfNotExists(conn);

                // 2. 确保 file_location.datanode_id 字段存在
                ensureDatanoIdColumn(conn, dialect);

                // 3. 确保 file_location.status 字段存在 (C3 根因修复：
                //    历史 file_location 由旧构造函数建表时漏建 status 列，与 jnfs.sql 不一致)
                ensureStatusColumn(conn, dialect);

                // 4. 创建 schema_version 表并写入版本号
                createSchemaVersionAndInsert(conn, 1);

                conn.commit();
                LOG.info("MysqlV0ToV1: 迁移完成");
                return null;
            } catch (SQLException e) {
                conn.rollback();
                LOG.error("MysqlV0ToV1: 迁移失败，已回滚", e);
                return ctx.mode() + " migration failed: " + e.getMessage();
            }
        }
    }

    /**
     * 确保三张锚点业务表存在（file_metadata / file_location / file_upload_lock）。
     * <p>
     * mysql 存量 V0 部署：三张表必然已存在（detectVersion 判定 V0 的前提是锚点表 file_metadata 存在），
     * 此处 CREATE TABLE IF NOT EXISTS 为 no-op，行为与旧实现一致。
     * <p>
     * H2 全新部署首启：detectH2Version 不走 CURRENT_VERSION 捷径、返回 0，本步骤是 V0 链头，
     * 必须在此创建锚点表，否则后续 ALTER TABLE file_location 补列会因表不存在而失败。
     * DDL 复用 MySQLMetadataManager 的建表语句（与 mysql/jnfs.sql V2+ 终态一致），并带上全部列与索引，
     * 这样后续 ALTER ADD COLUMN / ADD INDEX 经 columnExists / isDuplicateIndexError 判定为已存在而跳过，天然幂等。
     */
    private void ensureBusinessAnchorTables(Connection conn) throws SQLException {
        // DDL 单一来源：JdbcMetadataManager.anchorTableDdl()（与 NameNode 侧 buildDdl 共用，避免 schema 漂移）。
        // 含全部列与索引，后续 ALTER ADD COLUMN / ADD INDEX 经 columnExists / isDuplicateIndexError 判定已存在跳过，天然幂等。
        for (String ddl : JdbcMetadataManager.anchorTableDdl()) {
            conn.createStatement().executeUpdate(ddl);
        }
        LOG.info("MysqlV0ToV1: 锚点业务表 (file_metadata/file_location/file_upload_lock) 已确保存在");
    }

    private void createNodeRegistryIfNotExists(Connection conn) throws SQLException {
        // DDL 单一来源：NodeRegistryDdl（含 free_space 列，V6）。
        // 与 MySQLMetadataManager.ensureNonAnchorTables 及 Registry 启动自建共用同一份 DDL，避免 schema 漂移。
        conn.createStatement().executeUpdate(
                org.jnfs.common.NodeRegistryDdl.createTableDdl()
        );
        LOG.info("MysqlV0ToV1: node_registry 表已确保存在");
    }

    private void ensureDatanoIdColumn(Connection conn, JdbcDialect dialect) throws SQLException {
        if (dialect.columnExists(conn, "file_location", "datanode_id")) {
            LOG.info("MysqlV0ToV1: file_location.datanode_id 列已存在，跳过 ALTER");
            return;
        }

        // 添加 datanode_id 列（允许 NULL，过渡期）
        conn.createStatement().executeUpdate(
                "ALTER TABLE `file_location` "
                        + "ADD COLUMN `datanode_id` VARCHAR(128) DEFAULT NULL "
                        + "COMMENT 'DataNode节点ID (关联 node_registry.node_id)' "
                        + "AFTER `file_hash`"
        );

        // 添加索引（如果不存在，重复索引经 dialect 判定后幂等跳过）
        try {
            conn.createStatement().executeUpdate(
                    "ALTER TABLE `file_location` ADD INDEX `idx_node` (`datanode_id`)"
            );
        } catch (SQLException e) {
            // mysql=1061 / h2=SQLState 42xxx+"already exists"
            if (!dialect.isDuplicateIndexError(e)) {
                throw e;
            }
            LOG.info("MysqlV0ToV1: file_location.idx_node 索引已存在（{}），跳过", e.getErrorCode());
        }

        LOG.info("MysqlV0ToV1: file_location.datanode_id 列已添加");
    }

    private void ensureStatusColumn(Connection conn, JdbcDialect dialect) throws SQLException {
        if (dialect.columnExists(conn, "file_location", "status")) {
            LOG.info("MysqlV0ToV1: file_location.status 列已存在，跳过 ALTER");
            return;
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
