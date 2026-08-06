package org.jnfs.namenode;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * MySQL 元数据管理器
 * <p>
 * 继承 {@link JdbcMetadataManager}，复用全部 JDBC 业务逻辑（queryByHash / logAddFile / 锁 / backfill 等）。
 * 子类只负责：createDataSource（jdbc:mysql，HikariCP）+ MysqlDialect + 全量锚点外的业务表 DDL
 * （node_registry / replication_group / replica_sync_task / replication_policy / replication_control）。
 * <p>
 * <b>行为锚点</b>：与旧实现完全一致。mysql 全新部署场景由 {@link NameNodeServer#main} 先跑
 * {@code MigrationRunner.run(MYSQL,...)}（freshDeploy 捷径只建 schema_version + 写版本 5），
 * 业务表由本构造函数的 CREATE TABLE IF NOT EXISTS 兜底建出；存量升级场景由迁移链建表，
 * 此处 CREATE TABLE IF NOT EXISTS 对已存在的表无副作用（幂等）。
 */
public class MySQLMetadataManager extends JdbcMetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLMetadataManager.class);

    public MySQLMetadataManager(String host, int port, String dbName, String user, String password) {
        this(createDataSource(host, port, dbName, user, password));
    }

    /**
     * 使用已有的 DataSource 构造（迁移流程中先创建 DataSource，再传入）。
     * <p>
     * 父类构造时执行锚点表 DDL（file_metadata / file_location / file_upload_lock），
     * 本构造再补建其余业务表（mysql 全新部署兜底），与旧实现建表范围完全一致。
     */
    public MySQLMetadataManager(HikariDataSource dataSource) {
        super(dataSource, JdbcDialect.dialectFor(StorageMode.MYSQL));
        try (Connection conn = dataSource.getConnection()) {
            ensureNonAnchorTables(conn);
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
     * 补建锚点表之外的业务表（mysql 全新部署兜底）。
     * <p>
     * 与旧实现建表语句逐字节一致；所有列定义与 {@code mysql/jnfs.sql} V3 schema 保持一致。
     * 存量升级场景由迁移框架（MysqlV0ToV1 + MysqlV1ToV2 + MysqlV2ToV3）负责补列/建表，
     * 此处 CREATE TABLE IF NOT EXISTS 对已存在的表/列无副作用（幂等）。
     */
    private void ensureNonAnchorTables(Connection conn) throws SQLException {
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
    }
}
