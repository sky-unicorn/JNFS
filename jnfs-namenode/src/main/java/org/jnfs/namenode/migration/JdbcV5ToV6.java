package org.jnfs.namenode.migration;

import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC 模式（mysql / h2）V5 -> V6 迁移步骤。
 * <p>
 * 为 node_registry 增加 {@code free_space} 列（BIGINT NOT NULL DEFAULT 0）。
 * <p>
 * 背景：版本 5 之前 node_registry 仅承载节点身份（node_id/node_type/host/port/last_heartbeat），
 * 但 Registry 从未向其写入，节点注册信息仅存内存，重启即失。版本 6 将 Registry 节点注册
 * 持久化到 node_registry（单机模式 Registry 与 NameNode 共享同一条 H2 文件库；集群模式
 * 共享 MySQL），需落盘 DataNode 剩余空间 free_space 供 Dashboard 展示离线节点历史值，故增列。
 * <p>
 * 幂等性（INV-3）：经 {@link JdbcDialect#columnExists} 检查，列已存在则跳过（mysql=DATABASE()/
 * h2=CURRENT_SCHEMA，零分支）。
 * <p>
 * 失败拒绝启动（INV-4）：SQLException 向上抛出，由 MigrationRunner 记录并返回 fail，
 * NameNode {@code System.exit(2)} 拒绝启动。
 * <p>
 * 版本号写入：{@code handlesOwnVersionWrite()} 返回 false，由
 * {@link org.jnfs.common.migration.MigrationRunner#writeJdbcVersion} 单点完成。
 */
public class JdbcV5ToV6 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcV5ToV6.class);

    static final String TABLE = "node_registry";
    static final String COLUMN = "free_space";

    @Override
    public int fromVersion() {
        return 5;
    }

    @Override
    public int toVersion() {
        return 6;
    }

    @Override
    public boolean supports(StorageMode mode) {
        return mode == StorageMode.MYSQL || mode == StorageMode.H2;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        if (ctx.dataSource() == null) {
            return ctx.mode() + " mode requires a DataSource";
        }
        JdbcDialect dialect = JdbcDialect.dialectFor(ctx.mode());

        try (java.sql.Connection conn = ctx.dataSource().getConnection()) {
            if (dialect.columnExists(conn, TABLE, COLUMN)) {
                LOG.info("JdbcV5ToV6: {}.{} 列已存在，跳过 ALTER", TABLE, COLUMN);
                return null;
            }
            conn.createStatement().executeUpdate(
                    "ALTER TABLE `" + TABLE + "` "
                            + "ADD COLUMN `" + COLUMN + "` BIGINT NOT NULL DEFAULT 0 "
                            + "COMMENT 'DataNode剩余空间(字节); NameNode=0' "
                            + "AFTER `last_heartbeat`"
            );
            LOG.info("JdbcV5ToV6: {}.{} 列已添加", TABLE, COLUMN);
        }
        return null;
    }
}
