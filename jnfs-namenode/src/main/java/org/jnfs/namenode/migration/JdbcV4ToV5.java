package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC 模式（mysql / h2）V4 -> V5 迁移步骤（no-op）。
 * <p>
 * 版本 5 引入 H2 嵌入式文件库后端，复用与 mysql 同一份 schema（DDL 完全一致），
 * 不产生任何 schema 变更。本步骤仅用于把 schema_version 推进到 5，
 * 保证 mysql / h2 两种 JDBC 模式与 file 链版本号全局统一（V5）。
 * <p>
 * handlesOwnVersionWrite() 返回 false（与 MysqlV1ToV2 / V2ToV3 / V3ToV4 一致），
 * 版本号写入由 {@link org.jnfs.common.migration.MigrationRunner#writeJdbcVersion} 单点完成。
 * <p>
 * 幂等性（INV-3）：不读不改任何表，重入安全。
 * 失败拒绝启动（INV-4）：本步骤不可能失败。
 */
public class JdbcV4ToV5 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcV4ToV5.class);

    @Override
    public int fromVersion() {
        return 4;
    }

    @Override
    public int toVersion() {
        return 5;
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
    public String migrate(MigrationContext ctx) {
        // no-op：H2 后端复用 mysql schema，无 DDL 变更，仅升版本号 4→5
        LOG.info("JdbcV4ToV5: JDBC 模式无 schema 变更，no-op（仅升版本号 4→5）");
        return null;
    }
}
