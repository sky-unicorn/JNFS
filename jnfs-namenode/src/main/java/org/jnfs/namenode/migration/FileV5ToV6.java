package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File 模式 V5 -> V6 迁移步骤（no-op）。
 * <p>
 * 版本 6 为 node_registry 增加 free_space 列，仅影响 JDBC 模式（mysql/h2）。
 * file 模式（WAL 日志）不持有 node_registry，本步骤仅为保证 file 链与 mysql/h2 链
 * 版本号全局统一（V6）。
 * <p>
 * 幂等性（INV-3）：不读不改任何文件，重入安全。
 * 失败拒绝启动（INV-4）：本步骤不可能失败。
 * <p>
 * 版本号写入：由 {@link org.jnfs.common.migration.MigrationRunner} 在本步骤返回成功后，
 * 原子写 meta_version（handlesOwnVersionWrite 返回默认 false）。
 */
public class FileV5ToV6 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(FileV5ToV6.class);

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
        return mode == StorageMode.FILE;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) {
        // no-op：file 模式不持有 node_registry，仅升版本号 5->6
        LOG.info("FileV5ToV6: file 模式无变更，no-op（仅升版本号 5->6）");
        return null;
    }
}