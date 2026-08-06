package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File 模式 V4 -> V5 迁移步骤（no-op）。
 * <p>
 * 版本 5 引入 H2 嵌入式文件库后端，仅影响 JDBC 模式（mysql/h2）。
 * file 模式（WAL 日志）不启用冗余、不引入新日志格式，本步骤仅为保证
 * file 链与 mysql/h2 链版本号全局统一（V5）。
 * <p>
 * 幂等性（INV-3）：不读不改任何文件，重入安全。
 * 失败拒绝启动（INV-4）：本步骤不可能失败。
 * <p>
 * 版本号写入：由 {@link org.jnfs.common.migration.MigrationRunner} 在本步骤返回成功后，
 * 原子写 meta_version（handlesOwnVersionWrite 返回默认 false）。
 */
public class FileV4ToV5 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(FileV4ToV5.class);

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
        return mode == StorageMode.FILE;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) {
        // no-op：file 模式不启用冗余、不引入新日志格式，仅升版本号 4->5
        LOG.info("FileV4ToV5: file 模式无变更，no-op（仅升版本号 4->5）");
        return null;
    }
}
