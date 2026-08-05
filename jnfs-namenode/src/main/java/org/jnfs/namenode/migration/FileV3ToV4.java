package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;

/**
 * File 模式 V3 → V4 迁移步骤（no-op）。
 * <p>
 * node_drain 表仅 mysql 模式使用（drain 仅 mysql 模式启用，见设计 §1.3 非目标），
 * file 模式不启用冗余，自然也不启用 drain，无需任何操作。
 */
public class FileV3ToV4 implements MigrationStep {

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
        return mode == StorageMode.FILE;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        return null; // no-op
    }
}
