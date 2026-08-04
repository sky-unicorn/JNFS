package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;

/**
 * File 模式 V2 → V3 迁移步骤（no-op）。
 * <p>
 * replication_policy / replication_control 仅 mysql 模式使用，
 * file 模式不启用冗余，无需任何操作。
 */
public class FileV2ToV3 implements MigrationStep {

    @Override
    public int fromVersion() {
        return 2;
    }

    @Override
    public int toVersion() {
        return 3;
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
