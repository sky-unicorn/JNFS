package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File 模式 V1 → V2 迁移步骤
 * <p>
 * 动作：**no-op**（仅升版本号，不改变日志格式）。
 * <p>
 * 设计依据（§5.2）：冗余仅限 mysql 集群模式，file 单机模式不启用冗余、保持单副本。
 * namenode_meta.log 行格式保持 V1（ADD|filename|hash|node_id|storageId），不引入 REPLICA 行。
 * 本步骤的存在仅为保证版本号在 file / mysql 两种模式下全局统一（V2）。
 * <p>
 * 幂等性（INV-3）：不读不改任何文件，重入安全。
 * 失败拒绝启动（INV-4）：本步骤不可能失败。
 * <p>
 * 版本号写入：由 {@link org.jnfs.common.migration.MigrationRunner} 在本步骤返回成功后，
 * 原子写 meta_version（handlesOwnVersionWrite 返回默认 false）。
 */
public class FileV1ToV2 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(FileV1ToV2.class);

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
        return mode == StorageMode.FILE;
    }

    @Override
    public String migrate(MigrationContext ctx) {
        // no-op：file 模式不启用冗余，日志格式保持 V1，仅升版本号
        LOG.info("FileV1ToV2: file 模式不启用冗余，no-op（仅升版本号 1→2）");
        return null;
    }
}
