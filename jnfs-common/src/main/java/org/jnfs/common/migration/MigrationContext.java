package org.jnfs.common.migration;

import javax.sql.DataSource;
import java.io.File;

/**
 * 迁移步骤执行上下文
 * 向迁移步骤暴露所需的最小接口
 */
public final class MigrationContext {

    private final StorageMode mode;
    private final File dataDir;
    private final DataSource dataSource;

    public MigrationContext(StorageMode mode, File dataDir, DataSource dataSource) {
        this.mode = mode;
        this.dataDir = dataDir;
        this.dataSource = dataSource;
    }

    public StorageMode mode() {
        return mode;
    }

    /**
     * file 模式下的数据目录
     */
    public File dataDir() {
        return dataDir;
    }

    /**
     * mysql 模式下的数据源（可能为 null）
     */
    public DataSource dataSource() {
        return dataSource;
    }
}
