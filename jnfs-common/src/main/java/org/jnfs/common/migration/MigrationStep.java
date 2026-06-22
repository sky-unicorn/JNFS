package org.jnfs.common.migration;

/**
 * 迁移步骤接口
 * <p>
 * 每个实现类表示一次从 fromVersion 到 toVersion 的模式迁移。
 * 迁移步骤必须幂等可重入（INV-3）。
 */
public interface MigrationStep {

    /** 起始版本（含），执行前数据必须处于此版本 */
    int fromVersion();

    /** 目标版本（含），执行成功后写入此版本 */
    int toVersion();

    /** 仅当前存储模式适用时返回 true */
    boolean supports(StorageMode mode);

    /**
     * 执行迁移
     * @param ctx 迁移上下文
     * @return null 表示成功，非空字符串表示失败原因
     */
    String migrate(MigrationContext ctx) throws Exception;

    /**
     * 是否由本步骤自行管理版本号写入
     * <p>
     * 默认为 false：MigrationRunner 在 migrate() 返回成功后，会调用 writeVersion() 写入新版本号。
     * 返回 true 表示本步骤已经在自己的事务里完成了版本号写入（用于保证 MySQL 模式
     * 迁移 DML 与版本号写入在同一事务内，原子性要求见设计文档 §4.6）。
     * <p>
     * 注意：步骤自己负责版本号写入时，必须严格遵守幂等性（重跑不产生副作用）。
     */
    default boolean handlesOwnVersionWrite() {
        return false;
    }
}
