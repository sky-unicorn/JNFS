# Schema Migration (数据迁移强制规则)

**任何涉及存储信息变更、新旧无法兼容的改动，必须实现完整的迁移方案。** 这包括但不限于：

- 表结构变更（新增/删除/修改字段、索引调整）
- 本地存储格式变更（file 模式日志格式、序列化格式）
- 数据语义变更（字段含义变化、编码规则调整）
- 任何会导致旧版本数据无法被新版本代码正确读取的改动

### 强制要求

1. **新增 MigrationStep**：在 `jnfs-namenode/src/main/java/org/jnfs/namenode/migration/` 下实现迁移步骤类，同时覆盖 file 和 mysql 两种模式
2. **注册步骤**：在 `jnfs-namenode/src/main/resources/META-INF/migrations/` 下新增对应的 `.properties` 配置文件
3. **递增版本号**：`MigrationRunner.CURRENT_VERSION` 必须递增，版本号单调递增不跳号
4. **更新 jnfs.sql**：`mysql/jnfs.sql` 必须反映最新完整 schema（含 `schema_version` 表）
5. **遵守四项不变式**（设计文档 §3.2）：
   - **INV-1**: `storage_id` 一旦分配永不变更
   - **INV-2**: `storage_id` 全局唯一
   - **INV-3**: 迁移步骤必须幂等可重入
   - **INV-4**: 迁移失败必须拒绝启动（`System.exit(2)`）
6. **禁止就地兼容代码**：不允许在业务逻辑中出现 `// 兼容旧数据` 分支，所有兼容处理必须通过迁移步骤完成
7. **Maven 版本号**：破坏性变更（无法兼容旧数据）对应大版本号递增。当前版本 `1.0.0-SNAPSHOT`

### 迁移框架参考

- 框架代码：`jnfs-common/src/main/java/org/jnfs/common/migration/`
- 设计文档：`docs/design/upgrade-migration-architecture.md`
- 现有步骤：`FileV0ToV1`（日志格式统一）、`MysqlV0ToV1`（DDL + 版本表）
