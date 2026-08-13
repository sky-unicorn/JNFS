# Schema Migration (数据迁移强制规则)

**任何涉及存储信息变更、新旧无法兼容的改动，必须实现完整的迁移方案。** 这包括但不限于：

- 表结构变更（新增/删除/修改字段、索引调整）
- 本地存储格式变更（H2 文件库 schema、序列化格式；FILE 仅作为历史 `namenode_meta.log` 规整的前置步骤保留）
- 数据语义变更（字段含义变化、编码规则调整）
- 任何会导致旧版本数据无法被新版本代码正确读取的改动

### 强制要求

1. **新增 MigrationStep**：在 `jnfs-namenode/src/main/java/org/jnfs/namenode/migration/` 下实现迁移步骤类，通过 `supports(StorageMode)` 显式声明覆盖的模式——**必须同时覆盖 h2 和 mysql**（同一份 JDBC 步骤经方言路由即可，如 `JdbcV4ToV5`）；FILE 链作为历史日志规整的前置步骤仍需保留，以支撑老 file 部署平滑升级
2. **注册步骤**：在 `jnfs-namenode/src/main/resources/META-INF/migrations/` 下新增对应的 `.properties` 配置文件
3. **递增版本号**：`MigrationRunner.CURRENT_VERSION` 必须递增，版本号单调递增不跳号
4. **更新 schema 定义**：`mysql/jnfs.sql` 必须反映最新完整 schema（含 `schema_version` 表）；锚点业务表 DDL 以 `JdbcMetadataManager#anchorTableDdl` 为单一来源，h2 / mysql 共用，避免 schema 漂移
5. **遵守四项不变式**（设计文档 §3.2）：
   - **INV-1**: `storage_id` 一旦分配永不变更
   - **INV-2**: `storage_id` 全局唯一
   - **INV-3**: 迁移步骤必须幂等可重入
   - **INV-4**: 迁移失败必须拒绝启动（`System.exit(2)`）
6. **禁止就地兼容代码**：不允许在业务逻辑中出现 `// 兼容旧数据` 分支，所有兼容处理必须通过迁移步骤完成
7. **Maven 版本号**：破坏性变更（无法兼容旧数据）对应大版本号递增。当前版本 `1.0.0-SNAPSHOT`
8. **单机→集群迁移（单向）**：H2 = 单机，MYSQL = 集群。每次数据结构变更都要考虑「单机(h2) → 集群(mysql)」的迁移可行性——二者共用同一份 schema（DDL 单一来源），保证 h2 数据结构/数据可被 mysql 正确读取与迁移；**反向不需考虑**（集群数据无法迁移回单机）。禁止在业务逻辑出现「单机→集群升级」分支，统一经迁移步骤处理

### 迁移框架参考

- 框架代码：`jnfs-common/src/main/java/org/jnfs/common/migration/`
- 设计文档：`docs/design/upgrade-migration-architecture.md`
- 现有步骤：`FileV0ToV1`（日志格式统一）、`MysqlV0ToV1`（DDL + 版本表）、`JdbcV4ToV5`（CURRENT_VERSION 4→5，h2/mysql 共享 no-op 升版本）、`FileToH2Importer` / `FileToMysqlImporter`（老 file 日志 → JDBC 跨模式导入，per-row 幂等 + 原子标记）
