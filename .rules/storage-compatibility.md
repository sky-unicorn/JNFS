# Storage Compatibility

系统运行时支持两种存储模式：**h2**（嵌入式 H2 文件库，单机模式 / 单副本）和 **mysql**（远端 MySQL，集群模式 / 多副本）。FILE 仅作为迁移框架内部规整历史 `namenode_meta.log` 的前置步骤存在，**不是运行时存储模式**。任何涉及数据存储（读写、查询、删除、数据结构变更）的代码，**必须同时适配 h2 与 mysql 两种模式**，不能只实现或测试某一种就假设另一种可用。

### 1. 同一入口、同一流程

所有数据形式（mysql、h2）的增、删、改、查，**必须走同一个入口、同一套流程**。共享逻辑统一收口在 `JdbcMetadataManager`（`org.jnfs.namenode.JdbcMetadataManager`，持有 `HikariDataSource` + `JdbcDialect`）；子类 `MySQLMetadataManager` / `H2MetadataManager` 只负责 `createDataSource`（各自 JDBC URL）与方言实例，不持有独立业务逻辑。业务代码不得绕过统一入口直接操作各自的 Manager。

### 2. 无法统一时——同一入口内按模式分支

当 mysql 与 h2 确实存在无法统一的行为差异（主要是 SQL 方言）时，**仍必须从同一入口进入**，再在内部按模式分支。差异收口到 `JdbcDialect`（`org.jnfs.common.migration.JdbcDialect`）：共享的 DDL / SQL 保持零分支（锚点业务表 DDL 以 `JdbcMetadataManager#anchorTableDdl` 为单一来源），仅在必要处（`<=>` / `NOW()-INTERVAL` / `DATABASE()`、重复键错误判定等）按方言路由。**禁止为某个模式另开独立的对外入口**——分支发生在入口内部，不在入口之前。

### 3. 数据结构变更必须覆盖全部模式

每次涉及数据结构变更（建表 / DDL / 字段 / 索引 / 序列化格式），**必须同时考虑 h2 与 mysql**，不能只改某一种。能力判断一律使用 `MetadataManager#isJdbcBacked()`，不要用 `instanceof MySQLMetadataManager` / `H2MetadataManager` 这类耦合具体实现的方式；迁移步骤通过 `MigrationStep#supports(StorageMode)` 显式声明覆盖的模式（如 `JdbcV4ToV5` 同时 `supports(MYSQL || H2)`）。

### 4. 单机 → 集群迁移路径（单向）

H2 = 单机模式（多副本仅限同机不同磁盘，无跨机物理隔离），MYSQL = 集群模式（多副本，跨机）。两种模式的副本/冗余/对账/排空能力对齐，仅物理隔离能力不同。每次数据结构变更时，**必须考虑「单机（h2）→ 集群（mysql）」的迁移路径**：h2 与 mysql 共用同一份 schema（DDL 单一来源），确保 h2 上产生的数据结构与数据能被集群模式正确读取与迁移。**反之不需要考虑**——集群（mysql）数据无法迁移回单机（h2），单向兼容即可。迁移实现的具体强制规则见 `schema-migration.md`。
