# JNFS 升级与数据迁移架构设计方案

## 1. 背景与问题

### 1.1 问题来源
JNFS 处于 `0.1.x` 早期阶段,表结构与单机存储格式会随版本持续演进。典型破坏性变更已经发生过:

- `8639507 feat(core): 引入节点ID机制` — 把 `host:port` 标识换成 `node_id`,影响 file 模式日志、`file_location` 表、心跳协议
- 未来预计的变更:元数据字段增减、索引调整、存储格式优化、分布式锁表演进等

### 1.2 核心痛点
没有统一升级机制,只能在业务代码里就地打补丁:
- **兼容代码散落在业务逻辑里**,无法退出,越堆越多
- **线上数据处于哪个版本不可见**,运维靠猜
- **升级失败时没有 fail-fast**,容易出现半新半旧的脏状态
- **双存储模式(file / mysql)需要分别处理**,容易遗漏一边

### 1.3 设计目标
1. 启动时自动识别当前数据版本,按序执行迁移步骤
2. 迁移失败必须拒绝启动,杜绝带伤运行
3. file 与 mysql 两种存储模式使用统一的迁移框架,各自提供实现
4. 老兼容代码获得明确的"退出路径",不再长期堆积
5. 机制足够轻量,不引入额外中间件依赖

---

## 2. 现状诊断

### 2.1 已存在的就地兼容代码

| 位置 | 兼容对象 | 处理方式 |
|---|---|---|
| `MetadataManager.recover()` `:71-86` | file 模式日志新旧两种格式 | 运行时分支判断,旧格式用 `computeIfAbsent` 补 `storageId` |
| `mysql/jnfs.sql` `:42-43` | `file_location.datanode_id` 与 `datanode_addr` 并存 | 字段冗余,注释标记"兼容旧数据" |

### 2.2 为什么不能再继续这样
- 第二次同类变更出现时,兼容代码会再叠加一层,业务逻辑可读性急剧下降
- 兼容分支没有删除时机,长期成本累积
- 没有版本号概念,无法回答"这个实例的数据格式是哪个版本"

---

## 3. 方案对比

| 方案 | 思路 | 优点 | 缺点 | 适用 |
|---|---|---|---|---|
| **A. 版本号 + 启动迁移** | 引入 `schema_version`,启动时按序执行迁移脚本 | 机制清晰、可审计、可重放 | 初次落地需改造启动流程 | 长期演进的生产行项目 |
| **B. 双写 + 异步清理** | 新版本同时写新旧两份格式,几个版本后删除旧格式 | 支持滚动升级、不停机 | 写入与存储成本翻倍,逻辑复杂 | 集群不能停服的场景 |
| **C. 显式标记破坏性版本** | 文档标注"此版本需手动迁移",提供一次性脚本 | 零代码成本 | 人工操作易错、无法强制 | 用户少、能停服的早期阶段 |

### 3.1 选型结论
当前阶段(0.1.x、用户量小、允许短暂停服)采用 **方案 A**。

理由:
- 实施成本与方案 C 接近,但能拿到版本可见性与 fail-fast 能力
- 方案 B 的不停机滚动升级目前不是刚需,等真正出现 HA 集群再演进
- 框架足够通用,未来切换到方案 B 也能复用迁移步骤定义

### 3.2 核心不变式

下列不变式是整个迁移机制的**先决约束**,任何 schema 变更或 PR 都必须遵守。违反任何一条都会导致外部引用断裂或数据错乱。

| ID | 不变式 | 触发原因 |
|---|---|---|
| **INV-1** | `storage_id` 一旦分配,**永不变更** | 外部系统持有 storage_id 引用文件,变更即引用断裂 |
| **INV-2** | `storage_id` 全局唯一 | file / mysql 两种模式下都不允许重复 |
| **INV-3** | 迁移步骤必须**幂等可重入** | 中途崩溃后重启必须能安全继续,不能产生副作用 |
| **INV-4** | 迁移失败必须拒绝启动 | 不允许带半新半旧的数据对外提供服务 |

#### 现状已违反 INV-1(必须修复)

`MetadataManager.java:80-86` 对旧格式行(无 `storageId`)走的 fallback 分支:

```java
storageId = hashToId.computeIfAbsent(hash, k -> UUID.randomUUID().toString());
```

- `UUID.randomUUID()` **每次进程启动都生成新值**
- `hashToId` 是 `recover()` 的内存参数,不持久化
- 结果:**每次 NameNode 重启,旧文件的 storage_id 全部变化**

如果已有外部系统持有旧文件返回的 storage_id,这是已经存在的 bug,必须借这次迁移一并修复。

#### MySQL 模式下的 storage_id

`file_metadata.storage_id` 是主键(`mysql/jnfs.sql:60`),一直就是 UUID 主键,任何能跑起来的老版本都有值。**MySQL 模式迁移不需要为 storage_id 做任何额外操作**,只需在落地前审计上传响应路径,确认 storage_id 一定来自持久化层而非临场生成。

---

## 4. 推荐方案详细设计

### 4.1 版本号定义规则

- 采用单调递增的整数,**不**与 Maven 版本号绑定
- 每种存储模式独立维护版本序列(file 模式与 mysql 模式可不同步)
- 迁移步骤只允许线性递增,不支持跳过中间版本

### 4.2 版本号存储

#### 4.2.1 file 模式
在 `namenode_meta.log` 同目录新增版本文件:

```
<工作目录>/
├── namenode_meta.log
└── meta_version           # 内容仅为一个整数,如 "3"
```

- 文件不存在时视为版本 `0`(当前"就地兼容"代码所处的状态)
- 首次启动后立即写入当前目标版本

#### 4.2.2 mysql 模式
新增元表,不与业务表耦合:

```sql
CREATE TABLE `schema_version` (
  `version`       INT          NOT NULL COMMENT '当前 schema 版本',
  `upgraded_at`   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`version`)
) ENGINE=InnoDB CHARACTER SET=utf8mb4 COMMENT='schema 版本记录';
```

未来若需更精细审计,可扩展为 `schema_migration_history(version, script, checksum, migrated_at)`。

### 4.3 版本判定逻辑(全新部署 vs 老数据)

启动时不能简单地用"`schema_version` 不存在 = 版本 0"判定。必须区分三种情况,否则全新部署会被误判为版本 0,触发对不存在表的迁移而报错。

#### 4.3.1 三态判定

| 情况 | 表/文件状态 | 判定版本 | 后续动作 |
|---|---|---|---|
| **已纳入管理** | `schema_version` 表存在(file: `meta_version` 文件存在) | 读取表中/文件中的版本号 | 按需增量迁移 |
| **老数据** | `schema_version` 不存在,但业务表 `file_metadata` 存在(file: `namenode_meta.log` 存在) | `0` | 按 0→1→2→… 链式迁移 |
| **全新部署** | `schema_version` 不存在,业务表也不存在(file: 日志也不存在) | `CURRENT_VERSION` | 跳过所有迁移,直接建最新版 schema,写入当前版本号 |

#### 4.3.2 判定伪代码

```java
int detectVersion(StorageMode mode) {
    if (mode == MYSQL) {
        if (!tableExists("schema_version")) {
            // 关键判定:检查一个"所有版本都存在"的业务表
            if (tableExists("file_metadata")) {
                return 0;                 // 老数据
            } else {
                return CURRENT_VERSION;   // 全新部署
            }
        }
        return readSchemaVersion();
    }

    if (mode == FILE) {
        if (!fileExists("meta_version")) {
            if (fileExists("namenode_meta.log")) {
                return 0;                 // 老数据
            } else {
                return CURRENT_VERSION;   // 全新实例
            }
        }
        return readFileVersion();
    }
    throw new IllegalArgumentException("unknown storage mode: " + mode);
}
```

#### 4.3.3 业务表判定的 SQL 细节

- **必须**用 `information_schema` 判断表是否存在,**不要**用 `SELECT ... FROM` 查询(空表或无权限时会误判)
- 判定锚点表选 `file_metadata` 这种**所有版本都存在**的表,不要选可能在中途版本被改名/删除的表

```sql
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'file_metadata';
```

#### 4.3.4 全新部署的后续动作

判定为 `CURRENT_VERSION` 后,启动流程:
1. 执行完整建表脚本(当前版本的 `jnfs.sql`)
2. `INSERT INTO schema_version(version) VALUES (CURRENT_VERSION)`(file 模式写入 `meta_version` 文件)
3. 跳过所有 `MigrationStep`
4. 正常初始化业务组件

**结论**:全新部署与老部署升级走两条完全不同的路径,最终都收敛到同一个版本号。

#### 4.3.5 老数据升级完整路径

以当前老旧服务升级到新版本为例:

```
1. 停旧服务
2. 替换 jar
3. 启动新服务
   │
   ├─ 检测:schema_version 表不存在,file_metadata 表存在
   │         ──> 判定为老数据,版本 = 0
   │
   ├─ 建 schema_version 表(空)
   │
   ├─ 执行 0→1:(示例)回填 file_location.datanode_id
   │             UPDATE schema_version SET version = 1
   │
   ├─ 执行 1→2:(未来)删除 datanode_addr 字段
   │             UPDATE schema_version SET version = 2
   │
   └─ ... 直到跑到当前版本
       ──> 正常启动对外服务
```

### 4.4 迁移步骤接口

放置于 `jnfs-common`,与 `DaemonThreadFactory` / `SegmentedLocks` 等共用工具同级:

```java
package org.jnfs.common.migration;

public interface MigrationStep {
    /** 起始版本(含),执行前数据必须处于此版本 */
    int fromVersion();
    /** 目标版本(含),执行成功后写入此版本 */
    int toVersion();
    /** 仅当前存储模式适用时返回 true */
    boolean supports(StorageMode mode);
    /** 执行迁移,返回 null 表示成功,返回非空字符串作为失败原因 */
    String migrate(MigrationContext ctx) throws Exception;
}
```

`MigrationContext` 暴露给迁移步骤的最小接口:
- `StorageMode mode()` — 当前存储模式
- `File dataDir()` — file 模式下的数据目录
- `DataSource dataSource()` — mysql 模式下的数据源(可能为 null)
- `Logger logger()`

### 4.5 注册与发现

各模块在自己的 `META-INF/migrations/` 下声明:

```
jnfs-namenode/src/main/resources/META-INF/migrations/
├── file_v0_to_v1.properties
├── mysql_v0_to_v1.properties
└── mysql_v1_to_v2.properties
```

properties 文件指向一个 `MigrationStep` 实现类:

```properties
class=org.jnfs.namenode.migration.FileV0ToV1
```

启动时通过 `ServiceLoader` 或目录扫描统一加载,按 `(fromVersion, 模块)` 排序后执行。使用配置文件而非纯 `ServiceLoader` 的原因:便于一眼看到当前版本演进路径。

### 4.6 执行器

`org.jnfs.common.migration.MigrationRunner` 负责编排:

```java
public final class MigrationRunner {
    public static MigrationResult run(StorageMode mode, Object context) {
        // 1. 读取当前版本
        // 2. 按 fromVersion 升序加载适用步骤
        // 3. 循环执行,每步成功后立即持久化新版本号
        // 4. 任一步骤失败立即返回,不再继续
        // 5. 全部完成或无步骤可执行时返回 ok
    }
}
```

关键原则:
- **原子性**:每一步迁移与对应的版本号写入必须一起成功或一起失败
  - file 模式:先写临时文件 `meta_version.tmp`,再 `renameTo` 覆盖
  - mysql 模式:迁移 DML/DML 与 `UPDATE schema_version` 在**同一个事务**内
- **幂等性**:迁移步骤必须可重入,中途崩溃重启后能安全再次执行
- **顺序性**:严格按 `fromVersion` 升序执行,不允许跳版本

### 4.7 启动流程改造

`NameNodeServer` / `RegistryServer` / `DataNodeServer`(如未来需要)的 `main` 方法早期插入:

```java
public static void main(String[] args) {
    // 1. 解析配置、确定 storageMode 与 dataDir
    // 2. 执行迁移(必须在初始化任何业务管理器之前)
    MigrationResult r = MigrationRunner.run(storageMode, ctx);
    if (r.failed()) {
        LOG.error("数据迁移失败,拒绝启动。原因: {}", r.message());
        System.exit(2);
    }
    // 3. 正常初始化业务组件
}
```

迁移失败必须 `System.exit(2)` 让 systemd / 脚本感知到"不可恢复"而非"重启中"。

### 4.8 现有兼容代码的退出路径

方案 A 落地后,现有两处就地兼容代码逐步清理。注意:**两条路径都只做 DDL / 数据重写,不做"反查 node_id"** — 因为迁移时拿不到老 DataNode 的 node_id(详见 §4.9)。

#### 4.8.1 file 模式日志格式

**迁移步骤动作(`FileV0ToV1`)**:
1. 读 `namenode_meta.log` 全部行
2. 对旧格式行 `ADD|filename|hash|host:port`(无 storageId):
   - 同一 hash 已分配过 storageId 则复用,否则 `UUID.randomUUID()` 生成一次
   - 同 hash 内**去重**,保证 INV-2
3. 把所有行统一重写为新格式 `ADD|filename|hash|host:port|storageId`,写入 `namenode_meta.log.tmp`
   - 注意:此阶段**保留 `host:port` 而非替换为 `node_id`**,node_id 补全见 §4.9
4. `fsync` 临时文件
5. `renameTo` 原子替换原文件
6. 写入 `meta_version = 1`

**幂等性保证**:
- 重跑时,凡是已经包含 storageId 的行(包括上次已重写的)直接保留,不为它重新分配
- 中途崩溃的恢复:临时文件残留则删除,原文件未替换则按"全新迁移"重跑,所有已分配的 storageId 都在原文件里,不会变

**`MetadataManager.recover()` 的修改**:
- 迁移完成后,`:80-86` 的 else 分支**直接删除**
- 任何不满足 `parts.length == 5 && "ADD".equals(parts[0])` 的行视为日志损坏,抛 `IOException` 而非静默补全
- 这样"必须经过迁移"成为硬约束 — 未迁移的旧数据启动直接失败,强制走迁移流程

#### 4.8.2 mysql `file_location` 兼容字段

**迁移步骤动作(`MysqlV0ToV1`)**:
1. `CREATE TABLE IF NOT EXISTS node_registry`(如果不存在)
2. `ALTER TABLE file_location` 确保 `datanode_id` 字段存在,且允许为 NULL(过渡期)
3. `CREATE TABLE IF NOT EXISTS schema_version` 并 `INSERT` 当前版本 `1`
4. **不**执行 `UPDATE ... SET datanode_id = ...` 反查补全(反查不到,见 §4.9)

**后续清理**:
- 保留 `datanode_addr` 字段至少一个版本,直到 §4.9 在线补全把所有 `datanode_id IS NULL` 的记录都填上
- 全部补完后,定义 `v1_to_v2`:`ALTER TABLE file_location DROP COLUMN datanode_addr`

### 4.9 node_id 在线补全机制(运行时)

#### 4.9.1 为什么不能在迁移步骤里补 node_id

历史数据只有 `host:port`(物理地址),没有 `node_id`(逻辑身份)。而 `node_id` 由各节点本地 `NodeIdManager.initialize()` 生成(`jnfs-common/NodeIdManager.java:42`),持久化在**节点自己的** `node_id.dat`。迁移步骤在 NameNode 启动时执行,此时:

- 老 DataNode 从未生成过 `node_id.dat`,Registry 里也没有它们的 `node_id`
- 即使等所有 DataNode 都升级启动了,各自生成的 UUID 是**全新的**,与历史 `file_location.dentanode_addr` 之间的关联**已经丢失**

**结论**:node_id 补全必须从"冷数据迁移"改为"节点注册时的运行时在线补全"。

#### 4.9.2 补全触发点:DataNode 心跳注册

DataNode 升级后第一次心跳到 Registry,心跳格式为 `node_id|host:port|freeSpace`。Registry 收到心跳后:

```
1. 写入/更新 node_registry (node_id, host:port)
2. 触发异步补全任务:
     UPDATE file_location
     SET datanode_id = ?
     WHERE datanode_addr = ?           -- 当前心跳的 host:port
       AND datanode_id IS NULL
```

**语义正确性**:
- DataNode 自己启动了 → 它持有 `host:port` → 它能证明"我这个 node_id 现在就是这个地址"
- 历史 `file_location` 里所有 `datanode_addr = 这个 host:port` 的记录,补上这个 `node_id` 是正确的

#### 4.9.3 IP 变更与节点接管

在线补全机制天然支持后续场景:

| 场景 | 行为 |
|---|---|
| 老 DataNode 升级,IP 不变 | 第一次心跳补齐所有 `datanode_addr = 当前 IP` 的记录 |
| DataNode 换 IP 后重启(同 node_id) | 新 IP 心跳补齐 `datanode_addr = 新 IP` 的记录(若有);旧 IP 残留记录保持 NULL |
| 老节点永久下线,新机器接管同 IP | 新机器首次心跳用自己的新 node_id 补齐历史记录 |
| 某地址对应的 DataNode 再也不会上线 | 残留 `datanode_id IS NULL` 的记录由清理任务处理(标记 status=0 或归档),不阻塞 v2 迁移 |

#### 4.9.4 过渡期配套设计

| 项 | 处理方式 |
|---|---|
| 读路径(查文件位置) | `datanode_id IS NULL` 时 fallback 到 `datanode_addr`,与 `RegistryHandler:165` 的 fallback 语义一致 |
| 补全进度监控 | `SELECT COUNT(*) FROM file_location WHERE datanode_id IS NULL` 运维可查 |
| file 模式对应处理 | `namenode_meta.log` 中历史行的 `host:port`,在 NameNode 启动后按 Registry 查到的 `host:port → node_id` 映射在线反查补全;新增行直接写入 `node_id` |
| v1→v2 删除 `datanode_addr` 的前置条件 | `SELECT COUNT(*) FROM file_location WHERE datanode_id IS NULL` 结果为 0 |

---

## 5. 落地计划

按两批 PR 推进,避免单次改动过大。

### 5.1 第一批:框架与可见性

- [ ] 在 `jnfs-common` 新增 `migration` 包:`MigrationStep` / `MigrationContext` / `MigrationResult` / `MigrationRunner`
- [ ] 新增 `schema_version` 表(`mysql/jnfs.sql`)与 file 模式 `meta_version` 文件约定
- [ ] `MigrationRunner` 挂入 `NameNodeServer` / `RegistryServer` 启动入口
- [ ] 此阶段**不**放任何迁移步骤,只验证版本读取与空迁移路径

交付物:能够回答"当前实例数据格式是哪个版本"。

### 5.2 第二批:历史兼容代码改造

- [ ] 实现 `FileV0ToV1`(日志重写 + 一次性分配 storageId)
- [ ] 实现 `MysqlV0ToV1`(DDL only:建 `node_registry` / `schema_version` / `file_location.datanode_id`)
- [ ] 修复 `MetadataManager.recover()` 的非持久化 storageId 补全(违反 INV-1 的现存 bug),改为解析失败即抛异常
- [ ] 实现 `RegistryHandler` 收到 DataNode 心跳时的 `file_location.dentanode_id` 在线补全(§4.9.2)
- [ ] 读路径实现 `datanode_id IS NULL` 时 fallback 到 `datanode_addr`(§4.9.4)
- [ ] 审计上传响应路径,确认 `storage_id` 一定来自持久化层(§3.2)

### 5.3 后续约束(写入贡献指南)

- 任何 PR 若改动 schema 或本地存储格式,**必须**同时新增一个 `MigrationStep`
- 评审 checklist 增加:"是否包含迁移步骤?是否在 file 和 mysql 两种模式下都有效?"
- 禁止在新代码里再出现 `// 兼容旧数据` 这种就地分支

---

## 6. 风险与约束

| 风险 | 缓解措施 |
|---|---|
| 迁移步骤执行到一半断电 | 每步使用临时文件 + rename 或单事务保证原子;步骤必须幂等 |
| file 模式数据量大导致全表扫描慢 | 大表迁移分批,每批后 checkpoint 版本号(此时需要细粒度版本,见 4.2 的扩展设计) |
| 双存储模式实现漂移 | 接口统一,抽象方法在两个模式下都要实现,CI 中各跑一次 |
| 迁移步骤本身有 bug | 失败拒绝启动 + 完整错误日志;提供 `--skip-migration=N` 应急开关但默认禁用,且在日志中显著告警 |
| 集群多节点同时启动并发迁移 | 迁移由各自节点独立执行;集群级协同迁移(分布式锁)留待后续方案 B 演进 |

### 6.1 非目标
- 不处理跨大版本(如 0.x → 1.x)的协议兼容,那属于客户端协议版本化范畴
- 不做不停机滚动升级
- 不做数据回滚工具(向前修复优先于回滚)

---

## 7. 附录

### 7.1 版本演进记录表

> 每次新增迁移步骤时,在本表追加一行,便于全局追溯。

| 版本 | 模式 | 步骤类 | 说明 | 引入版本 |
|---|---|---|---|---|
| 0 → 1 | file | `FileV0ToV1` | 日志格式统一为 `ADD|filename|hash|node_id|storageId` | 0.1.x |
| 0 → 1 | mysql | `MysqlV0ToV1` | `file_location.datanode_id` 回填 | 0.1.x |
| 1 → 2 | mysql | `MysqlV1ToV2` | (规划中)删除 `datanode_addr` 字段 | 0.2.x |

### 7.2 决策记录
- **2026-06-22**:选用方案 A,理由见 §3.1
- **待定**:集群多节点同时迁移的协同机制(可能演进到方案 B)
