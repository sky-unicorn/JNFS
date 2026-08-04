# JNFS 冗余存储与夜间集群同步 — 半设计文档

> 状态：**已定稿**（团队审查通过，3 个开放决策已确认，见 §15）
> 版本：v0.2
> 日期：2026-08-03
> 范围：冗余组、实时写副本、夜间对账同步、副本未就绪拉取策略、管理界面、元数据迁移

---

## 1. 背景与目标

### 1.1 背景

JNFS 当前是「逻辑集群、物理单副本」架构：

- 一个文件 = 一个 SHA-256 hash = 一个 `storageId` = 存在**单个** DataNode 本地（`hash[0:2]/hash[2:4]/hash` 路径，整文件密文落盘）。
- 上传链路：`Driver → NameNode(PRE_UPLOAD) → NameNode 返回单节点(UPLOAD_LOC) → Driver 直连 DataNode 写密文 → COMMIT_FILE 写元数据`。
- 下载链路：`Driver → NameNode 返回单节点(DOWNLOAD_LOC) → 直连读`，**无任何故障转移**。
- DataNode 之间无任何通信、无数据复制、无同步；心跳只上报 `freeSpace`。
- 元数据：MySQL 模式 `file_location` 表 schema **天然支持同一 hash 多行**（`UNIQUE(file_hash, datanode_id)`），但代码只写一行、读用 `LIMIT 1`；File 模式 `namenode_meta.log` 单行 `ADD|filename|hash|node_id|storageId`。

### 1.2 目标（业务需求）

1. **冗余组**：管理界面可将 2~3 个节点配置为一个冗余组（组内最少 2、最多 3，**配置几个就存几份**）；组内节点互备，上传的文件在组内每个节点都有副本。
2. **实时写副本**：上传时 Driver 并发向组内所有节点写副本，副本实时就位。
3. **夜间对账同步**：多 DataNode 从单机模式升级为集群后数据不统一（历史遗留不一致），或实时写副本时部分节点失败（如 2 成 1 败），需在**非高峰时段（凌晨）** 执行对账同步，将缺失副本补齐到组内各节点，并校验节点数据一致性。
4. **副本未就绪时拉取策略**：副本未就绪时（实时写部分失败 / 节点临时不可用），**回退主节点（primary）拉取**，保证读可用。
5. **自动晋升**：主节点宕机时副本自动晋升为新主节点（目标方案，二期实现）。

### 1.3 非目标（MVP 不做）

- **block 分块**：保持「整文件副本」粒度，不引入 HDFS 风格分块（用户需求是整文件双副本，非分块多副本）。
- **quorum 写**：整文件双副本无需 quorum 一致性。
- **机架/机器维度感知**：当前节点无 rack 标签，副本选择按 `host:port` 去重即可。
- **quorum 写强一致**：副本实时写 + 部分成功即提交，不要求全副本强一致（夜间对账兜底）。
- **被动回源**：不启用副本节点实时从 primary 拉取（需求 3 用「回退主节点」解决，见 §8.4）。
- **file 模式冗余**：冗余仅限 mysql 集群模式，file 单机模式不启用冗余（保持单副本）。

---

## 2. 现状分析（关键事实）

| 维度 | 现状 | 对冗余设计的影响 |
|---|---|---|
| 文件粒度 | 整文件单 hash，无分块 | 副本 = 整文件复制，无 block 索引 |
| 放置策略 | `WeightedRandomStrategy.select` 按 `freeSpace` 加权随机返回**1 个**节点 | 需扩展为组成员选择 + primary 去重 |
| 副本概念 | 无。`MySQLMetadataManager.java:234` 注释明确「当前架构只支持单副本」 | 全新引入 |
| 上传链路 | Driver 只向 1 个 DataNode 写 | 需改为向组内所有节点并发写，延迟受最慢节点影响 |
| 下载链路 | NameNode 返回 1 地址，Driver 无故障转移 | **必须补 Driver 端副本回退** |
| 元数据 | `file_location` 多对多 schema 但代码单行；`MetadataEntry.address` 单值 | schema 复用，代码改多行读写 |
| 心跳 | DataNode→Registry 每 5s，payload 仅 `node_id\|host:port\|freeSpace` | 不需上报 block 列表（同步由 NameNode 按元数据驱动） |
| 节点状态 | Registry 内存态，重启即失 | 冗余组配置持久化到 mysql（不依赖 Registry 内存） |
| Dashboard | 只读节点列表 + 安全配置，路由/鉴权完备 | 可直接扩展节点管理/副本配置/同步进度页 |
| Driver API | 仅 `uploadFile` / `downloadFile` | 下载接口需支持多副本列表回退 |
| 迁移框架 | `CURRENT_VERSION=1` | 需升 V2 |

---

## 3. 名词与角色

| 名词 | 定义 |
|---|---|
| **冗余组（Replication Group）** | 2~3 个 DataNode 组成的集合，组内节点互备。副本数 = 组内节点数。 |
| **PRIMARY** | 文件的主副本节点：写入口、读首选、夜间对账的源。每文件唯一。 |
| **SECONDARY** | 文件的次副本节点：只读，实时写就位后可读。 |
| **ACTIVE(1)** | 副本已就位（实时写成功或对账补齐），可对外服务。 |
| **CORRUPT(0)** | 副本损坏/丢失，不可读。 |
| **同步器（Sync Rigger）** | NameNode 内置的夜间对账任务：按元数据计算副本差集（实时写失败 / 历史遗留不一致），驱动 DataNode 间传输补齐并校验一致性。 |

---

## 4. 总体设计

```
┌─────────────────────────────── JNFS 冗余架构 ───────────────────────────────┐
│                                                                             │
│  Dashboard(HTTP)  ──配置──▶  Registry ──冗余组定义──▶  NameNode             │
│   节点管理/副本配置/同步进度                    │  │  │  对账同步器(凌晨)     │
│                                                │  │  │                      │
│  Driver ──上传──▶  NameNode 选组内节点列表                               │
│     │₀              │  ✓ 返回 [primary, secondary1, ...]                  │
│     │₀              │                                                    │
│     ▼              │                                                    │
│  Driver ──并发写──┬─▶ DataNode(PRIMARY)   写密文 ✓                       │
│                   ├─▶ DataNode(SECONDARY) 写密文 ✓                       │
│                   └─▶ DataNode(SECONDARY) 写密文 ✗  ← 部分失败           │
│     │₀              │                                                    │
│     ▼ 成功的节点                                                    │
│  Driver ──COMMIT──▶ NameNode 登记成功副本行                               │
│     │₀              │  ✓ PRIMARY 行 + 成功的 SECONDARY 行                │
│     │₀              │  ✗ 失败的副本 → 留给凌晨对账补齐                    │
│     │₀              │                                                    │
│  Driver ──下载──▶  NameNode 返回[primary, 已就绪副本]                    │
│     │₀              │                                                    │
│     ▼              │                                                    │
│  按序回退 ──────────────▶  PRIMARY ──night 对账──▶  补齐失败副本          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**核心原则：实时写副本 + 部分成功即提交 + 夜间对账兜底。**

- 上传时 Driver **并发向组内所有节点写副本**，延迟 = 最慢节点（非总和）。
- **部分成功即提交**：组内 3 节点写 2 成 1 败 → COMMIT 成功，元数据只记成功的副本行；失败的副本留给夜间对账补齐。
- **夜间对账**：NameNode 凌晨调度，按元数据差集（实时写失败 + 历史遗留不一致）驱动 DataNode 间传输补齐，并校验节点间数据一致性。
- 副本未就绪时（实时写失败 / 节点临时不可用），读请求回退 primary。

---

## 5. 数据模型

### 5.1 MySQL 模式

复用 `file_location` 表（多对多 schema 已具备），新增 2 列：

```sql
-- 无副本语义变更的列，全部幂等（IF NOT EXISTS），供 V1→V2 迁移使用
ALTER TABLE file_location ADD COLUMN replica_role TINYINT NOT NULL DEFAULT 0
  COMMENT '0=PRIMARY, 1=SECONDARY';
ALTER TABLE file_metadata ADD COLUMN replication_factor TINYINT NOT NULL DEFAULT 1
  COMMENT '目标副本数；1=单副本，2/3=组内节点数';
ALTER TABLE file_location ADD INDEX idx_hash_status (file_hash, status);
```

**行语义（关键设计）**：

| 场景 | file_location 行 |
|---|---|
| 上传成功（实时写 N 份，全部成功） | N 行：primary 行 `replica_role=0, status=1` + 副本行 `replica_role=1, status=1` |
| 上传成功（部分失败，如 2 成 1 败） | 仅成功节点行（primary 行 + 成功的副本行）；失败节点**无行**，夜间补齐后登记 |
| 夜间对账补齐 | 追加缺失的副本行：`(file_hash, secondary_node, replica_role=1, status=1)` |
| 副本损坏 | 该行 `status=0` |

- `replication_factor` 由「primary 节点所在冗余组的大小」决定（组内配 2 个节点 → 2，配 3 个 → 3，不在组内 → 1）。
- 查询改造：`queryByHash` 去掉 `LIMIT 1`，返回 `List<MetadataEntry>`，按 `replica_role ASC, status DESC` 排序（PRIMARY 优先、ACTIVE 优先）。

**冗余组配置存储**（用户决策：冗余仅限 mysql 集群模式，配置持久化到 mysql）：

- 新增 `replication_group` 表（mysql）：

```sql
CREATE TABLE IF NOT EXISTS replication_group (
  group_id    VARCHAR(64)  NOT NULL COMMENT '组ID',
  group_name  VARCHAR(128) NOT NULL COMMENT '组名',
  node_ids    VARCHAR(512) NOT NULL COMMENT '组成员node_id列表,逗号分隔(2~3个)',
  create_time DATETIME     NOT NULL,
  update_time DATETIME     NOT NULL,
  PRIMARY KEY (group_id)
);
```

- Dashboard 读写该表；NameNode 定期从 mysql 加载冗余组定义（NameNode 本就连元数据库）。
- **冗余功能仅在 `storage.mode=mysql`（Registry 连元数据库）时启用**；file 模式（单机）不启用冗余，保持单副本。

**对账同步任务表**（决策 10：对账任务持久化，解决 I6）：

- 新增 `replica_sync_task` 表（mysql），NameNode 对账器发现差集后落表，崩溃可恢复：

```sql
CREATE TABLE IF NOT EXISTS replica_sync_task (
  task_id      VARCHAR(64) NOT NULL COMMENT '任务ID',
  file_hash    CHAR(64)    NOT NULL COMMENT '文件hash',
  source_node  VARCHAR(128) NOT NULL COMMENT '源节点(primary)',
  target_node  VARCHAR(128) NOT NULL COMMENT '目标节点',
  status       TINYINT NOT NULL DEFAULT 0 COMMENT '0=PENDING,1=IN_FLIGHT,2=DONE,3=FAILED',
  retry_count  TINYINT NOT NULL DEFAULT 0 COMMENT '累计失败次数(达4告警)',
  file_size    BIGINT NOT NULL DEFAULT 0 COMMENT '文件大小(字节,用于限速与超时)',
  create_time  DATETIME NOT NULL,
  update_time  DATETIME NOT NULL,
  PRIMARY KEY (task_id),
  UNIQUE KEY uk_hash_target (file_hash, target_node),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账同步任务表';
```

- `status` 流转：`PENDING(0) -> IN_FLIGHT(1) -> DONE(2)`；失败回 `PENDING(0)` 且 `retry_count++`。
- `retry_count` 即 §7.8 的 4 次告警计数源（实时写 2 次 + 对账 2 次累计到此）；手动重试（决策 11）将其重置为 0。
- NameNode 启动时扫描 `status IN (0,1)`：`IN_FLIGHT` 视为崩溃中断、回退 `PENDING` 重新派发；`PENDING` 直接恢复派发。

### 5.2 File 模式（不启用冗余）

**用户决策：冗余仅限 mysql 集群模式**。file 模式为单机模式，不启用冗余：

- `namenode_meta.log` 行格式**保持不变**（`ADD|filename|hash|node_id|storageId`），不引入 REPLICA 行。
- 上传/下载链路在 file 模式下走原单副本逻辑（冗余代码在 file 模式短路）。
- `FileV1ToV2` 迁移为 **no-op**（仅升版本号，不改变日志格式），保证版本号全局统一。

### 5.3 迁移 V1 → V2

遵守 `.claude/rules/schema-migration.md` 强制规则：

| 项 | 值 |
|---|---|
| `MigrationRunner.CURRENT_VERSION` | 1 → **2** |
| 新增步骤 | `MysqlV1ToV2`、`FileV1ToV2` |
| 注册文件 | `META-INF/migrations/mysql_v1_to_v2.properties`、`file_v1_to_v2.properties` |
| `mysql/jnfs.sql` | 同步更新为最新 schema（含 `replica_role`、`replication_factor`、`idx_hash_status`、`replication_group`、`replica_sync_task` 表） |

- **MysqlV1ToV2**：执行 §5.1 三条 ALTER（幂等，通过 `information_schema` 检查列/索引是否存在，参考 `MysqlV0ToV1` 模式）+ 建 `replication_group` 表 + 建 `replica_sync_task` 表（决策 10）；存量行 `replica_role=0`（视为 PRIMARY）、`replication_factor=1`。**不假定 `datanode_id NOT NULL`**——V0→V1 的 node_id 回填是在线异步的，升级瞬间仍有 NULL 行，保持 NULL 不强制回填。
- **FileV1ToV2**：**no-op**（仅升版本号），file 模式不启用冗余、不改日志格式。
- 四项不变式：INV-1/2（storage_id 不变）、INV-3（幂等）、INV-4（失败 `System.exit(2)`）均需满足。

---

## 6. 上传链路（实时写副本 + 部分成功即提交）

### 6.1 节点选择

`REQUEST_UPLOAD_LOC` 由「返回单节点」改为「返回组内节点列表」：

```java
// NameNode 侧：返回组内所有可用节点（含 primary）
List<String> selectReplicaTargets(String fileHash, List<String> candidates) {
    String primary = selectPrimary(fileHash, candidates);          // 复用加权随机，排除已持有该文件的节点
    ReplicationGroup group = replicationGroupOf(primary);          // primary 所在冗余组
    if (group == null) return [primary];                            // 不在组内 → 单副本
    List<String> members = group.members();                          // 组内全部节点
    return [primary] + members.filter(n -> n != primary);           // primary 恒第一
}
```

- 响应格式从 `host:port` 扩展为 `primary|secondary1|secondary2`（`|` 分隔，第一段恒为 primary，兼容老 Driver 取首段）。

### 6.2 Driver 并发写多份

```java
// Driver 侧：并发向所有目标节点写密文
List<String> targets = parseTargets(uploadLocResponse);   // [primary, sec1, sec2]
List<Future<WriteResult>> futures = new ArrayList<>();
for (String addr : targets) {
    futures.add(executor.submit(() -> uploadToDataNodeWithRetry(addr, hash, ciphertext)));
}

List<String> succeeded = new ArrayList<>();   // 成功的节点
List<String> failed = new ArrayList<>();       // 失败的节点
for (int i = 0; i < futures.size(); i++) {
    try {
        futures.get(i).get(perNodeTimeout, MS);   // 并发，等最慢的
        succeeded.add(targets.get(i));
    } catch (Exception e) {
        failed.add(targets.get(i));                // 部分失败，不中断其他
    }
}

// primary 必须成功（否则整个上传失败，用户重试）
if (!succeeded.contains(primary)) throw new UploadFailedException("primary 写失败");
// 部分成功即提交：COMMIT 时只登记 succeeded 列表
commitFile(filename, hash, succeeded);

// uploadToDataNodeWithRetry: 单节点写失败立即重试1次，仍失败则抛异常（用户决策）
WriteResult uploadToDataNodeWithRetry(addr, hash, ciphertext) {
    try { return uploadToDataNode(addr, hash, ciphertext); }
    catch (Exception e1) {
        try { return uploadToDataNode(addr, hash, ciphertext); }   // 重试1次
        catch (Exception e2) { throw e2; }                          // 仍失败，留给夜间对账
    }
}
```

- **并发而非串行**：延迟 = 最慢节点，而非总和（用户决策）。
- **primary 必须成功**：primary 是写入口和读首选，primary 失败则整个上传失败。
- **副本部分失败可接受 + 重试1次**：secondary 写失败立即重试 1 次，仍失败则不阻塞上传，留给夜间对账补齐（用户决策）。
- **仅 mysql 模式启用**：file 模式下冗余短路，走原单副本逻辑。

### 6.3 元数据登记（COMMIT_FILE）

NameNode 收到 `COMMIT_FILE`（payload 含成功的节点列表）后，**为每个成功节点登记一行**：

- primary 行：`replica_role=0, status=1`
- 成功的 secondary 行：`replica_role=1, status=1`
- 失败的 secondary：**无行**，夜间对账发现差集后补齐。

### 6.4 秒传路径

`PRE_UPLOAD` 命中秒传时，返回当前 PRIMARY 地址，**不触发副本补齐**。旧文件若副本不足（如历史遗留单副本），交给夜间对账统一补。

### 6.5 失败副本的登记时机

实时写失败的副本**不在上传链路登记**，由夜间对账器（§7）发现 `replication_factor > 实际副本数` 后补齐并登记。

---

## 7. 夜间对账同步机制

### 7.1 触发

- **NameNode 单点调度**（非 DataNode 自驱，避免多节点全量扫描脑裂/带宽风暴）。
- **核心窗口 01:00-03:00**（用户决策）：高优先级、正常限速补齐副本。
- **03:00 后软截止**（用户决策）：切换到低资源模式（降并发、降带宽），继续处理未完成任务直到跑完为止，不强制中断。
- cron 默认 `0 0 1 * * ?`（凌晨 01:00 启动），低资源阈值时间可配（默认 03:00）。
- 用 `ScheduledExecutorService` + `DaemonThreadFactory`（项目规范）。
- **职责定位：对账补救，不是副本唯一来源。** 副本由上传链路实时写就位，对账只补两类缺失。

### 7.2 同步源与差集计算

NameNode 元数据是唯一事实源。**差集来源两类**：

1. **实时写失败**：上传时部分副本节点写失败（无行），目标副本数未满足。
2. **历史遗留不一致**：单机改集群前的存量文件（原各节点独立数据，副本数不足），或新增节点入组后未补齐的历史数据。

```java
for each (storage_id, file_hash, primary_node) in file_metadata:
    expected = replicationFactor(file_hash);               // 该文件目标副本数（组大小 or 1）
    actual   = file_location.count(file_hash, status=ACTIVE);
    if (actual >= expected) continue;
    missing  = expected - actual;
    for target in chooseSecondaryNodes(primary_node, missing):  // 组内除已持有节点外
        enqueueReplicaJob(file_hash, primary_node, target);
```

**一致性校验**：对账时同时检测两类异常——

- 「元数据有行但节点本地无文件」→ 缺失副本，补齐（status 标记后重新拉取）。
- 「节点本地有文件但元数据无行」→ 孤儿文件，记录告警，**不自动删除**（MVP）。

### 7.3 DataNode 间传输（补齐缺失副本）

**方案：目标 DataNode 从源 DataNode 直连拉取**（少一次 NameNode 转发跳数，大文件带宽不经过 NameNode）。

- 架构师 A 论证：方案 X（NameNode 转发）会让 NameNode 成为瓶颈；方案 Y 下 NameNode 只负责协调与进度记录。
- 源节点固定为 primary（副本最权威），目标为组内缺失副本的节点。

### 7.4 新增 CommandType

```java
DATA_REPLICA_PULL_REQUEST   // target -> source: "我要拉 hash H"
DATA_REPLICA_PULL_RESPONSE  // source -> target: 返回文件长度
DATA_REPLICA_CHUNK          // source -> target: 密文流（复用现有 ByteBuf 流式）
DATA_REPLICA_COMMIT         // target -> NameNode: 我已持有 H，登记 ACTIVE
```

### 7.5 幂等与去重

- `file_location.uk_hash_node` 保证同一 `(file_hash, datanode_id)` 唯一；同步前 `INSERT IGNORE`。
- 同一任务重复执行天然幂等（已 ACTIVE 的行不再补）。

### 7.6 限速与并发

- **核心窗口（01:00-03:00）**：全局带宽上限默认 50 MB/s，并发任务数默认 4。
- **软截止后（03:00 起，低资源模式）**：带宽上限降为 10 MB/s，并发任务数降为 1（用户决策：较低资源继续跑完为止）。
- 单任务按 chunk 检查当前速率，超限 `Thread.sleep`。

### 7.7 进度可观测

- 任务队列持久化到 mysql `replica_sync_task` 表（决策 10，解决 I6）；内存中维护 `replicaSyncQueue`（待派发）+ `failedReplicaQueue`（告警）作快速读取镜像。
- NameNode 启动时扫描 `replica_sync_task` 中 `status IN (0,1)` 恢复未完成任务：`IN_FLIGHT` 回退 `PENDING` 重派，`PENDING` 直接恢复。
- Dashboard 通过 `/api/replication/sync` 读：`{summary:{total_pending,synced_today,failed,current_jobs}, current_jobs[], failed_jobs[], alerts[]}`（详见 §16.7）。

### 7.8 失败重试

两类失败，重试策略不同：

- **实时写失败（上传链路，用户决策）**：副本节点写失败后**立即重试 1 次**；仍失败则不阻塞上传（部分成功即提交），该副本留给夜间对账补齐。
- **对账补齐失败（用户决策）**：夜间对账发现某副本缺失后，**立即对同一节点再重试 1 次**；仍失败则进入死信队列 → Dashboard 异常告警。
- **告警阈值：连续 4 次失败**（实时写 1 次 + 实时写重试 1 次 + 对账 1 次 + 对账再重试 1 次），此时可判定节点/网络存在持续问题，需人工介入。`replica_sync_task.retry_count` 为计数源。
- **手动重试重置计数器（决策 11）**：Dashboard 手动重试（`POST /api/replication/sync/retry/{taskId}`）将该任务 `retry_count` 重置为 0，视为运维介入后重新开始 4 次窗口。手动重试达 `maxRetries`（默认 4）仍失败时按钮灰显。

---

## 8. 副本未就绪时拉取策略（需求 3）

**用户决策：回退主节点（primary）。**

由于副本是**实时写**的，正常情况下副本已就位。需回退 primary 的场景：

1. **实时写部分失败**：上传时某副本节点写失败（无副本行 / 节点本地无文件）。
2. **节点临时不可用**：副本节点宕机或网络抖动。
3. **历史遗留**：单机改集群的存量文件，夜间首次对账前副本未补齐。

回退 primary 保证读可用，不启用被动回源（见 §8.4）。

### 8.1 NameNode 返回结构

`handleDownloadLocRequest` 当前返回 `filename|hash|host:port`，扩展为有序列表：

```
filename|hash|primary_addr|replica1_addr|replica2_addr
```

- **前两段不变**（兼容老 Driver，老 Driver 至少能拿 primary）。
- primary 恒第一位；后续为已就绪副本（`file_location` 中 `status=1` 的副本行，按 `replica_role ASC, status DESC` 排序）。
- **未就绪副本不在列表中**（实时写失败无行 → 天然排除），Driver 不会白试。

### 8.2 Driver 故障转移（必须补，当前完全缺失）

```java
// 新协议: filename|hash|primary|replica1|replica2
String[] parts = locInfo.split("\\|");
List<String> candidates = new ArrayList<>();
for (int i = 2; i < parts.length; i++) if (!parts[i].isEmpty()) candidates.add(parts[i]);

IOException last = null;
for (String addr : candidates) {
    try {
        downloadFromDataNode(host(addr), port(addr), hash, targetFile);
        return targetFile;                       // 成功即返回
    } catch (IOException | RuntimeException e) { // 含 HMAC 校验失败
        last = wrap(e, addr);
        backoff(candidates.indexOf(addr));       // 指数退避 200ms/400ms/800ms
    }
}
throw last;   // 全部失败
```

- **HMAC 校验失败也必须触发切换**（副本可能被篡改，不能视为最终成功）。
- 超时：per-node（连接 6s + 传输按文件大小动态计算，仿 upload 的 `60 + fileSize/51200`），全局超时 = 节点数 × per-node。

### 8.3 未就绪判定

- **元数据视角**：实时写失败的副本无行，查询即排除（不会返回给 Driver）。
- **物理视角兜底**：副本节点临时宕机但行仍在（`status=1`）→ Driver 尝试连接失败 → 切下一副本；或副本行 `status=1` 但节点本地文件丢失（极端）→ DataNode 返回 `ERROR`，Driver 捕获后切下一副本。
- **MVP**：不新增 `ERROR_NOT_REPLICATED` 错误码（复用 `ERROR`，Driver 统一回退即可）；可观测性靠 DataNode 端日志区分「副本未就绪」与「文件损坏」。二期可加独立错误码。

### 8.4 冷启动存量数据与被动回源

- **冷启动存量**：单机改集群前的存量文件，夜间首次对账补齐副本。对账前 primary 正常则读不受影响（回退 primary 兜底）。
- **不启用被动回源**（否决架构师 B 的 PROXY_DOWNLOAD 方案）：被动回源让副本节点实时从 primary 拉取，本质是「即时懒加载副本」，与「夜间对账」语义冲突，且引入 DataNode→NameNode 反向依赖。需求 3 已由「回退 primary」解决，被动回源为高成本低收益，MVP 不做。

---

## 9. 容错与故障切换

### 9.1 primary 宕机

由于副本是**实时写**的，primary 宕机时**组内其他副本已有数据**（区别于旧设计「副本未同步」），故障切换更可靠：

- **短期（分钟级）**：Driver 回退到已就绪副本，对应用透明。
- **长期（小时级）自动晋升（用户决策，二期）**：

  1. 依赖 Registry 心跳超时（30s）判定 primary 失联；
  2. 组内已就绪副本竞选（`replica_role=1 AND status=1`），**选主策略：持有副本数最多的节点优先**（用户决策，数据最全者晋升）；副本数相同则取 node_id 字典序最小者 tie-break。
  3. 晋升：`UPDATE file_location SET replica_role=0 WHERE datanode_id=? AND file_hash IN (...)`；**原 primary 恢复后自动降级为 SECONDARY 并重新同步**（用户决策，无需人工确认）。

- **MVP 过渡**：已就绪副本可读（Driver 回退）；**手动晋升**（Dashboard 一键把副本升为 primary）作为兜底，自动选举留二期。

### 9.2 副本节点宕机

- 对账同步跳过该节点并记录失败队列；`file_location` 行**保留不删**（`status=0`）。
- 节点恢复后重新补同步（幂等）；行存在使对账器能判断「曾经有副本」的目标副本数。

### 9.3 冗余组配置持久化（用户决策：mysql）

- Registry 节点状态纯内存，重启即失；但**冗余组配置持久化到 mysql `replication_group` 表**（见 §5.1），不依赖 Registry 内存。
- Dashboard 读写 mysql，NameNode 定期从 mysql 加载冗余组定义，Registry 重启不影响冗余组配置。

---

## 10. 管理界面

> **进程归属（决策 9，解决 C1）**：管理 API 的 HTTP 端点由 **Registry 进程的 `DashboardServer` 提供**（复用现有 `AuthFilter` 鉴权），Registry 新增元数据库 DataSource 读写 `replication_group`/`replication_policy`/`replica_sync_task` 表。NameNode 仍定期从 mysql 加载冗余组定义。**不引入 NameNode HTTP 服务**，避免双 HTTP 端口运维负担。

所有写接口走 `addProtected` 鉴权（复用现有 `AuthFilter`）。

### 10.1 新增页面/接口

| 路由 | 方法 | 说明 |
|---|---|---|
| `/api/replication/groups` | GET/POST | 冗余组配置（勾选 2~3 节点为一组，校验不重复、不重叠） |
| `/api/nodes/{id}/drain` | POST | 标记节点排空（后续上传不选它，副本角色降级） |
| `/api/replication/policy` | GET/PUT | 同步窗口（核心 01:00-03:00、软截止时间）、限速、并发；PUT 持久化到 mysql |
| `/api/replication/sync` | GET | 同步进度（`total_pending/synced/failed/current_jobs`） |
| `/api/nodes/{id}/promote` | POST | 手动晋升副本为 primary（MVP 兜底） |

### 10.2 审计

- MVP：logback 结构化日志 `time|user|action|target|result`（如 `2026-08-03T02:15|admin|DRAIN|node-3|OK`）。
- 二期：接入 `audit_log` 表。

### 10.3 副本数改小（不自动删）

- 冗余组从 3 节点改为 2 节点时，**同步器不删除多余副本**（同步是补、不是收；删除是不可逆破坏操作）。
- 检测到超副本文件时仅记告警「N 个文件副本数超出策略」，由管理员手动清理（二期提供 `/api/replication/prune`）。

---

## 11. 决策确认与待定问题

### 11.1 已确认决策（用户回复）

| # | 决策点 | 结论 |
|---|---|---|
| 1 | 冗余组数量 | **支持多个冗余组**（各组独立，组间节点不重叠） |
| 2 | 冗余范围与配置存储 | **冗余仅限 mysql 集群模式**；冗余组配置持久化到 **mysql `replication_group` 表**；file 模式不启用冗余 |
| 3 | 同步窗口 | **核心窗口 01:00-03:00**；03:00 后**软截止**，低资源模式继续跑完为止 |
| 4 | 副本不足告警阈值 | **待确认**（见 §11.2） |
| 5 | 自动晋升选主策略 | **持有副本数最多的节点优先**；相同则 node_id 字典序最小 tie-break |
| 6 | 原 primary 恢复 | **自动降级为 SECONDARY 并重新同步**，无需人工确认 |
| 7 | 对账期间未就绪读取 | **接受**回退 primary |
| 4 | 副本不足告警阈值 | **连续 4 次失败后告警**：实时写 1 次 + 实时写重试 1 次 + 夜间对账 1 次 + 对账失败立即再重试 1 次 |
| 8 | 实时写副本失败处理 | **立即重试 1 次**，仍失败则留给夜间对账补齐；对账失败也立即再重试 1 次 |
| 9 | 管理 API 进程归属 | **复用 Registry 进程连元数据库**（新增 DataSource 读写 `replication_group`/`replication_policy`/`replica_sync_task` 表；NameNode 定期从 mysql 加载冗余组定义） |
| 10 | 对账任务持久化 | **落 `replica_sync_task` 表 + NameNode 启动恢复**（对账进度不因 NameNode 崩溃丢失） |
| 11 | 手动重试与告警计数 | **手动重试重置 4 次失败计数器**（视为运维介入后重新开始计数窗口） |

**关于冗余组用途（决策 1 补充说明）**：用户表示"冗余组的主要干什么尚未完全想好"。当前文档定义的用途为：组内节点互为副本，上传到组内某节点的文件会在组内所有节点都有副本（容灾 + 读分流）。多组用于隔离管理（如不同业务/物理位置/性能等级）。若后续用途明确，可调整组语义。

### 11.2 待确认问题

**问题 4：副本不足告警阈值**（用户要求详细描述）

**为什么需要这个阈值**：由于「部分成功即提交」+「夜间对账」，某个文件「实际副本数 < 期望副本数」是一个**常见的暂时状态**——例如：

- 上传时某副本节点写失败（重试 1 次仍失败）→ 该副本暂缺，等凌晨对账补。
- 凌晨对账时该节点仍在宕机 → 本轮没补上，等下一轮。

如果**每次**副本数不足都立即告警，会产生大量噪音告警（每天每个写失败的文件都报一次）。因此需要一个判定阈值，区分两种情况：

- **暂时不足（正常，不告警）**：副本数低于期望，但还没到"该报警"的程度。
- **长期不足（异常，告警）**：副本数低于期望已经持续足够久，说明对账也补不上，需要运维人工介入（可能节点永久故障、磁盘满等）。

**具体要决定的是这个"足够久"的判定标准**，有三种可选维度：

| 维度 | 含义 | 示例 |
|---|---|---|
| 时间维度 | 副本不足持续超过 N 小时 | 如持续 > 24h / 48h / 72h 才告警 |
| 对账轮次维度 | 跨过 N 个对账窗口仍未补齐 | 如跨过 1 个窗口（即次日凌晨对账后仍缺）才告警 |
| 失败次数维度 | 对账补齐该文件连续失败 N 次 | 如连续失败 3 次才告警 |

**已确认答案（用户决策）**：按**连续失败次数**判定，**4 次失败后告警**。

计算方式：
1. 实时写：第 1 次失败
2. 实时写立即重试：第 2 次失败
3. 夜间对账补齐：第 3 次失败
4. 对账失败立即再重试：第 4 次失败
→ 第 4 次失败后升级为异常告警，人工介入。

**理由**：4 次连续失败足以说明不是瞬时抖动（网络闪断、节点瞬时繁忙），而是持续性的节点/网络/磁盘问题，需要人工排查。

**影响**：该策略对账一旦发现缺失会立即再试，告警更及时；但需确保对账重试不会把正常慢节点误判为失败（需结合超时阈值）。

---

## 12. 实施分期

### MVP（一期）

1. 数据模型：`file_location` 加 `replica_role`、`file_metadata` 加 `replication_factor`、索引、`replication_group` 表；迁移 V1→V2（mysql 模式 ALTER + 建表，file 模式 no-op）。
2. 上传链路（仅 mysql 模式）：NameNode 返回组内节点列表 + Driver 并发写多份 + 失败重试1次 + 部分成功即提交（primary 必须成功）。
3. 夜间对账器（仅 mysql 模式）：NameNode 调度（核心 01:00-03:00 + 软截止低资源续跑）、差集计算（实时写失败 + 历史遗留）、DataNode 直连拉取、幂等、限速、进度队列。
4. 下载链路：NameNode 返回有序列表 + Driver 故障转移回退。
5. 管理界面：冗余组配置（mysql 持久化）、节点 drain、对账进度、手动晋升、同步策略配置。
6. 冷启动存量：单机改集群的存量数据夜间首次对账全量补齐。

### 二期

1. **自动晋升**（用户决策）：Registry 心跳超时检测 + 副本竞选（副本数最多优先）+ 角色翻转 + 原 primary 自动降级。
2. `ERROR_NOT_REPLICATED` 独立错误码 + 审计日志表 + 超副本清理接口。

---

## 13. 风险与影响

| 风险 | 影响 | 缓解 |
|---|---|---|
| 迁移破坏存量 | File 模式 ADD 行解析、MySQL 存量行 | 不改 ADD 行、`datanode_id` 允许 NULL、幂等 ALTER |
| 双副本同时故障丢数据 | 关键文件不可恢复 | 关键数据可配 3 副本组 |
| 夜间对账带宽打满 | 影响其他业务 | 核心窗口限速 50MB/s + 并发 4；软截止后降为 10MB/s + 并发 1 |
| primary 宕机期间写 | 实时写时该文件 primary 写失败 → 整个上传失败 | 用户重试；其他副本已就位的可由对账补 primary 角色 |
| 冗余组误配（同物理机） | 冗余失效 | 组配置校验提示同 host 节点 |
| 副本数改小后残留 | 超副本文件 | 仅告警不自动删，人工清理 |

---

## 14. 参考文件

| 文件 | 说明 |
|---|---|
| `jnfs-namenode/src/main/java/org/jnfs/namenode/NameNodeHandler.java` | `handleUploadLocRequest`(224)、`handleDownloadLocRequest`(305)、`handleCommitFile`(241) 需改造 |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/MySQLMetadataManager.java` | `queryByHash`(106) `LIMIT 1` 改多行；`logAddFile`(265) 加副本行 |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/MetadataCacheManager.java` | `MetadataEntry.address`(145) 单值改 `List<String>` |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/WeightedRandomStrategy.java` | 复用于 primary 选择，加排除已有副本节点 |
| `jnfs-driver/src/main/java/org/jnfs/driver/JNFSDriver.java` | `downloadFile`(434) 加故障转移回退 |
| `jnfs-common/src/main/java/org/jnfs/common/CommandType.java` | 新增 `DATA_REPLICA_*` 命令 |
| `jnfs-common/src/main/java/org/jnfs/common/migration/MigrationRunner.java` | `CURRENT_VERSION`(34) 升 2 |
| `jnfs-registry/src/main/java/org/jnfs/registry/DashboardServer.java` | 新增节点管理/副本配置/同步进度路由 |
| `mysql/jnfs.sql` | `file_location`(49) 加列、`file_metadata` 加列、索引 |

---

## 15. 团队审查结论与待确认项

> 2026-08-03 团队审查报告：后端工程师、网络技术专家、UI+前端工程师独立审查，挑刺专家交叉审查，架构师汇总。
> 完整审查报告见 `docs/design/redundant-storage-review.md`。

### 15.1 必须修复的致命问题（实施前）

| # | 问题 | 处理 |
|---|---|---|
| **C1** | Dashboard 进程归属：`DashboardServer` 在 `RegistryServer` 启动，Registry 不连元数据库，但 §10 要求 Dashboard 读写 `replication_group` | 复用 Registry 连元数据库（新增 DataSource，registry 端连接 `jnfs` 库做冗余组读写；元数据主写入仍由 NameNode 负责）；Dashboard HTTP 仍由 Registry 提供 |
| **C2** | `MetadataCacheManager` 是 `Cache<String, MetadataEntry>` 单值，无法承载多副本 | 重构为 `Cache<String, List<MetadataEntry>>`；`MetadataEntry` 增 `List<String> addresses` + `List<Integer> roles`；`put()` 改多地址；`handleDownloadLocRequest/handlePreUpload/handleCheckExistence` 全部同步改 |
| **C3** | `MySQLMetadataManager` 构造函数 DDL 缺 `status` 列，与 `jnfs.sql` 不一致 | 建表 DDL 收敛到迁移框架：构造函数只保留 `CREATE TABLE IF NOT EXISTS` 最小骨架，所有列定义由 `MysqlV0ToV1`（已存在）+ `MysqlV1ToV2`（新增）负责；`jnfs.sql` 与迁移一致 |

### 15.2 重要问题（实现前必须澄清）

| # | 问题 | 处理 |
|---|---|---|
| I1 | `handleUploadLocRequest` 不接收 fileHash | 协议改：Driver 在 `REQUEST_UPLOAD_LOC` 携带 hash（`new byte[0]` → `hash.getBytes()`） |
| I2 | `WeightedRandomStrategy.select` 返回 `host:port` 丢 `node_id` | 扩展 `LoadBalancer` 接口返回结构化对象，或上层用 `NodeAddressResolver.getNodeId(hostPort)` 反查 |
| I3 | 下载跨节点退避 200/400/800ms 不合理 | 去掉跨节点退避，改为立即尝试下一节点；退避仅用于同一节点重试 |
| I4 | 老 Driver 校验 `parts.length != 3`（`JNFSDriver:439`），5 段返回会炸 | 见 §15.3 协议兼容性 |
| I5 | DataNode 无 NameNode 连接能力 | DataNode 新增 `namenodePoolMap`（复用 `ChannelPoolUtils`）+ `NettyClientBootstrap`；复用 `SecurityConfig.getToken()` 鉴权 |
| I6 | 对账队列内存态，NameNode 崩溃即丢 | **待用户决策**（见 §15.5） |

### 15.3 协议兼容性（破坏性变更声明）

三处协议变更为**破坏性变更**，Driver 与 NameNode 必须同步升级，不支持灰度：

| 命令 | 现格式 | 新格式 |
|---|---|---|
| `RESPONSE_UPLOAD_LOC` | `host:port` | `primary\|sec1\|sec2`（`\|` 分隔） |
| `COMMIT_FILE` | `filename\|hash\|addr`（3 段） | `filename\|hash\|addr1,addr2,addr3`（`addr` 用 `,` 分隔以区分外层 `\|`） |
| `RESPONSE_DOWNLOAD_LOC` | `filename\|hash\|host:port`（3 段） | `filename\|hash\|primary\|replica1\|replica2` |

Driver 同步升级接收多值；老客户端不能混用。设计文档 §8.1 原"前两段兼容老 Driver"表述不成立，删除。

### 15.4 UI 设计规格（完整版见 §16）

UI 采用**两级 Tab** 结构（节点监控 \| 冗余存储管理；子 Tab：冗余组管理 \| 对账同步 \| 同步策略 \| 告警），新增 12 个 API 端点：

| 端点 | 方法 | 用途 |
|---|---|---|
| `/api/replication/groups` | GET/POST | 冗余组列表/创建 |
| `/api/replication/groups/{id}` | PUT/DELETE | 冗余组修改/删除（§10 漏，由 UI 补） |
| `/api/nodes/{id}/drain` | POST | 节点排空/恢复（`{"drain": true\|false}`） |
| `/api/nodes/{id}/promote` | POST | 手动晋升副本（MVP 兜底） |
| `/api/replication/policy` | GET/PUT | 同步策略配置 |
| `/api/replication/sync` | GET/POST | 同步进度/手动触发全量 |
| `/api/replication/sync/retry/{taskId}` | POST | 手动重试失败任务（**重置 4 次计数器**） |
| `/api/replication/alerts` | GET | 活跃/已恢复告警 |

**关键修正**（挑刺专家 A5 发现）：UI 不能将 `role: primary/replica` 展示为**节点属性**，因为 PRIMARY/SECONDARY 是**文件级副本角色**。组管理页只显示组成员 `node_id` 列表，不显示 role。

### 15.5 已确认决策（用户定稿 2026-08-03）

经合并审视 9 个候选待确认项后，3 个需用户拍板的决策已全部确认采用推荐方案：

| # | 决策点 | 确认方案 | 理由 |
|---|---|---|---|
| **U1** | 管理 API 进程归属（解决 C1） | **复用 Registry 进程连元数据库** | 改动最小，不引入 NameNode HTTP 服务；Registry 新增 DataSource 读写 `replication_group`/`replication_policy`/`replica_sync_task` 表，NameNode 仍定期从 mysql 加载冗余组定义 |
| **U2** | 对账任务持久化（解决 I6） | **落 `replica_sync_task` 表 + NameNode 启动恢复** | 凌晨对账期间 NameNode 崩溃不至于全部丢失；NameNode 启动时扫描 `replica_sync_task` 中 `status=PENDING/IN_FLIGHT` 的任务恢复或重算 |
| **U3** | 手动重试与告警计数语义 | **手动重试重置 4 次计数器** | 视为运维介入后重新开始 4 次窗口，避免计数错乱 |

其余 6 项已由架构师/工程师自主决定（端口复用 5369、不建 DataNode 间连接池、限速 1MB 粒度、`replication_factor` 写入时快照、并发写共享 `.enc`、DataNode 用 `SecurityConfig.getToken()` 发 COMMIT）。

### 15.6 工程师遗漏问题（已补入设计）

| # | 问题 | 处理 |
|---|---|---|
| M1 | 三处协议变更破坏灰度 | §15.3 明确"破坏性变更，同步升级" |
| M2 | Driver 并发写线程池生命周期 | Driver 实例级 `replicaWriteExecutor`（`Executors.newFixedThreadPool(3, new DaemonThreadFactory("Driver-ReplicaWrite"))`，`close()` 时 shutdown） |
| M3 | mysql 模式无冗余组的降级路径 | §6.1 伪代码 `if (group == null) return [primary]` 已对，新增单元测试覆盖 |
| M4 | `DATA_REPLICA_PULL` 无完整性校验 | target 拉完比对 `data.length == pullResponse.fileLength`；若不一致视为拉取失败 |
| M6 | 组改小后对账反向补齐 | `replication_factor` 写入时快照，对账按行内快照判定，不查当前组大小（与 §10.3"组改小不删副本"语义一致） |

---

## 16. UI 设计规格（完整版）

> 设计人：UI 工程师 + 前端工程师，2026-08-03
> 风格：沿用现有 Dashboard 卡片+表格+弹窗风格，原生 HTML/CSS/JS（无框架），CSS 变量体系复用。

### 16.1 整体布局

```
+======================================================================+
|  JNFS 运维监控中心                              [修改密码] [登出]    |
+======================================================================+
|  [ 节点监控 | 冗余存储管理 ]                                         |
+----------------------------------------------------------------------+
```

**Tab 切换**：纯 CSS + JS `display:none/block`，无页面刷新。

冗余存储管理 Tab 内部再分 4 个子 Tab：

```
|  [ 冗余组管理 | 对账同步 | 同步策略 | 告警 ]                         |
```

### 16.2 节点监控 Tab 增强

现有节点列表表格**新增"操作"列**：

```
+--------------------------------------------------------------+
| 节点ID | 节点地址 | 剩余空间 | 最后心跳 | 状态  | 操作       |
|--------|----------|----------|----------|-------|-----------|
| node-1 |10.0.0.1:9300| 500 GB| 10:30:00|[在线] |[排空][晋升]|
| node-2 |10.0.0.2:9300| 300 GB| 10:30:01|[在线] |[排空]      |
| node-3 |10.0.0.3:9300| 200 GB| 08:00:00|[离线] |[晋升]      |
+--------------------------------------------------------------+
```

**规则**：
- 排空按钮：仅在线可点击；离线灰显
- 晋升按钮：仅副本角色可点击；primary 灰显
- 排空中的节点状态徽标变橙色（`status-draining`）

### 16.3 冗余组管理子 Tab

```
+--------------------------------------------------------------+
|  冗余组管理                              [+ 创建冗余组]        |
+--------------------------------------------------------------+
|  +----------------------------------------------------------+ |
|  | 组ID     | 节点成员            | 状态   | 操作           | |
|  |----------|---------------------|--------|----------------| |
|  | rg-001   | node-1 node-2       | 正常   | [编辑][删除]   | |
|  | rg-002   | node-3 node-4 node-5| ⚠ 同host|[编辑][删除]   | |
|  +----------------------------------------------------------+ |
+--------------------------------------------------------------+
```

**创建/编辑弹窗校验规则**：
1. 2~3 个节点（不足/超限禁止确认）
2. 不重叠（节点已属其他组 → 红色提示）
3. 同 host 告警（仅警告，不阻止）
4. 离线节点 checkbox disabled

**注意（挑刺修正）**：组成员列表**不显示 role**（PRIMARY/SECONDARY 是文件级，不存在节点级）。

### 16.4 对账同步子 Tab

```
+--------------------------------------------------------------+
|  对账同步                               [手动触发全量对账]     |
+--------------------------------------------------------------+
|  +------------+ +------------+ +------------+ +------------+  |
|  | 待同步任务 | | 已完成     | | 同步失败   | | 当前执行中 |  |
|  |     12     | |    156     | |     2      | |     3      |  |
|  +------------+ +------------+ +------------+ +------------+  |
|                                                                |
|  同步进度                                                      |
|  [████████████████████░░░░░░░░░░░░░░░░] 56% (168/300)        |
|                                                                |
|  当前同步任务 / 最近失败任务（表格）                            |
+--------------------------------------------------------------+
```

**轮询**：每 5 秒，仅当前可见 Tab 发起请求。

### 16.5 同步策略子 Tab

```
+--------------------------------------------------------------+
|  同步策略配置                              [保存配置]          |
+--------------------------------------------------------------+
|  核心同步窗口                                                  |
|  开始时间  [ 01 ] : [ 00 ]    结束时间  [ 03 ] : [ 00 ]      |
|  软截止时间  [ 02 ] : [ 30 ]                                  |
|                                                                |
|  传输限制                                                      |
|  限速 (MB/s)   [  50  ]     (0 = 不限速)                      |
|  最大并发数    [   3  ]     (1~10)                             |
+--------------------------------------------------------------+
```

时间用 `<select>` 下拉（小时 00-23，分钟 00/15/30/45），避免自由输入。

### 16.6 告警子 Tab

```
+--------------------------------------------------------------+
|  告警                                                        |
+--------------------------------------------------------------+
|  +------------+ +------------+                                 |
|  | 活跃告警   | | 已恢复告警 |                                 |
|  |     2      | |     5      |                                 |
|  +------------+ +------------+                                 |
|                                                                |
|  活跃告警 / 已恢复告警（表格：级别/内容/冗余组/触发/恢复时间） |
+--------------------------------------------------------------+
```

**告警来源**：
- 严重：某节点连续 4 次同步失败
- 警告：冗余组内存在同 host 节点

顶部 Tab 标签旁显示活跃告警红色圆点数字（如 `告警 (2)`）。

### 16.7 API 端点（完整列表）

| 端点 | 方法 | 请求体 | 成功响应 | 失败响应 |
|---|---|---|---|---|
| `/api/replication/groups` | GET | — | `{groups: [...]}` | — |
| `/api/replication/groups` | POST | `{groupId, nodeIds[]}` | `{success: true, group: {...}}` | `{success: false, errors: [...]}` |
| `/api/replication/groups/{id}` | PUT | `{nodeIds[]}` | 同 POST | 同 POST |
| `/api/replication/groups/{id}` | DELETE | — | `{success: true, message: ...}` | `{success: false, error: ...}` |
| `/api/nodes/{id}/drain` | POST | `{drain: true\|false}` | `{success: true, message: ...}` | `{success: false, error: ...}` |
| `/api/nodes/{id}/promote` | POST | `{groupId: ...}` | `{success: true, groupId, message: ...}` | `{success: false, error: ...}` |
| `/api/replication/policy` | GET | — | `{syncWindow, rateLimitMbps, maxConcurrency, updatedAt}` | — |
| `/api/replication/policy` | PUT | `{syncWindow, rateLimitMbps, maxConcurrency}` | `{success: true, updatedAt}` | `{success: false, errors: [...]}` |
| `/api/replication/sync` | GET | — | `{summary, currentJobs[], failedJobs[], alerts[]}` | — |
| `/api/replication/sync` | POST | `{}` | `{success: true, triggeredAt}` | `{success: false, error: ...}` |
| `/api/replication/sync/retry/{taskId}` | POST | — | `{success: true, message: ...}` | `{success: false, error: ...}` |
| `/api/replication/alerts` | GET | — | `{active[], resolved[]}` | — |

### 16.8 样式规范

**新增 CSS 变量**（在现有 `:root` 扩展）：
```css
--success-color: #2e7d32;  --success-bg: #e8f5e9;
--warning-color: #e67e22;  --warning-bg: #fff3e0;
--danger-color:  #c62828;  --danger-bg:  #ffebee;
--info-color:    #0277bd;  --info-bg:    #e1f5fe;
```

**新增样式**（完整 CSS 见审查报告附录）：
- `.tab-nav` / `.tab-content` — Tab 切换
- `.progress-bar` / `.fill` — 进度条
- `.action-btn` / `.action-btn:disabled` — 表格内操作按钮
- `.status-badge.status-draining` / `.status-syncing` — 状态徽标扩展
- `.alert-level-critical` / `.alert-level-warning` — 告警级别
- `.toast.success` / `.toast.error` — 操作反馈

### 16.9 关键 JS 模式

**Tab 切换**：
```javascript
function switchTab(tabGroup, tabId) {
    document.querySelectorAll(`[data-tab-group="${tabGroup}"] .tab-content`)
        .forEach(el => el.classList.remove('active'));
    document.querySelectorAll(`[data-tab-group="${tabGroup}"] .tab`)
        .forEach(el => el.classList.remove('active'));
    document.getElementById(tabId).classList.add('active');
    document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
}
```

**轮询策略**：
- 节点监控 Tab：2s 轮询
- 冗余组管理：5s 轮询
- 对账同步：5s 轮询
- 同步策略：仅加载一次
- 告警：5s 轮询
- **优化**：仅可见 Tab 发起轮询，切换时停止旧轮询、启动新轮询

**冗余组节点勾选校验**（含同 host/重叠/数量/离线）：
```javascript
function validateGroupNodeSelection(selectedNodeIds, allNodes, existingGroups, editingGroupId) {
    const errors = [], warnings = [];
    if (selectedNodeIds.length < 2) errors.push('至少选择 2 个节点');
    if (selectedNodeIds.length > 3) errors.push('最多选择 3 个节点');
    // 重叠检查
    selectedNodeIds.forEach(nodeId => {
        existingGroups.forEach(group => {
            if (group.groupId === editingGroupId) return;
            if (group.nodes.some(n => n.nodeId === nodeId)) {
                errors.push(`节点 ${nodeId} 已属于冗余组 ${group.groupId}，不可重复分配`);
            }
        });
    });
    // 同 host 检查（仅警告）
    const hosts = {};
    allNodes.filter(n => selectedNodeIds.includes(n.nodeId))
        .forEach(n => { const h = n.address.split(':')[0]; (hosts[h] ??= []).push(n.nodeId); });
    Object.entries(hosts).forEach(([host, ids]) => {
        if (ids.length > 1) warnings.push(`节点 ${ids.join(', ')} 位于同一主机 (${host})`);
    });
    return { errors, warnings, valid: errors.length === 0 };
}
```

### 16.10 交互流程（核心 6 个）

1. **创建冗余组**：勾选节点 → 实时校验 → POST → 刷新列表
2. **排空节点**：列表点 [排空] → 确认弹窗 → POST → 状态变 [排空中]
3. **晋升节点**：副本列表点 [晋升] → 确认弹窗（提示原 primary 降级）→ POST
4. **配置同步策略**：修改表单 → 前端校验 → 确认弹窗 → PUT → Toast 提示
5. **手动触发全量对账**：对账页点 [手动触发] → 确认弹窗 → POST → 进度区开始更新
6. **重试失败任务**：失败列表点 [重试] → POST → 任务移至等待队列（**重置 4 次失败计数器**）