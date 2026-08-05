# 节点排空（Drain）设计 — 半设计文档

> **状态**：**已定稿**（用户 2026-08-05 拍板，§13 六个待定项全部采纳推荐方案）
> **版本**：v0.2
> **日期**：2026-08-05
> **范围**：节点排空/恢复的语义、不变式、约束、API、数据模型、选路改造、角色策略、并发与告警
> **关系**：本文是 `redundant-storage-design.md` §10.1 `drain` 端点的细化专项，不重复讲冗余组/对账同步本身
> **前置阅读**：`redundant-storage-design.md` §3（名词）、§5（数据模型）、§10.1（API）、§15.4（角色是文件级）

---

## 0. 关键认知（读前必读）

本文档围绕三个**必须先对齐**的事实展开，方案设计均建立在这三点之上：

**F1 — primary/secondary 是「文件级」副本角色，不是「节点级」属性。**
> 引 §15.4：UI 不能将 role 展示为节点属性。节点 node-A 对文件 X 可能是 primary，对文件 Y 可能是 secondary，对文件 Z 又可能是 primary。
> 推论：不存在"这个节点是不是主节点"的二元判断，只能说"node-A 是哪些 file_hash 的 primary"。

**F2 — drain 与 online/offline 是两个独立维度。**
> - `online/offline`：由心跳自动判定（Registry 维护 `last_heartbeat`），是节点客观存活状态。离线后节点从 `dataNodes` 映射消失，副本地址无法解析会被跳过。
> - `drain`：由管理员手动设置（`POST /api/nodes/{id}/drain`），是调度意图/管理标记，只影响"新写入选路 + 角色策略"，**不影响节点的存活判定**。
> - 三态徽标：🟢在线 / 🟠排空中 / ⚫离线。排空只在"在线"基础上叠加，**离线节点不可设置排空**（offline 节点自动从选路排除，无需 drain 标记）。

**F3 — 当前 drain 实现是半成品，标记不生效。**
> `ReplicationApiHandler.java:203-204` 自述：`drain 仅 Registry 内存（NameNode selectReplicaTargets 不排除 drained 节点）`，响应 message 注明 `"marked, not yet enforced"`。
> NameNode 选路（`NameNodeHandler.java:189`）**完全不读 drain 标记**。整套排空功能是空操作，依赖二期落地。

本文要做的就是把 F3 那个空操作补完整，同时在补的过程中把所有不变式、约束、并发坑一次说清。

---

## 1. 背景与目标

### 1.1 背景

`redundant-storage-design.md` §10.1 已定义 `POST /api/nodes/{id}/drain` 端点，意图是：
> 标记节点排空（后续上传不选它，副本角色降级）。

但目前实现停留在"内存打个标记"，且没有：
- 持久化（重启即失）
- 接入 NameNode 选路（标记对选路无效）
- 任何业务约束（任意节点随便排空、可能让整组无可用节点）
- 角色迁移策略（"副本角色降级"具体怎么降、降给谁，未定义）
- 并发保护（多个管理员同时操作可能让组空）

### 1.2 目标

1. 让 drain 真正生效：新上传的选路**必须**排除已排空节点。
2. 持久化：drain 状态跨进程（NameNode 要读）、跨重启生效。
3. 业务规则可执行：把"非冗余组不排空、组内至少留 1 个活节点"等约束落到 API 校验层，UI 不靠自觉。
4. 角色策略可决策：明确排空触发时"primary 怎么办"（延迟迁 vs 立即迁），本文档推荐**延迟迁移**并说明理由。
5. 并发安全：组级原子校验，避免"两个管理员同时排空最后两节点 → 组空"的竞态。
6. 可观测：冗余度降级、drain 状态有告警/可视化入口。

### 1.3 非目标

- **数据物理迁移**：drain 不搬数据、不删数据。物理迁移由 `replica_sync_task` 同步器负责（主文档 §7）。"drain → 真下线"之间的数据搬迁是另一条链路。
- **自动 cancel drain 时的角色回迁**：取消排空时，已迁走的 primary 不自动回迁到原节点（详见 §5.2 决策）。
- **跨组迁移**：drain 不改变节点所属冗余组（组的成员关系在 `replication_group.node_ids` 里独立维护）。
- **block 分块 / quorum 写**：保持主文档的整文件副本粒度。
- **file 模式启用 drain**：与冗余组一样，仅 `storage.mode=mysql` 时启用；file 单机模式不启用冗余，自然也不启用 drain。

---

## 2. 名词与状态机

### 2.1 新增/复用名词

| 名词 | 定义 | 备注 |
|---|---|---|
| **drain 状态** | 节点的"不再接收新写入"管理标记。值域：`ACTIVE` / `DRAINING` | 持久化到 `node_drain.drain_status` |
| **drain 时刻** | 节点进入 `DRAINING` 的时间戳，用于审计和"drain 多久了"展示 | 持久化到 `node_drain.drain_at` |
| **组冗余度** | 组内 `online && !draining` 的节点数 | 实时计算（不持久化），用于告警阈值 |
| **可服务节点（alive node）** | `online && !draining` | 选路候选集 |

### 2.2 节点状态机

```
                    ┌──────────┐
        heartbeat   │          │  heartbeat timeout
       ───────────▶ │  ONLINE  │ ─────────────────▶ OFFLINE
                    │          │                    (Registry 维护)
                    └────┬─────┘                    不在排空讨论范围
                         │ admin POST drain=true
                         │ (且 online)
                         ▼
                    ┌──────────┐
                    │DRAINING  │ (仍 online，仍可读已有数据)
                    └────┬─────┘
                         │ admin POST drain=false
                         ▼
                    ┌──────────┐
                    │  ONLINE  │
                    └──────────┘
```

- **ONLINE → DRAINING**：API 校验通过后置位（详见 §4、§9）。
- **DRAINING → ONLINE**：API 收到 `drain=false` 后清除（不做数据/角色回迁，详见 §5.2 决策 D2）。
- **DRAINING → OFFLINE**：心跳超时（与 ONLINE → OFFLINE 同路径），drain 标记保留在 DB，节点恢复后回到 DRAINING 状态。
- **OFFLINE → DRAINING**：拒绝（API 校验拦截）。离线节点无需 drain 标记——它已经自动从选路排除。

### 2.3 与 §15.4 文件级角色的关系（重点重申）

drain 标记是**节点级**的开关，落到**文件级**的 `replica_role` 上才有业务效果。两者关系：

- 节点被排空 = 该节点**不再作为新文件的 primary 或 secondary 候选**（选路层排除）。
- 该节点身上**已有的 primary 角色怎么办**？→ 见 §5 角色迁移策略。
- 该节点身上**已有的 secondary 角色怎么办**？→ 保留不动（secondary 本来就只是冗余读，drain 不影响读可用性）。

---

## 3. 核心不变式

| ID | 不变式 | 违反后果 | 强制点 |
|---|---|---|---|
| **INV-D1** | 排空后，组内 `alive node`（online && !draining）数 ≥ 1 | 组内无可用节点，新上传 100% 失败 | `handleDrain` API 入口校验 |
| **INV-D2** | drain 操作是原子的组级决策 | 并发竞态导致组空 | `SegmentedLocks` 按 `groupId` 加锁（见 §9） |
| **INV-D3** | drain 状态跨进程、跨重启一致 | NameNode 选路失效（旧 bug） | `node_drain.drain_status` 持久化 + NameNode 定期加载 |
| **INV-D4** | drain 不删除该节点上的数据，不迁移已有副本（角色层） | 数据丢失 | 仅置位 `drain_status` + 选路排除；不动 `file_location` 行 |
| **INV-D5** | drain 节点上的 primary 角色保持可读 | 误删 primary → 读可用性下降 | drain 不动 `replica_role=0` 的行（详见 §5.1 决策） |

---

## 4. 业务规则

基于 v0.1 review 中用户提出的 5 条规则，**纠正方案 4 的概念错误后**的最终版：

### R1 — 非冗余组节点不显示排空按钮

- **规则**：节点 `n` 若不属于任何 `replication_group`（`replicationGroupStore.getGroupByNodeId(n) == null`），UI 不显示排空按钮；后端 API 也要校验，防止绕过 UI。
- **理由**：非冗余组节点承载的是**单副本数据**，无其他副本可兜底。drain 的完整语义是"停止接收新写 + 数据可迁走 + 安全下线"，单副本节点**没有副本可迁**——drain 它 = 该节点上的数据成为孤儿（除非人工导出）。从防误操作角度，禁用。
- **前后端**：前端 `DashboardServer.js` 在 `drainNode` 入口判断 `groupOf(nodeId) == null` 直接 return；后端 `handleDrain` 加同条件校验（白名单只接受组内节点）。

### R2 — 排空/恢复 toggle 语义

- `POST /api/nodes/{id}/drain {"drain": true}` → 置位 `DRAINING`，响应 `200`。
- `POST /api/nodes/{id}/drain {"drain": false}` → 清除回 `ACTIVE`，响应 `200`。
- 状态变更写入 `node_registry`，**同步落库**（不是先写内存再异步刷）。
- toggle 校验：
  - `drain=true` 时，节点必须 `online` 且在某个组内（R1），且满足 INV-D1。
  - `drain=false` 时，无校验，直接清除。

### R3 + R5 — 排空后组内至少保留 1 个 alive node

> 这两条原方案重复，合并为 R3。

- **规则**：执行 `drain=true` 前，**预览**排空后组内 alive 节点数：
  - 预览值 = `当前组内 alive 数 - 1`（如果目标节点当前是 alive）
  - 若预览值 = 0 → 拒绝（409 `GROUP_WOULD_BE_EMPTY`）
- **错误响应**：

```json
{
  "error": "GROUP_WOULD_BE_EMPTY",
  "message": "排空节点 {n} 后，组 {g} 将无 alive 节点可用。请先扩组或迁移数据。",
  "groupId": "g1",
  "currentAliveCount": 1,
  "wouldBeAliveCount": 0
}
```

- **取舍说明**：R3 选**"可用性优先"**（保 ≥1 alive 即放行），不强制"保留 N 个 alive 以维持副本数"。理由：
  - MVP 简化：冗余度降级靠告警提示（§10.2），不靠拒绝。
  - 真实 decommission 流程中，确实存在"主动让组临时降级到 1 副本"以完成下线最后一台机器的场景——过严会卡住运维。
  - **代价**：3 节点组排空 2 个后，新写入的副本目标排除 drained → 退化为单副本（详见 §7.2 选路降级语义）。这种状态必须告警，但不应阻止操作。
- **未来增强**：可配置副本数下限 `policy.minAlivePerGroup`，默认 1，集群紧张时可调到 2/3（本期不实现，预留扩展点）。

### R4 — 排空触发时的角色迁移（**文件级**语义重写）

> 用户原方案：把"主节点"当节点级属性、提"副本数量最多的升级为主"。这个表述与 §15.4 文件级角色模型冲突，已纠正如下。

**最终规则 R4**：

- **R4.1 角色迁移策略选择**：排空**不立即**迁移 primary 角色（详见 §5 决策 D1：推荐**延迟迁移**）。
- **R4.2 排空瞬间不动 `replica_role`**：drain 置位时，**不修改**该节点上任何 `file_location` 行的 `replica_role`。即：drain 节点上已有的 primary 副本**仍标记为 primary**，secondary 仍为 secondary。
- **R4.3 读可用性保证**：drain 不切断读。`DOWNLOAD_LOC` 返回时，drain 节点仍可作为副本候选（如果上面有数据）——Driver 故障转移继续工作。
- **R4.4 物理迁移走 sync 任务**：当操作员要**真正下线**该节点（uninstall）时，触发"清空该节点数据"流程，由 `replica_sync_task` 同步器按文件级逐一处理：
  1. 找该节点作为副本的 file_hash 列表
  2. 对每个 file_hash，在组内**其他 alive 节点**里选一个作为新副本目标（优先选持有该文件副本的，否则当新增）
  3. 数据搬迁 + 角色迁移（per-file 复用 `handlePromote` 逻辑）
  4. 完成后该节点才能安全 uninstall

> 也就是说，**drain = 标记下线意图**；**sync 任务 = 执行下线搬运**。两个动作解耦。

### R6（隐式新增）— UI 操作反馈

- 排空按钮文案随状态切换：`[排空]` ↔ `[恢复]`。
- 排空中的节点状态徽标变橙色（`status-draining`），与"在线（绿）/离线（灰）"三态区分。
- 排空确认弹窗文案：明确"已标记的节点后续上传不再选中，已有数据可继续读，物理数据由同步任务后台搬运"。
- 提示"冗余度降级"：当组内 alive < 组大小（3 节点组活 ≤2、2 节点组活 =1）时，行内加 ⚠ 徽标。

---

## 5. 角色迁移策略（决策）

### 5.1 决策 D1：延迟迁移 vs 立即迁移

| 维度 | **B. 延迟迁移**（推荐） | A. 立即迁移 |
|---|---|---|
| 触发时机 | drain 置位时**不动** `replica_role`；等 sync 任务或卸载时再迁 | drain 置位时**立即**对该节点作为 primary 的每个 file_hash 做 promote |
| 读可用性 | drain 节点上的 primary 仍可读（仅是"该节点不该再有新数据"） | 立即切到新 primary，读链路无缝（但 round-trip 可能多一跳） |
| 取消排空的代价 | 无（drain 清除即可） | 需要把迁走的 primary 再迁回来，复杂 |
| 实现复杂度 | 低：drain 只做标记 + 选路排除 | 高：drain 要触发 per-file 的角色事务 |
| 与 HDFS 惯例 | 符合 | 不符 |
| 边缘 case | drain 节点宕机后，原 primary 副本随节点丢失 → 需 driver 走 secondary 故障转移 | 已迁走，无此问题，但代价是迁移期内的角色表震荡 |

**推荐 B（延迟迁移）**，理由：
1. 符合业界 decommission 惯例（HDFS 即如此）。
2. drain 的本质是"退役意图标记"，执行在卸载那一刻更合语义。
3. 取消 drain 不需要回迁（无副作用）。
4. 实现简单，drain 与 sync 任务职责清晰。

**代价**：drain 节点宕机时，其上 primary 副本丢失，driver 必须能从 secondary 切走（这本来就是主文档 §8.2 的故障转移机制，drain 场景不引入新要求）。

### 5.2 决策 D2：取消 drain 的角色回迁

- **决策**：取消 drain（`drain=false`）时，**不回迁**已迁移走的 primary 角色。
- **理由**：drain 期间可能已有新文件被 primary 写到其他节点（受 §7 选路降级影响），此时回迁会破坏"新文件 primary 已经在新节点"的现状，造成角色震荡。
- **影响**：取消 drain 后，原排空节点重新进入选路候选（作为新文件的副本目标），但历史上被它持有的 primary 角色保留在迁走后的目标节点上。这是**最终一致**状态，可接受。

---

## 6. 数据模型变更

### 6.1 新增/修改表

**新建 `node_drain` 专表**（持久化 drain 状态，跨进程可读）：

```sql
-- V4 迁移：V3→V4 新建专表（幂等：CREATE TABLE IF NOT EXISTS）
CREATE TABLE IF NOT EXISTS `node_drain` (
  `node_id`     VARCHAR(128) NOT NULL COMMENT '节点ID（关联运行时节点，非外键）',
  `drain_status` TINYINT NOT NULL DEFAULT 0 COMMENT '0=ACTIVE, 1=DRAINING',
  `drain_at`    DATETIME NULL DEFAULT NULL COMMENT 'DRAINING 置位时间（清除时置 NULL）',
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='节点排空状态表';
```

**字段语义**：

| 字段 | 取值 | 说明 |
|---|---|---|
| `drain_status` | `0` (ACTIVE) / `1` (DRAINING) | drain 状态。`0` = 可作为选路候选；`1` = 排除。 |
| `drain_at` | `DATETIME` / `NULL` | 置位时间，用于"已排空多久"展示 + 告警"drain 超过 N 天未处理"。 |

> **修正说明（2026-08-05，覆盖原 §13-Q3 的机制部分）**：原定稿 Q3 选择"给 `node_registry` 加列"。落地核查发现 **`node_registry` 表当前从未被写入**——Registry 全程用内存 `dataNodes` 维护节点（`RegistryHandler` 注册/心跳仅 `dataNodes.put`，无任何 DB 写），`node_registry` 运行时为空表。因此 `UPDATE node_registry SET drain_status=1 WHERE node_id=?` 会命中 0 行、drain 永不持久化。改为新建独立 `node_drain` 表：Registry 写、NameNode 启动读，不依赖 `node_registry` 是否有数据，自洽且幂等。**满足原 Q3 的意图**（持久化 + 跨进程可读，INV-D3），仅改变机制。`node_registry` 的真正落库属另一条改造链路，不在本期范围。

### 6.2 NameNode 加载 drain 状态

- NameNode 启动时一次性 `SELECT node_id FROM node_drain WHERE drain_status = 1` 加载到内存 `drainedNodes: Set<String>`。
- **不**做定时轮询（drain 状态变更低频，靠 Registry 推 / NameNode 重启加载即可，二期可加配置中心推送）。
- Registry 设置 drain 后，下一次 NameNode 启动生效（接受这个时延，因为 drain 是运维操作不是高频事件）。

> **关于时延的妥协**：MVP 范围内不实现 Registry → NameNode 的实时推送。若需要即时生效，方案是 Registry 通过 RPC 通知 NameNode 刷新 `drainedNodes` 缓存。本期不做，记入 §12 未来工作。

### 6.3 Schema 迁移要求

按 `.claude/rules/schema-migration.md` 强制规则：

1. 在 `jnfs-namenode/src/main/java/org/jnfs/namenode/migration/` 下新增 `MysqlV3ToV4`（CREATE TABLE IF NOT EXISTS node_drain + 版本表写入 4）。
2. 同目录新增 `FileV3ToV4`（no-op，file 模式不启用 drain）。
3. `MigrationRunner.CURRENT_VERSION` 升到 4。
4. 注册 `mysql_v3_to_v4.properties` 和 `file_v3_to_v4.properties`。
5. 更新 `mysql/jnfs.sql` 反映最新 schema。
6. 幂等性：`CREATE TABLE IF NOT EXISTS`（MySQL 8.0+），重入安全。

---

## 7. 选路改造

### 7.1 现状

`NameNodeHandler.selectReplicaTargets(String fileHash)`（`NameNodeHandler.java:189`）当前逻辑：

```
candidates = dataNodes 中所有 nodeInfo
过滤 existingNodeIds（已持有该文件的节点）
primary = loadBalancer.selectNodeId(candidates) // 加权随机
若 primary 在某 group 内 → 返回 [primary] + group 其他成员
否则 → 单副本返回 [primary]
```

**完全不读 drain 标记**（F3 已述）。

### 7.2 改造

**改造点**：在 `candidates` 构造时增加 drain 过滤：

```java
// 新增：过滤 drained 节点
List<String> candidates = new ArrayList<>();
for (String nodeInfo : dataNodes) {
    String[] parts = nodeInfo.split("\\|");
    String nodeId = parts[0];
    // 保留 4 个过滤条件：
    // 1) 不在已持有该文件的节点中
    // 2) 当前 online（dataNodes 已是 online 集合，Registry 心跳维护）
    // 3) drain_status = 0（新增，NameNode 启动时加载的 drainedNodes 集合）
    // 4) [可选] 节点可达 — 当前不做 ping，靠心跳间接保证
    if (!existingNodeIds.contains(nodeId) && !drainedNodes.contains(nodeId)) {
        candidates.add(nodeInfo);
    }
}
```

**组降级语义**（重要）：

| 组大小 | alive 数 | 选路结果 | 业务影响 |
|---|---|---|---|
| 3 | 3 | `[primary, sec1, sec2]` | 正常 3 副本 |
| 3 | 2 | `[primary, sec1]` 或 `[primary, sec2]` | **降级到 2 副本**（告警） |
| 3 | 1 | `[primary]` | **降级到单副本**（严重告警） |
| 2 | 2 | `[primary, sec]` | 正常 2 副本 |
| 2 | 1 | `[primary]` | 降级到单副本（告警） |
| 1 | 1 | `[primary]` | 不在组内的节点不在本文讨论范围 |

> **降级时 driver 端不需要改**：driver 一直支持变长副本列表（`locInfo.split("\\|")` 解析 §8.2 现有机制）。降级到 1 副本等价于历史单副本模式。

### 7.3 NameNode 缓存失效

- `drainedNodes: Set<String>` 缓存在 NameNode 启动时构建。
- Registry 端更新 drain 状态后，**NameNode 缓存仍是旧值**，直到下次启动（这是 §6.2 的妥协）。
- 临时缓解方案：NameNode 暴露 `POST /admin/reload-drain` 端点供运维触发重载。**本期不实现**，记入 §12。

---

## 8. API 端点

### 8.1 修订 `/api/nodes/{id}/drain`

| 维度 | 现实现 | v0.1 设计 |
|---|---|---|
| 持久化 | ❌ 内存 | ✅ `node_drain.drain_status` |
| 非冗余组校验 | ❌ 无 | ✅ R1（防绕过） |
| INV-D1 校验 | ❌ 无 | ✅ R3 预览 + 拒绝 |
| 并发保护 | ❌ 无 | ✅ 组级 `SegmentedLocks` |
| 错误码 | 仅 500 | 200/400/404/409 |
| 审计 | 已有（`audit` 调用） | 保留 |
| 响应 message | `"marked, not yet enforced"` | `"drain status updated: ACTIVE→DRAINING"`（实际生效） |

### 8.2 请求/响应

**Request**：

```http
POST /api/nodes/{id}/drain HTTP/1.1
Content-Type: application/json
Authorization: <admin token>

{"drain": true}    // 或 {"drain": false}
```

**Response 200（成功）**：

```json
{
  "success": true,
  "message": "drain status updated: ACTIVE → DRAINING",
  "nodeId": "node-1",
  "drainStatus": "DRAINING",
  "drainAt": "2026-08-05T10:30:00Z"
}
```

**Response 400（非冗余组节点）**：

```json
{
  "error": "NODE_NOT_IN_GROUP",
  "message": "节点 node-1 不属于任何冗余组，无法排空"
}
```

**Response 404（节点不存在）**：

```json
{
  "error": "NODE_NOT_FOUND",
  "message": "node-1 not found in node_registry"
}
```

**Response 409（违反 INV-D1）**：

```json
{
  "error": "GROUP_WOULD_BE_EMPTY",
  "message": "排空节点 node-1 后，组 g1 将无 alive 节点",
  "groupId": "g1",
  "currentAliveCount": 1,
  "wouldBeAliveCount": 0,
  "hint": "请先扩组或迁移数据"
}
```

**Response 409（节点已离线）**：

```json
{
  "error": "NODE_OFFLINE",
  "message": "节点 node-1 已离线，离线节点自动从选路排除，无需 drain"
}
```

**Response 405（方法不允许）**：

```json
{"error": "METHOD_NOT_ALLOWED"}
```

### 8.3 审计

- 沿用现有 `audit(username, "DRAIN", nodeId, result)` 格式。
- `result` 扩展为 `SET` / `CLEAR` / `REJECTED:<reason>`，便于审计回溯拒绝原因。

### 8.4 其他相关端点（不修改，记录在册）

- `GET /api/nodes` 列表接口：需**新增** `drainStatus` / `drainAt` 字段透出（前端徽标需要）。
- `GET /api/replication/groups`：可保留不变（组状态由组内成员聚合，前端算）。
- `POST /api/nodes/{id}/promote`：不变，受 §5.1 策略 B 影响——drain 期间不主动 promote，由 sync 任务驱动。

---

## 9. 并发与错误处理

### 9.1 并发保护（INV-D2）

**风险**：管理员 A、B 同时点排空组内最后 2 个 alive 节点，各自校验都通过，提交后组空。

**方案**：以 `groupId` 为 key 的 `SegmentedLocks`（`jnfs-common` 已有工具，`.claude/rules/common-utilities.md` 提及）：

```java
// 在 handleDrain 入口
ReplicationGroup g = replicationGroupStore.getGroupByNodeId(nodeId);
if (g == null) return error(400, "NODE_NOT_IN_GROUP");

synchronized (LOCKS.getLock(g.getGroupId())) {
    // 1) 重新读 group 当前 alive 节点集（重读防 TOCTOU）
    // 2) 计算排空后 alive 数
    // 3) 若 < 1 → 409 GROUP_WOULD_BE_EMPTY
    // 4) 写 node_drain.drain_status
    // 5) 更新内存 drainedNodes
    // 6) 写审计
}
```

**Registry 多实例部署**（未来）：`SegmentedLocks` 是进程内锁，多 Registry 实例时需换 DB 行锁或分布式锁（`SELECT ... FOR UPDATE`）。本期 Registry 单实例，进程内锁足够。

### 9.2 错误码统一

| 错误码 | HTTP | 触发条件 |
|---|---|---|
| `NODE_NOT_FOUND` | 404 | `node_registry` 无该 node_id |
| `NODE_NOT_IN_GROUP` | 400 | R1 拦截（非冗余组节点） |
| `NODE_OFFLINE` | 409 | R2 拦截（offline 节点） |
| `GROUP_WOULD_BE_EMPTY` | 409 | INV-D1 违反 |
| `DB_ERROR` | 500 | SQL 异常（保留现有 catch） |
| `INVALID_JSON` | 400 | body 解析失败 |
| `METHOD_NOT_ALLOWED` | 405 | 非 POST 方法 |

### 9.3 部分失败回滚

- 写 `node_registry` 失败 → 不修改内存缓存，返回 500，无副作用。
- 锁内出现异常 → 抛出后由现有 `catch (SQLException)` 兜底，**不清理锁**（`synchronized` 块自动释放）。

---

## 10. 监控与告警

### 10.1 Registry 端

- `GET /api/nodes` 列表返回 `drainStatus` / `drainAt` 字段。
- `GET /api/replication/sync` 同步进度接口扩展：返回 `drainingNodes: [...]` 列表（便于告警脚本）。

### 10.2 告警项（建议接入现有告警框架，详见主文档 §10）

| 告警 | 触发条件 | 严重度 |
|---|---|---|
| `GROUP_REDUNDANCY_DEGRADED` | 组内 alive 节点数 < 组大小 | WARNING（alive=2/3 时）/ CRITICAL（alive=1/3 或 1/2 时） |
| `NODE_DRAIN_TOO_LONG` | 节点 `DRAINING` 持续 > 7 天 | WARNING（提醒操作员完成下线流程） |
| `DRAIN_GROUP_EMPTY` | 某组全部 alive=0（理论上 INV-D1 不会发生，但若发生必是 DB 损坏或极端竞态） | CRITICAL |

### 10.3 审计

- 所有 `DRAIN` 操作记录 `audit` 日志，含拒绝原因。
- 建议接入 ELK / 文件分析，长期追踪"哪些节点被反复 drain/cancel"。

---

## 11. 验收用例

> 实现完成后按此清单逐项验证，**所有用例必须通过**才视为 v0.1 完成。

### 11.1 正常路径

| # | 场景 | 预期 |
|---|---|---|
| C1 | 在线、非冗余组节点 POST `drain=true` | 400 `NODE_NOT_IN_GROUP`，`drain_status` 不变 |
| C2 | 在线、3 节点组内节点 A POST `drain=true` | 200，`drain_status=1`，`drain_at` 写入；NameNode 启动后 `drainedNodes` 含 A |
| C3 | DRAINING 节点 A POST `drain=false` | 200，`drain_status=0`，`drain_at=NULL`；不触发角色回迁 |
| C4 | 离线节点 POST `drain=true` | 409 `NODE_OFFLINE`，`drain_status` 不变 |
| C5 | DRAINING 节点心跳超时 | `last_heartbeat` 更新停滞，状态变 OFFLINE，`drain_status` 保留为 1 |
| C6 | OFFLINE 节点心跳恢复 | 回到 ONLINE，`drain_status` 仍为 1（保持 DRAINING） |

### 11.2 选路

| # | 场景 | 预期 |
|---|---|---|
| C7 | 3 节点组 1 节点 DRAINING，新文件上传 | `locInfo` 返回 2 节点（primary + alive 副本），driver 写 2 副本 |
| C8 | 3 节点组 2 节点 DRAINING，新文件上传 | `locInfo` 返回 1 节点（仅 primary），driver 写 1 副本，告警 `GROUP_REDUNDANCY_DEGRADED` 触发 |
| C9 | DRAINING 节点上有 file_hash 的 primary 副本，下载该文件 | `locInfo` 仍返回该 DRAINING 节点（读可用），driver 优先尝试；失败时切到 secondary |

### 11.3 并发

| # | 场景 | 预期 |
|---|---|---|
| C10 | 3 节点组 2 alive，并发请求排空 A 和 B | 必有 1 个 409 `GROUP_WOULD_BE_EMPTY`，最终 alive 数 = 1 |
| C11 | 2 节点组 2 alive，并发排空 A 和 B | 同 C10，最终 alive = 1 |
| C12 | 排空 + 上传并发 | 上传选路必须看不到 drain 标记（`drainedNodes` 一致性），不能选出 DRAINING 节点 |

### 11.4 持久化

| # | 场景 | 预期 |
|---|---|---|
| C13 | 排空节点 A → 重启 Registry | `node_drain` 仍 `drain_status=1`，`GET /api/nodes` 返回 `DRAINING` |
| C14 | 排空节点 A → 重启 NameNode | 启动后 `drainedNodes` 含 A，新上传不选 A |

### 11.5 迁移

| # | 场景 | 预期 |
|---|---|---|
| C15 | V3 schema 启动 V4 代码 | 自动执行 `MysqlV3ToV4`，新建 `node_drain`、写 `schema_version=4`，幂等可重跑 |
| C16 | V4 schema 启动 V3 代码 | 拒绝启动（V3 不识别 `schema_version=4`，按主文档 INV-4） |

---

## 12. 范围外 / 未来工作

| # | 内容 | 优先级 |
|---|---|---|
| F1 | Registry → NameNode 实时推送 drain 状态变更（避免重启延迟） | 中 |
| F2 | NameNode 暴露 `POST /admin/reload-drain` 端点 | 中 |
| F3 | file 模式 drain（本期不做，因 file 模式无冗余组） | 低 |
| F4 | drain 节点上的"立即迁移"可选策略 A（产品决策需要时） | 低 |
| F5 | `policy.minAlivePerGroup` 可配置副本数下限（替代 R3 硬编码 1） | 低 |
| F6 | Registry 多实例时换 DB 行锁 / 分布式锁 | 中 |
| F7 | drain 节点数据物理迁移的"一键触发卸载"入口（集成 sync 任务） | 中 |

---

## 13. 已定稿决策（用户 2026-08-05 拍板）

§13 原为 6 个待定项，用户全部采纳推荐方案，现固化为决策。后续实现以本节为准：

| # | 决策点 | 定稿方案 | 依据 |
|---|---|---|---|
| **Q1** | 角色迁移策略 | **B. 延迟迁移** | §5.1 — drain 只标记，不立即迁 primary；真下线时由 sync 任务 per-file 搬运。符合 HDFS decommission 惯例，取消 drain 无副作用 |
| **Q2** | 取消 drain 时是否回迁角色 | **不回迁** | §5.2 — 避免角色震荡；状态最终一致可接受 |
| **Q3** | drain 状态字段位置 | **新建 `node_drain` 专表（`drain_status` / `drain_at`）** | §6.1 修正（2026-08-05）— `node_registry` 运行时从未被写入（Registry 全内存），加列无法持久化；改独立专表，Registry 写、NameNode 启动读，满足 INV-D3 跨进程意图 |
| **Q4** | 告警 `NODE_DRAIN_TOO_LONG` 阈值 | **7 天** | §10.2 — 提醒操作员完成下线流程，留足运维窗口 |
| **Q5** | 非冗余组节点排空 UX | **按钮直接隐藏** | §4 R1 — 防误操作，后端 API 同步校验防绕过 |
| **Q6** | NameNode 缓存加载策略 | **启动时一次性加载** | §6.2 — drain 低频事件，接受重启生效时延；实时推送留作 §12 F1 未来工作 |

> **实现约束**：Q1/Q2 决定了 `handleDrain` **不触发任何 `replica_role` 变更**，仅置位 `drain_status` + 选路排除。这是与用户最初方案最大的差异，落地时务必不要引入"排空即迁主"的分支。

---

## 附录 A：变更清单

| 文件 / 模块 | 变更类型 | 说明 |
|---|---|---|
| `node_drain` 表 | CREATE | 新建专表（node_id PK, drain_status, drain_at, update_time） |
| `MigrationRunner.CURRENT_VERSION` | 改 3 → 4 | 触发 V3→V4 迁移 |
| `jnfs-namenode/migration/MysqlV3ToV4.java` | 新增 | DDL + 写 `schema_version` |
| `jnfs-namenode/migration/FileV3ToV4.java` | 新增 | no-op（file 模式不启用 drain） |
| `META-INF/migrations/mysql_v3_to_v4.properties` | 新增 | 注册步骤 |
| `META-INF/migrations/file_v3_to_v4.properties` | 新增 | 注册步骤 |
| `mysql/jnfs.sql` | 更新 | 反映新 schema |
| `ReplicationApiHandler.handleDrain` | 改写 | 加 R1/R2/R3 校验 + 持久化 + 组级锁 |
| `ReplicationApiHandler.audit` | 扩展 | `result` 加 `REJECTED:<reason>` |
| `NameNodeHandler.selectReplicaTargets` | 改造 | 加 drained 过滤 |
| `NameNodeHandler` 新字段 | 新增 | `drainedNodes: Set<String>` 启动时构建 |
| `DashboardServer.js` | 改 | 列表接口透出 `drainStatus`；非冗余组隐藏按钮 |
| `ReplicationGroupDao` | 可选 | 新增 `listAliveMembers(groupId)` 辅助方法 |
| `jnfs-registry/.../api/dao/NodeRegistryDao` | 新增 | 读写 `drain_status` 的 DAO |

---

## 附录 B：参考引用

- 主文档：`redundant-storage-design.md` §3（名词）、§5（数据模型）、§7（同步器）、§8.2（Driver 故障转移）、§10.1（API）、§10.2（审计）、§15.4（角色文件级）
- 工具：`SegmentedLocks`（`.claude/rules/common-utilities.md`）
- 规则：`schema-migration.md`（迁移强制）、`storage-compatibility.md`（双模式，本期 file 模式不启用）
- 现有代码：
  - `ReplicationApiHandler.java:188-208` 当前 drain 实现（半成品）
  - `NameNodeHandler.java:189-247` 当前选路实现
  - `ReplicationApiHandler.java:222-225` promote SQL 范式
