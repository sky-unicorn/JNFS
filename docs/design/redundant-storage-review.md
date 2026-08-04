# JNFS 冗余存储设计 — 团队审查报告

> 状态：已定稿（3 个开放决策已于 2026-08-03 全部确认，设计文档已落实）
> 日期：2026-08-03
> 审查角色：整体架构师（汇总）、后端工程师、网络技术专家、UI+前端工程师、问题挑刺专家（交叉审查）
> 审查对象：`docs/design/redundant-storage-design.md`

---

## 1. 结论摘要

设计文档**决策点清晰、用户决策齐全、迁移遵守四不变式、夜间对账职责定位准确**，完善度 **6.5/10**。

但存在 **3 个致命架构盲点**（不修复直接实施会卡壳）、**6 个重要矛盾**、**若干协议兼容性遗漏**。全部发现均已用代码核实。

---

## 2. 致命问题（实施前必须解决）

### [C1] Dashboard/管理 API 的进程归属（Registry vs NameNode）

**证据**：`DashboardServer.java` 属于 `jnfs-registry` 模块，由 `RegistryServer` 启动；Registry 只连 `dashboard_user` 表（`MysqlUserStore`），**不连元数据库**。NameNode 进程完全没有 HTTP/Dashboard。

**影响**：设计文档 §5.1/§10 反复说"Dashboard 读写 `replication_group` 表、持久化到 mysql"，但 Registry 进程无元数据 DataSource。UI 工程师设计的 12 个 API 中 8 个写 `replication_group`/`policy` 表，**无处落地**。

**候选方案**：
- (a) 冗余管理 API 放到 NameNode 进程（新增 `NameNodeDashboardServer`），Registry 的 Dashboard UI 通过 HTTP 调用。职责清晰，但 Registry→NameNode 跨进程调用。
- (b) 让 Registry 连接元数据库（新增 DataSource）。改动小，但打破 Registry/NameNode 解耦，Registry 与 NameNode 共享同一库。

**✅ 已确认方案（决策 9）**：采用 (b) 复用 Registry 连元数据库。Registry 新增 DataSource 读写 `replication_group`/`replication_policy`/`replica_sync_task` 表；NameNode 仍定期从 mysql 加载冗余组定义。**不引入 NameNode HTTP 服务**。详见设计文档 §10 段首说明。

### [C2] 元数据缓存必须从单值重构为多副本结构

**证据**：`MetadataCacheManager.java:145-157` `MetadataEntry` 是 `final String address` 单值；`:113-128` `put()` 用 `metaCache.put(hash, entry)` —— **key=hash，value=单个 entry**。

**影响**：设计文档 §5.1 要求"返回 `List<MetadataEntry>` 排序"，但当前 cache 结构一个 hash 只能缓存一条。这是**数据结构重构**而非补一致性方案：
- `Cache<String, MetadataEntry>` → `Cache<String, List<MetadataEntry>>`
- 连带 `handleDownloadLocRequest`（NameNodeHandler:305-327）单值访问、秒传返回单地址、`handleCheckExistence` 全部要改
- cache eviction 策略需重审（多副本行 = 一个 entry 列表）

**处理**：架构师已定案为**必须重构**（无替代方案），写入设计文档 §5.4。

### [C3] DDL 双源头不一致：构造函数建表缺 `status` 列

**证据**：`MySQLMetadataManager.java:62-71` 构造函数 `CREATE TABLE IF NOT EXISTS file_location` **没有 `status` 列**；`jnfs.sql:54` 已有 `status tinyint`。

**影响**：全新部署走构造函数建表（不经迁移），会建出无 `status` 的表；之后设计文档 §5.1 的 `ALTER TABLE ADD INDEX idx_hash_status (file_hash, status)` **建索引直接失败**。

**处理**：架构师已定案——**建表 DDL 收敛到迁移框架**（`MysqlV0ToV1` 已建表，V1ToV2 继承），构造函数只保留最小骨架，所有列定义统一走迁移。写入设计文档 §5.3。

---

## 3. 重要问题（实现前必须澄清）

| # | 问题 | 来源 | 处理 |
|---|---|---|---|
| [I1] | `handleUploadLocRequest` 当前不接收 fileHash（`JNFSDriver:520` 传空 `new byte[0]`），无法做组内选择 | 后端 S4-B | 协议改：Driver 在 REQUEST_UPLOAD_LOC 携带 hash |
| [I2] | `WeightedRandomStrategy.select` 返回 `host:port` 丢弃 `node_id`（:37,67），与组定义存 `node_id` 冲突 | 后端 S5-B | select 返回结构化对象或上层反查 `NodeAddressResolver.getNodeId` |
| [I3] | 下载跨节点退避"200/400/800ms"不合理——跨节点故障转移应**立即试下一个**，不 sleep | 网络 S5-N + 后端 G8-B | 删除退避，改为立即尝试 |
| [I4] | 老 Driver 校验 `parts.length != 3`（`JNFSDriver:439`），NameNode 返回 5 段会炸——设计文档 §8.1"前两段兼容"不成立 | 挑刺 M1 | 需明确协议灰度方案（见 §4 协议兼容） |
| [I5] | DataNode 当前只连 Registry，**无 NameNode 连接能力**，`DATA_REPLICA_COMMIT` 无处发送 | 网络 S6-N + 后端 S6-B | DataNode 新增 NameNode 客户端模块（复用 `ChannelPoolUtils`） |
| [I6] | 对账队列 `replicaSyncQueue` 内存态，NameNode 崩溃即丢 | 后端 G5-B/Q2-B | ✅ 决策 10：落 `replica_sync_task` 表 + NameNode 启动恢复（§5.1 新增表、§7.7 持久化、§7.8 retry_count 计数源） |

---

## 4. 协议兼容性（三处格式变更的灰度方案）

| 命令 | 现格式 | 新格式 | 风险 |
|---|---|---|---|
| `RESPONSE_UPLOAD_LOC` | `host:port` | `primary\|sec1\|sec2` | 老 Driver `split(":")` 解析端口会炸 |
| `COMMIT_FILE` | `filename\|hash\|addr`（3 段） | `filename\|hash\|addr1,addr2,...` | 老 NameNode 校验 `parts.length != 3` 报错 |
| `RESPONSE_DOWNLOAD_LOC` | `filename\|hash\|host:port`（3 段） | `filename\|hash\|primary\|replica1\|replica2` | 老 Driver 校验 `parts.length != 3` 报错 |

**处理**：三处协议变更**不可灰度**（Driver 与 NameNode 必须同步升级）。设计文档应明确声明"本次为破坏性协议变更，Driver 与 NameNode 需同版本升级"，不再假设"兼容老 Driver 取首段"。写入设计文档 §8.1。

---

## 5. 开放决策（已确认 2026-08-03）

三份产出共 9 个待确认项，经合并审视后**真正需要用户拍板的只有 3 个**，已全部确认采用推荐方案：

| # | 决策点 | 确认方案 | 说明 |
|---|---|---|---|
| [U1] | **管理 API 进程归属**（C1） | ✅ 复用 Registry 连元数据库 | Registry 新增 DataSource 读写 `replication_group`/`replication_policy`/`replica_sync_task` 表，不引入 NameNode HTTP 服务 |
| [U2] | **对账任务持久化**（I6） | ✅ 落 `replica_sync_task` 表 + NameNode 启动恢复 | 崩溃不丢进度；`IN_FLIGHT` 回退 `PENDING` 重派 |
| [U3] | **手动重试与告警计数语义**（A2） | ✅ 手动重试重置 4 次计数器 | 视为运维介入后重新开始窗口 |

其余 6 项已由架构师/工程师自主决定：
- 对账拉取**复用 5369 端口**（U1-N，不建新端口）
- DataNode 间**不建连接池**（U2-N，复用 `NettyClientBootstrap` 短连接）
- 限速 chunk **1MB 粒度**（U3-N）
- 软截止后**不中断在途任务**（U4-N，设计文档已答）
- 存量行 `replica_role=0` 视为 PRIMARY（U5-B，设计文档已答）
- `replication_factor` **写入时快照**（U7-B，符合 §10.3"组改小不删副本"语义）
- 并发写共享同一 `.enc` 密文文件（U8-B，已核实代码：`JNFSDriver` 上传后 finally 删除，多线程共享无冲突）
- DataNode 发 `DATA_REPLICA_COMMIT` 用 `SecurityConfig.getToken()`（U9-B，与 Driver 同源）

---

## 6. UI 设计审查结论

UI 工程师产出**整体质量高**（两级 Tab、布局/交互/API/样式齐全），但有两处需修正：

- **[A5] 节点级 role 语义错误**：UI 将 `role: primary/replica` 作为**节点属性**展示，但 PRIMARY/SECONDARY 是**文件级副本角色**（设计文档 §3 明确），同一节点对不同文件角色不同。组管理页应只显示组成员 `node_id` 列表，不显示 role。
- **[A2] 手动重试与告警冲突**：设计文档 §7.8 定义"连续 4 次失败告警"。手动重试按钮会让计数语义混乱。**定案：手动重试重置失败计数器**（视为运维介入后重新开始 4 次窗口）。

**UI 新增 API 合理性判定**：
- `PUT/DELETE /api/replication/groups/{id}` — 合理，设计文档 §10 漏了组的修改/删除
- `POST /api/replication/sync`（手动触发全量）— 合理，需与 cron 去重锁
- `POST /api/replication/sync/retry/{taskId}` — 合理，需重置计数器语义
- `GET /api/replication/alerts` — 合理，告警需落表（与 I6 同源）

---

## 7. 工程师遗漏的问题（已补入设计）

| # | 问题 | 处理 |
|---|---|---|
| [M1] | 三处协议变更破坏灰度（见 §4） | 设计文档声明"破坏性变更，需同步升级" |
| [M2] | 并发写线程池生命周期未定义 | Driver 实例级 `replicaWriteExecutor`（`DaemonThreadFactory`，`close()` 时 shutdown） |
| [M3] | mysql 模式无冗余组的降级路径 | `replication_group` 表为空时返回单节点 `[primary]`（设计文档 §6.1 已对，需明确） |
| [M4] | `DATA_REPLICA_PULL` 无完整性校验 | target 拉完校验文件大小，或复用上传 HMAC 模式 |
| [M6] | 组改小后对账可能反向补齐到旧 replication_factor | `replication_factor` 写入时快照，对账按行内快照判定，不查当前组大小 |

---

## 8. 关联文件

| 文件 | 证据 |
|---|---|
| `jnfs-registry/src/main/java/org/jnfs/registry/DashboardServer.java` | C1：Dashboard 在 Registry |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/MetadataCacheManager.java` | C2：单值 cache |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/MySQLMetadataManager.java` | C3：构造函数 DDL 缺 status |
| `mysql/jnfs.sql` | C3 对照：status 列已存在 |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/NameNodeHandler.java` | I1/I2/I4 |
| `jnfs-driver/src/main/java/org/jnfs/driver/JNFSDriver.java` | I3/M2：无故障转移、无 executor |
| `jnfs-datanode/src/main/java/org/jnfs/datanode/DataNodeServer.java` | I5：无 NameNode 连接 |
| `jnfs-namenode/src/main/java/org/jnfs/namenode/WeightedRandomStrategy.java` | I2：丢弃 node_id |