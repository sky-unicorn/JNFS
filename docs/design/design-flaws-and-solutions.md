# JNFS 项目设计漏洞审计报告

> **审计日期**: 2026-06-09
> **审计团队**: jnfs-audit (5人团队)
> **审计范围**: JNFS (Java Network File System) 全项目代码审计
> **项目版本**: 0.0.1-SNAPSHOT
> **审计方法**: 静态代码分析 + 架构推理 + 团队多角色交叉验证

---

## 目录

- [1. 执行摘要](#1-执行摘要)
- [2. 架构层审计](#2-架构层审计)
- [3. 后端逻辑审计](#3-后端逻辑审计)
- [4. 网络层审计](#4-网络层审计)
- [5. 客户端/Driver审计](#5-客户端driver审计)
- [6. UI层审计](#6-ui层审计)
- [7. 质量与可维护性审计](#7-质量与可维护性审计)
- [8. 安全性审计](#8-安全性审计)
- [9. 全部漏洞汇总](#9-全部漏洞汇总)
- [10. 解决方案路线图](#10-解决方案路线图)

---

## 1. 执行摘要

本次审计由 5 人团队（架构师、后端/网络专家、前端/UI工程师、QA挑刺专家）对 JNFS 项目进行了全维度审查。共发现 **35 项设计漏洞/风险**：

| 级别 | 数量 | 说明 |
|------|------|------|
| **严重 (P0)** | 9 项 | 可能导致数据丢失、安全漏洞或系统不可用，须 1 周内修复 |
| **高 (P1)** | 8 项 | 显著影响系统稳定性、性能或安全性，须 2 周内修复 |
| **中 (P2)** | 12 项 | 影响代码质量和可维护性，纳入迭代计划 |
| **低 (P3)** | 6 项 | 建议改进，但影响有限 |

---

## 2. 架构层审计

> **审计人**: 架构师

### 2.1 模块划分与依赖关系（正面评价）

```
jnfs-parent (POM)
├── jnfs-common      ── 公共组件、协议定义、工具类
├── jnfs-registry    ── 注册中心 (服务发现、心跳管理)
├── jnfs-namenode   ── 元数据管理 (文件Hash、位置、负载均衡)
├── jnfs-datanode   ── 数据存储 (文件读写、加密存储)
├── jnfs-driver      ── 客户端 SDK
├── jnfs-example     ── 示例与测试
└── jnfs-distribution ── 打包发布
```

- 模块职责单一，扇形依赖（全部依赖 `jnfs-common`），无循环依赖
- `jnfs-common` 公共工具提取合理：`DaemonThreadFactory`、`SegmentedLocks`、`NettyHandlerHelper`、`CommonChannelPoolHandler`、`ChannelPoolUtils`、`NettyClientBootstrap`、`ServerShutdownHelper`

### 2.2 设计漏洞

#### [P0-严重] ARCH-001: DataNode 存储无副本机制

**问题**: 每个文件只存储在单个 DataNode 上，无副本。一旦 DataNode 故障，文件不可访问。

**影响**: 单点数据丢失风险。

**方案**: 实现多副本存储（默认 3 副本），NameNode 维护副本映射关系。

---

#### [P1-高] ARCH-002: 模块间协议版本管理缺失

**问题**: 各模块共享 `Packet`/`CommandType`（`jnfs-common`），但 `Packet.version` 字段未被编解码器校验，无法实现协议兼容性管理。

**影响**: 协议升级时无法兼容旧版本客户端，必须全量升级。

**方案**: `PacketDecoder` 中增加版本校验，拒绝不兼容版本；`PacketEncoder` 支持多版本编码。

---

#### [P2-中] ARCH-003: NameNodeHandler 静态状态过多，无法测试

**问题**: `NameNodeHandler` 中 `metadataManager`、`cacheManager`、`dataNodes`、`pendingUploads`、`LOCKS` 均为 `static` 字段。

**影响**: 无法在单元测试中隔离创建独立的 Handler 实例，强依赖全局状态。

**方案**: 将组件依赖改为构造函数注入，`static` 仅保留常量。

---

#### ~~[P2-中] ARCH-004: 各模块心跳逻辑重复 70%~~ **[已修复 2026-06-09]**

**问题**: `DataNodeServer.sendHeartbeatToRegistry()` 和 `NameNodeServer.sendHeartbeatToRegistry()` 几乎相同：`pool.acquire()` → `future.addListener()` → 写入 → 释放。

**修复方案**: 提取为公共的 `HeartbeatSender` 工具类（`org.jnfs.common.HeartbeatSender`），提供 `broadcast()`/`broadcastString()` 静态方法，支持自定义 `CommandType`、`Function<InetSocketAddress, byte[]>` 载荷生成器和可选的 `ErrorHandler`。NameNodeServer 和 DataNodeServer 均已重构为调用 `HeartbeatSender.broadcastString()`，消除了约 70% 的重复代码。编译验证通过（BUILD SUCCESS）。

---

## 3. 后端逻辑审计

> **审计人**: 后端工程师 + 架构师

### 3.1 数据存储与一致性

#### [P0-严重] BACK-001: MySQL 分布式锁的 DELETE+INSERT 非原子操作

**文件**: `MySQLMetadataManager.java:133-159`

```java
// 步骤1: 清理过期锁
conn.prepareStatement(deleteSql).executeUpdate();
// 步骤2: 尝试获取锁
conn.prepareStatement(insertSql).executeUpdate();
return true;
```

**问题**: 两个操作非原子。并发时两个 NameNode 可同时通过步骤1和步骤2，都认为获取了锁。

**影响**: 同一文件被并发上传多次，产生冗余数据。

**方案**: 使用 `INSERT ... ON DUPLICATE KEY UPDATE` 原子操作。

---

#### [P0-严重] BACK-002: File 模式 `queryByHash()` 恒返回 null

**文件**: `MetadataManager.java:31`

```java
public MetadataCacheManager.MetadataEntry queryByHash(String hash) {
    return null; // 始终返回 null！
}
```

**问题**: File 模式下缓存未命中时无法回源查找，仅依赖全量内存。缓存淘汰后（或禁用缓存时）查找丢失。MySQL 模式可以回源 DB，双模式行为严重不一致。

**影响**: 缓存禁用后所有文件查询都返回"不存在"，用户被迫重复上传。

**方案**: File 模式按行扫描元数据日志文件，或使用 HashMap 内存索引。

---

#### [P1-高] BACK-003: 元数据提交与锁释放存在竞态窗口

**文件**: `NameNodeHandler.java:219-278` + `MySQLMetadataManager.java:212-251`

**问题**: `logAddFile()` 中 INSERT 元数据 + DELETE 锁在同一事务中，但上层 `handleCommitFile()` 先调用 `cacheManager.put()` 再进入同步块。多 NameNode 场景：节点A释放锁→节点B获取锁→节点A元数据尚未持久化→节点B认为无人上传，再次上传。

**影响**: 多 NameNode 部署下，相同文件可能被重复上传到不同 DataNode。

**方案**: 将持久化操作全部放入同步块内，依赖数据库 UNIQUE 约束兜底。

---

#### [P0-严重] BACK-004: MySQL `file_metadata` 表缺少 hash 唯一约束

**文件**: `MySQLMetadataManager.java:39-48` (建表SQL)

```sql
`file_hash` CHAR(64) NOT NULL,
KEY `idx_hash` (`file_hash`)  -- 仅普通索引，非 UNIQUE
```

**问题**: `file_hash` 无 UNIQUE 约束，同 hash 可被插入多次。分布式锁若失效，重复记录将永久存入。

**影响**: 同一文件在数据库中有多条元数据记录，数据冗余且查询结果不确定。

**方案**: `file_hash` 添加 UNIQUE KEY，同时 INSERT 使用 `INSERT IGNORE`。

---

#### [P1-高] BACK-005: TimedCache 与 DB 锁过期时间不匹配

**问题**: `pendingUploads`: 10 分钟；`file_upload_lock`: 30 分钟。若客户端 ALLOW 后断开不 commit，10 分钟后本地缓存过期但 DB 锁仍存活 20 分钟。

**影响**: 在 10-30 分钟内，新上传请求被 DB 锁阻止（WAIT 响应），但本地缓存已过期，双重判断结果不一致。

**方案**: 统一两个过期时间为相同值（建议都是 10 分钟），DB 锁过期后自动清理。

---

### 3.2 资源管理

#### [P2-中] BACK-006: `DataNodeHandler` 异常场景下临时文件残留

**文件**: `DataNodeHandler.java:312-329`

**问题**: `finishUpload()` 异常时调用 `resetState()` 将 `currentTmpFile` 置 null，后续 `channelInactive()` 中无法清理 `.tmp` 文件。

**影响**: 磁盘空间泄漏。

**方案**: 在 `channelInactive()` 中基于文件系统扫描 `.tmp` 文件，不依赖内存变量。

---

#### [P2-中] BACK-007: DataNode GC 线程间隔过大

**文件**: `DataNodeServer.java:152`

```java
gcScheduler.scheduleAtFixedRate(() -> {...}, 1, 60, TimeUnit.MINUTES);
```

**问题**: GC 每 1 小时执行一次，失败上传的 `.tmp` 文件最多滞留 1 小时。

**影响**: 频繁失败上传积累大量临时文件，浪费磁盘。

**方案**: 缩短至 5-10 分钟。

---

#### [P2-中] BACK-008: `NettyServerUtils` 内部和外部 Shutdown 的 EventLoopGroup 生命周期冲突

**文件**: `NettyServerUtils.java:78-84`

**问题**: `start0()` finally 中关闭 boss/worker Group。但 NameNode/DataNode 的 `shutdown()` 也用 `ServerShutdownHelper` 尝试关闭 workerGroup。可能导致重复 shutdownGracefully。

**影响**: 优雅关闭顺序不可控，极端场景可能异常。

**方案**: 统一由 `NettyServerUtils` 管理内部创建的 Group 生命周期，外部只管理各自传入的 Group。

---

## 4. 网络层审计

> **审计人**: 网络技术专家

### [P0-严重] NET-001: Token 明文传输，全网无加密

**文件**: `PacketDecoder.java:74`、`PacketEncoder.java:29-34`

**问题**: Token 以 UTF-8 明文在 Packet 中传输，所有节点间通信无 TLS/SSL。Packet 首字节就是 Magic=0xCAFEBABE，网络嗅探可直接捕获全量通信内容。

**影响**: 攻击者通过网络嗅探获取 Token 后可伪造所有请求（上传/下载/删除）。

**方案**: 引入 Netty `SslHandler`，至少对 Token 做 HMAC 摘要比对而非原文比对。

---

### [P1-高] NET-002: `PacketDecoder` 流模式状态不重置

**文件**: `PacketDecoder.java:22-23`

```java
private long fileBytesToRead = 0;
```

**问题**: `fileBytesToRead` 在流传输错误后（如 streamLength 被篡改、连接中途断开）无法重置。该 Channel 永久处于错误状态。

**影响**: 连接被错误状态锁定，无法处理后续请求，必须断开重建。

**方案**: 在 `exceptionCaught()` 或 `channelInactive()` 中重置 `fileBytesToRead = 0`。

---

### [P1-高] NET-003: 连接池无 `maxPendingAcquires` 限制

**文件**: `ChannelPoolUtils.java`

**问题**: `createDefaultPoolMap()` 创建 `FixedChannelPool` 但未配置 `maxPendingAcquires` 和 `acquireTimeout`。

**影响**: 高并发时 `pool.acquire()` 可能无限阻塞等待连接，形成请求堆积。

**方案**: 设置 `maxPendingAcquires`（如 256）+ `acquireTimeout`（如 5s），超时抛出异常触发故障转移。

---

### [P2-中] NET-004: 无空闲连接检测

**问题**: 所有服务端 Pipeline 未配置 `IdleStateHandler`。客户端异常断开（无 FIN/RST）时，服务端连接永驻。

**影响**: 半开连接堆积，消耗文件描述符和内存。

**方案**: `NettyServerUtils` 的 Pipeline 中添加 `IdleStateHandler(60, 0, 0)`，Handler 中 `userEventTriggered()` 关闭空闲连接。

---

### [P2-中] NET-005: DataNode 下载无背压机制

**文件**: `DataNodeHandler.java:196-224`

```java
DefaultFileRegion region = new DefaultFileRegion(file, 0, fileLength);
ctx.writeAndFlush(region); // 不检查 isWritable()
```

**问题**: 大文件下载不检查 `channel.isWritable()`，若客户端消费慢，写缓冲持续膨胀。

**影响**: 极端情况可能触发 OOM。

**方案**: 写前检查 `channel.isWritable()`，注册 `channelWritabilityChanged` 监听器暂停/恢复写入。

---

### [P3-低] NET-006: 心跳无响应校验

**问题**: NameNode/DataNode 发出心跳后立即释放连接回池，未校验 Registry 是否回复成功。

**影响**: Registry 静默丢包时，节点不知道自己已"离线"。

**方案**: 心跳请求读取响应确认，超时或错误时记录告警。

---

### [P3-低] NET-007: 编解码器注释与实现不一致

**文件**: `PacketEncoder.java:12`

**注释**: `Magic(4) + Version(1) + Command(1) + TokenLength(4) + Token(M) + Length(4) + Data(N)`
**实现**: 末尾还有 `streamLength(8)`，注释遗漏。

---

## 5. 客户端/Driver审计

> **审计人**: 前端工程师 (Driver)

### [P1-高] CLI-001: `requestUploadPermission()` 无限循环无退避

**文件**: `JNFSDriver.java:491-508`

```java
while (true) {
    // ... WAIT 响应
    Thread.sleep(1000); // 无上限重试
}
```

**问题**: 仅 `Thread.sleep(1000)` 无最大重试次数、无指数退避。若文件永远无法上传（如 DataNode 全部下线），客户端永久阻塞。

**影响**: 调用方线程永久卡死，资源泄漏。

**方案**: 限制最大重试次数（如 100 次），每次倍增加 sleep（指数退避）。

---

### [P1-高] CLI-002: `SyncHandler` 存在 TOCTOU 竞态条件

**文件**: `JNFSDriver.java:692-702`

```java
public void channelInactive(ChannelHandlerContext ctx) {
    channelClosed = true;
    if (queue.isEmpty()) {           // 检查
        // ...                          ← 竞态窗口
        queue.offer(errorPacket);    // 放入错误包
    }
}
```

**问题**: `queue.isEmpty()` 判断与 `queue.offer()` 之间存在 TOCTOU 窗口。正常响应可能在检查之后、插入之前到达，然后被错误包覆盖。

**影响**: 丢失正常响应，调用方收到"连接已断开"错误。

**方案**: 改用 `queue.offer(errorPacket, timeout)` 或在 `getResponse()` 中同时检查 `channelClosed` 和 `queue`。

---

### [P2-中] CLI-003: 脏连接被释放回连接池

**文件**: `JNFSDriver.java:623-673`

**问题**: `doSendRequest()` 的 finally 块无条件 `pool.release(channel)`。即使 `handler.getResponse()` 超时或抛异常，异常状态的 channel 也会被放回池。

**影响**: 后序请求从池中获取到脏连接，请求失败。

**方案**: 释放前校验 `channel.isActive()`，异常时 `channel.close()` 而非释放。

---

### [P2-中] CLI-004: `DownloadHandler` 无 `ReadTimeoutHandler`

**文件**: `JNFSDriver.java:773`

**问题**: `DownloadHandler` 的 Bootstrap 未注册 `ReadTimeoutHandler`。虽然有 30 分钟总超时（`waitForCompletion`），但无法及时检测连接假死。

**影响**: 网络静默时 30 分钟内无法感知，资源长期占用。

**方案**: Bootstrap 添加 `ReadTimeoutHandler(60, TimeUnit.SECONDS)`。

---

### [P2-中] CLI-005: `DownloadHandler` 的 `fileSize` 可被覆盖

**文件**: `JNFSDriver.java:787-797`

```java
long streamLen = packet.getStreamLength();
if (streamLen > 0) {
    fileSize = streamLen;    // 路径1
} else {
    fileSize = Long.parseLong(sizeStr); // 路径2
}
```

**问题**: `streamLength` 和 `data` 同时携带不同的 fileSize 值时会发生覆盖，导致 `receivedBytes >= fileSize` 条件永远不满足。

**影响**: `waitForCompletion()` 永久阻塞直到 30 分钟超时。

**方案**: 仅从一处读取 fileSize，或写入后对已设置的值不再覆盖。

---

### [P3-低] CLI-006: `JNFSDriver.close()` 连接池关闭永不执行

**文件**: `JNFSDriver.java:304-306`

```java
if (poolMap instanceof Closeable) {
    ((Closeable) poolMap).close();
}
```

**问题**: `ChannelPoolMap` 不继承 `Closeable`，instanceof 为 false，池关闭逻辑死代码。

**方案**: 显式遍历池中所有连接进行关闭。

---

### [P3-低] CLI-007: 三个 `uploadFile()` 重载统一抛出 `throws Exception`

**问题**: 所有公共 API 声明 `throws Exception`，无 `JNFSException` 细分体系。调用方无法区分 Auth/Timeout/Unavailable 等场景。

**方案**: 定义 `JNFSException` 及其子类：`AuthenticationException`、`TimeoutException`、`ServiceUnavailableException`。

---

## 6. UI层审计

> **审计人**: UI工程师

### [P1-高] UI-001: Dashboard 无认证机制

**文件**: `DashboardServer.java`

**问题**: 端口 15367 的 HTTP 仪表盘完全无身份认证，`/api/security` 接口暴露 Token 配置状态。

**影响**: 任何人可直接查看系统监控数据（节点状态、存储容量）。

**方案**: Dashboard 添加 Basic Auth 或 Token 校验。

---

### [P2-中] UI-002: Dashboard XSS 风险

**文件**: `DashboardServer.java:58-78` (`getNodesJson()`)

**问题**: 字符串拼接构建 JSON，前端 `innerHTML` 渲染。若 DataNode 地址被伪造为 `<script>alert(1)</script>`，可能触发 XSS。

**影响**: 注册中心被攻击后，仪表盘查看者可被执行脚本。

**方案**: 使用 Jackson/Gson 构建 JSON；前端 `textContent` 替代 `innerHTML`。

---

### [P2-中] UI-003: 错误信息泄露内部实现细节

**文件**: `DataNodeHandler.java:113`、`JNFSDriver.java:566`

```java
NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR,
    ("服务端错误: " + e.getMessage()).getBytes(...));
```

**问题**: 多处直接将 Java 异常消息（含文件绝对路径、内部类名）返回给客户端。

**影响**: 信息泄露有助于攻击者摸清系统结构。

**方案**: 返回预定义错误码和通用消息，详细错误仅记录日志。

---

### [P3-低] UI-004: Dashboard 使用已弃用的 `com.sun.net.httpserver`

**问题**: JDK 内部 API 可能在后续版本中删除。

**方案**: 迁移至 Netty HTTP 或 Jetty 嵌入式服务器。

---

## 7. 质量与可维护性审计

> **审计人**: QA 挑刺专家

### [P0-严重] QA-001: 项目完全没有单元测试

**问题**: `find . -name "*Test*.java"` 或 `find . -path "*/test/*"` 均无结果。关键路径（SegmentedLocks、SecurityUtil、PacketDecoder、负载均衡、MySQL元数据操作）零覆盖。

**影响**: 任何代码变更无自动化保护，回归风险极高。

**方案**: 
1. `jnfs-common` 优先：`SegmentedLocks`、`SecurityUtil`、`PacketDecoder`/`PacketEncoder`
2. `jnfs-namenode` 次之：`WeightedRandomStrategy`、`MetadataCacheManager`
3. 集成测试：端到端上传/下载流程

---

### [P0-严重] QA-002: 配置文件中暴露生产凭证

**文件**: `jnfs-namenode/src/main/resources/namenode.yml`

**问题**: Git 仓库中提交了真实 MySQL IP、密码和内部服务地址。

**影响**: 凭证泄漏，任何能访问仓库的人可连接生产数据库。

**方案**: 立即轮换已泄露的密码；配置文件改为模板，真实值通过环境变量注入。

---

### [P0-严重] QA-003: 硬编码安全密钥

**文件**: `SecurityConfig.java:26-27`

```java
public static final String DEFAULT_TOKEN = "jnfs-secure-token-2025";
static final byte[] DEFAULT_AES_KEY = "jnfs-aes-key-256bit-secure-key!!".getBytes();
```

**问题**: 反编译 jar 即可获得 Token 和 AES 密钥。生产环境若忘记配置，使用这些硬编码值。

**影响**: 全局安全形同虚设。

**方案**: 强制要求配置，无配置时拒绝启动（而非返回默认值）。

---

### [P0-严重] QA-004: SnakeYAML 2.0 存在 CVE-2022-1471

**文件**: `pom.xml:26` — `<snakeyaml.version>2.0</snakeyaml.version>`

**问题**: SnakeYAML 2.0 存在反序列化 RCE 漏洞（CVE-2022-1471）。

**影响**: 若加载不可信 YAML 配置，可能触发远程代码执行。

**方案**: 升级至 SnakeYAML 2.3+。

---

### ~~[P1-高] QA-005: RegistryServer 无优雅关闭~~ **[已修复 2026-06-09]**

**问题**: NameNodeServer 和 DataNodeServer 均有 shutdown hook + `ServerShutdownHelper`，但 RegistryServer 完全缺失。Dashboard HTTP Server 和 Cleaner 线程池无法释放。

**修复方案**: 三处修改实现完整优雅关闭：

1. **DashboardServer**: 将 `HttpServer` 从局部变量提升为实例字段 `httpServer`，新增 `stop()` 方法调用 `httpServer.stop(0)`，使 Dashboard HTTP 服务可被外部关闭。

2. **RegistryHandler**: 新增 `shutdown()` 静态方法，关闭内部 `cleanerExecutor`（定时清理过期节点的 ScheduledExecutorService）。

3. **RegistryServer**: 添加 `AtomicBoolean running` 幂等标志 + `DashboardServer` 实例引用 + `Runtime.addShutdownHook()` + `shutdown()` 方法。`shutdown()` 按序关闭：Dashboard HTTP → RegistryHandler cleaner。Netty EventLoopGroup 由 `NettyServerUtils.start0()` 内部 finally 块负责。编译验证通过（BUILD SUCCESS）。

---

### [P1-高] QA-006: `mysql/jnfs.sql` 含 `DROP TABLE`

**文件**: `mysql/jnfs.sql:23-24`

```sql
DROP TABLE IF EXISTS `file_location`;
CREATE TABLE ...
```

**问题**: 运维人员直接执行此 SQL 会清空现有数据。

**影响**: 生产数据丢失。

**方案**: 分离为 DDL 初始化脚本（含创建）和迁移脚本（含 DROP）；添加 `-- WARNING: This drops existing data` 注释。

---

### [P2-中] QA-007: Caffeine 依赖版本未统一管理

**文件**: `jnfs-namenode/pom.xml:38`

```xml
<dependency>
    <groupId>com.github.ben-manes.caffeine</groupId>
    <artifactId>caffeine</artifactId>
    <version>3.1.8</version>  <!-- 未在根POM的dependencyManagement中 -->
</dependency>
```

**方案**: 将 Caffeine 版本提升至根 POM 的 `<dependencyManagement>`。

---

### [P2-中] QA-008: Nexus 仓库使用 HTTP 明文

**文件**: `pom.xml:37`

```xml
<url>http://100.10.10.215:8081/repository/maven-releases/</url>
```

**影响**: Maven 部署时凭证明文传输。

**方案**: 改为 HTTPS。

---

### [P2-中] QA-009: 错误消息中英混杂

**问题**: 有的返回 `"Authentication Failed"`，有的返回 `"未知命令"`，不一致。

**方案**: 统一返回英文或使用 i18n 资源文件。

---

### [P2-中] QA-010: 缺少 Docker 化支持

**问题**: 项目无 `Dockerfile` 或 `docker-compose.yml`。

**方案**: 为每个服务模块添加 Dockerfile，提供 `docker-compose.yml` 用于一键部署。

---

### [P3-低] QA-011: Netty 版本较旧

**问题**: `netty.version` 为 `4.1.94.Final`，最新稳定版为 `4.1.112+`。

**方案**: 升级至最新 4.1.x。

---

### [P3-低] QA-012: ExampleApp 硬编码下载路径

**文件**: `ExampleApp.java:133`

```java
String downloadPath = "D:\\data\\jnfs\\download\\";
```

**方案**: 使用 `System.getProperty("java.io.tmpdir")` 或用户主目录。

---

## 8. 安全性审计

跨模块综合安全性评估。

### 8.1 Token 认证体系

| 问题 | 级别 | 详情 |
|------|------|------|
| 默认 Token 硬编码 | P0 | 反编译可获取，见 QA-003 |
| Token 明文传输 | P0 | 无 TLS，见 NET-001 |
| Token 无过期/轮换 | P1 | 一旦泄露永久有效 |
| Dashboard 无认证 | P1 | 健康检查接口暴露 |

### 8.2 数据安全

| 问题 | 级别 | 详情 |
|------|------|------|
| AES 密钥硬编码 | P0 | 与 Token 同源问题 |
| 配置文件泄漏凭证 | P0 | MySQL 密码/内网地址硬编码在 YAML |
| 无数据完整性校验 | P1 | 下载时不验证文件完整性 |
| 协议无 CRC/Checksum | P1 | 位翻转静默通过 |

### 8.3 防御机制

| 问题 | 级别 | 详情 |
|------|------|------|
| 无速率限制 | P2 | 无 DDoS 防护 |
| 无请求超时控制 | P2 | 慢连接占用资源 |
| 路径遍历防御 | P3 | 已有校验（DataNodeHandler.getStorageFile），较完备 |

---

## 9. 全部漏洞汇总

| # | 编号 | 类别 | 级别 | 简述 | 影响 |
|---|------|------|------|------|------|
| 1 | QA-003 | 安全 | P0 | 硬编码默认 Token 和 AES Key | 反编译可获取全局密钥 |
| 2 | QA-001 | 质量 | P0 | 零单元测试 | 无自动化回归保护 |
| 3 | QA-002 | 安全 | P0 | 配置文件泄漏生产凭证 | MySQL 密码暴露 |
| 4 | QA-004 | 安全 | P0 | SnakeYAML 2.0 CVE-2022-1471 | 反序列化 RCE |
| 5 | NET-001 | 网络 | P0 | Token 明文传输，全网无加密 | 嗅探获取 Token |
| 6 | BACK-001 | 后端 | P0 | 分布式锁 DELETE+INSERT 非原子 | 并发重复上传 |
| 7 | BACK-002 | 后端 | P0 | File 模式 queryByHash 恒返回 null | 缓存禁用后文件"丢失" |
| 8 | BACK-004 | 后端 | P0 | file_metadata 无 hash UNIQUE 约束 | 重复记录永久存在 |
| 9 | ARCH-001 | 架构 | P0 | DataNode 无副本，单点故障 | 数据丢失 |
| 10 | ~~QA-005~~ | ~~质量~~ | ~~P1~~ | ~~RegistryServer 无优雅关闭 (已修复)~~ | ~~资源泄漏~~ |
| 11 | QA-006 | 质量 | P1 | mysql/jnfs.sql 含 DROP TABLE | 误执行导致数据丢失 |
| 12 | ARCH-002 | 架构 | P1 | 协议版本字段未校验 | 无法兼容旧版本 |
| 13 | BACK-003 | 后端 | P1 | 元数据提交与锁释放竞态窗口 | 多 NameNode 重复上传 |
| 14 | BACK-005 | 后端 | P1 | TimedCache/DB锁过期时间不匹配 | 10-30分钟不一致 |
| 15 | NET-002 | 网络 | P1 | PacketDecoder 流状态不重置 | 连接错误状态锁定 |
| 16 | NET-003 | 网络 | P1 | 连接池无 maxPendingAcquires | 高并发无限堆积 |
| 17 | CLI-001 | 客户端 | P1 | requestUploadPermission 无限循环 | 线程永久卡死 |
| 18 | CLI-002 | 客户端 | P1 | SyncHandler TOCTOU 竞态 | 丢失正常响应 |
| 19 | UI-001 | UI | P1 | Dashboard 无认证 | 监控数据暴露 |
| 20 | ARCH-003 | 架构 | P2 | Handler 全 static 无法测试 | 可测试性差 |
| 21 | ARCH-004 | 架构 | P2 | 心跳逻辑重复 70% | 维护成本 |
| 22 | BACK-006 | 后端 | P2 | DataNode 临时文件残留 | 磁盘泄漏 |
| 23 | BACK-007 | 后端 | P2 | DataNode GC 间隔过大(1h) | 临时文件堆积 |
| 24 | BACK-008 | 后端 | P2 | EventLoopGroup 生命周期冲突 | 优雅关闭异常 |
| 25 | NET-004 | 网络 | P2 | 无 IdleStateHandler | 半开连接堆积 |
| 26 | NET-005 | 网络 | P2 | 下载无背压机制 | 写缓冲溢出 |
| 27 | CLI-003 | 客户端 | P2 | 脏连接释放回池 | 后续请求失败 |
| 28 | CLI-004 | 客户端 | P2 | Download无ReadTimeoutHandler | 假死30分钟无感知 |
| 29 | CLI-005 | 客户端 | P2 | DownloadHandler fileSize 覆盖 | 永久阻塞 |
| 30 | UI-002 | UI | P2 | Dashboard XSS 风险 | 脚本注入 |
| 31 | UI-003 | UI | P2 | 错误信息泄露内部路径 | 信息泄露 |
| 32 | QA-007 | 质量 | P2 | Caffeine 版本未统一管理 | 版本失控 |
| 33 | QA-008 | 质量 | P2 | Nexus 仓库 HTTP 明文 | 凭证泄漏 |
| 34 | QA-009 | 质量 | P2 | 错误消息中英混杂 | 用户体验差 |
| 35 | QA-010 | 质量 | P2 | 缺少 Docker 支持 | 部署不便 |
| 36 | NET-006 | 网络 | P3 | 心跳无响应校验 | 静默丢包 |
| 37 | NET-007 | 网络 | P3 | 注释与实现不一致 | 误导 |
| 38 | CLI-006 | 客户端 | P3 | close() 连接池关闭死代码 | 资源泄漏 |
| 39 | CLI-007 | 客户端 | P3 | 异常体系粗糙(throws Exception) | 难以精准处理 |
| 40 | UI-004 | UI | P3 | Dashboard 使用弃用 API | 未来不兼容 |
| 41 | QA-011 | 质量 | P3 | Netty 版本较旧 | 缺少 bug 修复 |
| 42 | QA-012 | 质量 | P3 | ExampleApp 硬编码路径 | 跨平台问题 |

---

## 10. 解决方案路线图

### 10.1 第一周 (P0 修复)

1. **SECURITY**: 立即轮换已泄露的 MySQL 密码和 Token
2. **SECURITY**: 删除 `SecurityConfig` 中的默认 Token/Key，无配置拒绝启动
3. **SECURITY**: 升级 SnakeYAML 至 2.3+
4. **BACKEND**: 修复分布式锁原子性问题（`INSERT ... ON DUPLICATE KEY UPDATE`）
5. **BACKEND**: `file_metadata.file_hash` 添加 UNIQUE 约束
6. **BACKEND**: File 模式 `queryByHash()` 实现文件扫描回源
7. **DB**: 从 Git 中移除含凭证的配置文件，改为 `.example` 模板

### 10.2 第二周 (P1 修复)

8. **NETWORK**: 修复 `PacketDecoder.fileBytesToRead` 状态重置
9. **NETWORK**: 连接池添加 `maxPendingAcquires` + `acquireTimeout`
10. **CLIENT**: `requestUploadPermission` 添加指数退避和最大重试
11. **CLIENT**: 修复 `SyncHandler` TOCTOU 竞态
12. **REGISTRY**: 添加 shutdown hook
13. **DB**: `mysql/jnfs.sql` 分离 DDL 初始化和迁移脚本
14. **BACKEND**: 统一 TimedCache 和 DB 锁过期时间为相同值

### 10.3 第一个月 (P2 修复 + 测试)

15. **TEST**: 为核心模块添加单元测试（目标覆盖率 60%+）
16. **ARCHITECTURE**: 心跳逻辑提取公共工具类
17. **NETWORK**: 添加 `IdleStateHandler` + 背压机制
18. **CLIENT**: 修复脏连接释放、ReadTimeout、fileSize 覆盖
19. **UI**: Dashboard XSS 修复 + 认证机制
20. **QUALITY**: 统一错误消息 + Caffeine 依赖管理

### 10.4 第二~三个月 (架构升级)

21. **ARCHITECTURE**: 实现多副本存储（默认 3 副本）
22. **PROTOCOL**: 添加 CRC 校验 + 版本协商
23. **SECURITY**: 部署 TLS/SSL + Token 轮换机制
24. **INFRA**: Docker 化 + docker-compose 一键部署
25. **OBSERVABILITY**: 集成 Prometheus 指标暴露

---

## 附录

### A. 审计团队

| 角色 | 职责 | 审计成果 |
|------|------|----------|
| **架构师** (team-lead) | 整体架构、模块依赖、设计模式 | 4 项发现 |
| **后端工程师** | 数据存储、并发控制、资源管理 | 8 项发现 |
| **网络技术专家** | 协议设计、编解码、连接池、安全性 | 7 项发现 |
| **前端工程师** | 客户端 SDK、Driver | 7 项发现 |
| **QA 挑刺专家** | 测试、日志、配置、依赖、安全 | 12 项发现 |

### B. 审计方法

- 静态代码审查（逐文件阅读 + 交叉验证）
- 架构推理（推演并发场景、故障场景）
- 依赖分析（pom.xml 依赖树 + Maven 版本管理）
- CVE 扫描（已知漏洞版本检查）

### C. 参考

- [Netty 最佳实践](https://netty.io/wiki/user-guide-for-4.x.html)
- [HDFS 架构](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Raft 一致性协议](https://raft.github.io/)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
