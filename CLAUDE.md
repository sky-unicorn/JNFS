# JNFS Project Rules

## Schema Migration (数据迁移强制规则)

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

## Database MCP Usage

When using database MCP tools, **only use `jnfs-db` MCP**. Do not use `anal-business-db`, `anal-system-db`, or `gridfoundation-db` unless explicitly instructed.

## Storage Compatibility

The system supports two storage modes: **file** and **mysql**. When writing or modifying any code that involves data storage (reading, writing, querying, deleting), you **must ensure compatibility with both modes**, not just one. This includes:

- All data access logic must work correctly under both `file` and `mysql` storage modes.
- Do not implement or test against only one mode and assume the other works.
- When changing storage-related code, verify the change is valid for both modes.

## Common Utilities (jnfs-common)

The `jnfs-common` module provides shared utility classes that eliminate code duplication across modules. **When writing new code, always use these utilities instead of re-implementing the patterns.**

### DaemonThreadFactory
**Package:** `org.jnfs.common.DaemonThreadFactory`
**When to use:** Any time you need a `ScheduledExecutorService` or `ThreadFactory` that creates daemon threads (background threads that don't prevent JVM shutdown).
```java
Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("MyThread-Name"));
```
**Replaces:** Anonymous `ThreadFactory` implementations with `t.setDaemon(true)`.

### SegmentedLocks
**Package:** `org.jnfs.common.SegmentedLocks`
**When to use:** When you need fine-grained locking by key (e.g., file hash, cache key) instead of a single global lock.
```java
private static final SegmentedLocks locks = new SegmentedLocks(128);
synchronized (locks.getLock(key)) { ... }
```
**Replaces:** Manual `Object[] SEGMENT_LOCKS` arrays with static initializers.

### NettyHandlerHelper
**Package:** `org.jnfs.common.NettyHandlerHelper`
**When to use:** In any Netty `SimpleChannelInboundHandler` that needs to validate tokens or send response packets.
```java
NettyHandlerHelper.validateToken(packet.getToken());
NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, data);
NettyHandlerHelper.sendError(ctx, "Error message");
```
**Replaces:** Private `sendResponse()` methods and `Constants.getValidToken().equals()` checks in each handler.

### CommonChannelPoolHandler
**Package:** `org.jnfs.common.CommonChannelPoolHandler`
**When to use:** As the `ChannelPoolHandler` for any `FixedChannelPool` or `SimpleChannelPool`. It auto-configures `PacketDecoder`/`PacketEncoder` in the pipeline.
```java
new CommonChannelPoolHandler();           // with active check on acquire
new CommonChannelPoolHandler(false);      // without active check (for Registry pools)
```
**Replaces:** Anonymous `ChannelPoolHandler` classes that only add PacketDecoder/PacketEncoder.

### ChannelPoolUtils
**Package:** `org.jnfs.common.ChannelPoolUtils`
**When to use:** When creating a `ChannelPoolMap<InetSocketAddress, SimpleChannelPool>` for connection pooling.
```java
ChannelPoolUtils.createDefaultPoolMap(workerGroup);
ChannelPoolUtils.createDefaultPoolMap(workerGroup, maxConnections);
ChannelPoolUtils.createPoolMap(workerGroup, customHandler, maxConnections);
```
**Replaces:** `AbstractChannelPoolMap` anonymous classes with inline Bootstrap configuration.

### NettyClientBootstrap
**Package:** `org.jnfs.common.NettyClientBootstrap`
**When to use:** When creating a Netty client `Bootstrap` for one-off connections (not pooled).
```java
Bootstrap b = NettyClientBootstrap.create(group);                                    // bare bootstrap
Bootstrap b = NettyClientBootstrap.createWithHandler(group, handler1, handler2);     // with pipeline
Channel ch = NettyClientBootstrap.connectSync(b, host, port, 6000);                 // sync connect
```
**Replaces:** Repeated `new Bootstrap().group().channel().option().handler(ChannelInitializer...)` patterns.

### ServerShutdownHelper
**Package:** `org.jnfs.common.ServerShutdownHelper`
**When to use:** In server `shutdown()` methods to cleanly release schedulers, connection pools, and EventLoopGroups.
```java
ServerShutdownHelper.shutdownAll(LOG, "NameNodeServer", runningFlag,
    new ScheduledExecutorService[]{scheduler1, scheduler2},
    poolMap, workerGroup);
```
**Replaces:** Hand-rolled shutdown sequences iterating pools, checking `isShutdown()`, etc.

### NetUtils
**Package:** `org.jnfs.common.NetUtils`
**When to use:** To get the local machine's IP address. **Always prefer this over `cn.hutool.core.net.NetUtil.getLocalhostStr()`** as it filters out loopback addresses.
```java
String ip = NetUtils.getLocalIp();
```
