# Common Utilities (jnfs-common)

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

### DataDirResolver
**Package:** `org.jnfs.common.DataDirResolver`
**When to use:** Any time you need to locate a runtime data file (e.g. `namenode_meta.log`, `node_id.dat`, `meta_version`) that must live alongside the application home. Resolves to `APP_HOME` system property (set by startup scripts), falling back to `user.dir`.
```java
File dataDir = DataDirResolver.dataDir();
File logFile = DataDirResolver.resolve("namenode_meta.log");
```
**Replaces:** Bare relative paths like `new File("namenode_meta.log")` or manual `System.getProperty("APP_HOME", user.dir)` lookups. Using this utility ensures migration code and business code always reference the same directory regardless of JVM working directory.
