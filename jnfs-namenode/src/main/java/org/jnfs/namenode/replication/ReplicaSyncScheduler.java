package org.jnfs.namenode.replication;

import org.jnfs.common.CommandType;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.NettyClientBootstrap;
import org.jnfs.common.NodeAddressResolver;
import org.jnfs.common.Packet;
import org.jnfs.common.SecurityConfig;
import org.jnfs.common.replication.ReplicaSyncTask;
import org.jnfs.common.replication.ReplicationGroup;
import org.jnfs.common.replication.SyncTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;

/**
 * 夜间对账同步调度器（§7 + 决策 10/11，JDBC 模式 mysql/h2 共用）。
 * <p>
 * 职责定位（§7.1）：对账补救，不是副本唯一来源。
 * <ul>
 *   <li>核心窗口 01:00-03:00：cron 触发，高优先级补齐副本</li>
 *   <li>03:00 后软截止：低资源模式（并发降为 1）继续跑未完成任务</li>
 *   <li>启动恢复（决策 10）：扫描 PENDING/IN_FLIGHT 任务恢复派发</li>
 * </ul>
 * <p>
 * 差集计算（§7.2 + M6）：遍历 file_metadata，按行内 replication_factor 判定 expected，
 * 对比 file_location 中 ACTIVE 副本数，缺则生成 PENDING 任务。
 * <p>
 * file 模式已退役；JDBC 模式（mysql 集群 / h2 同机多磁盘）均构造本类。
 */
public class ReplicaSyncScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSyncScheduler.class);

    /** 核心窗口并发任务数（§7.6） */
    private static final int CORE_CONCURRENCY = 4;
    /** 软截止后并发任务数（§7.6 低资源模式） */
    private static final int SOFT_DEADLINE_CONCURRENCY = 1;
    /** 核心窗口结束小时（03:00） */
    private static final int SOFT_DEADLINE_HOUR = 3;
    /** 分页查询 file_metadata 的页大小（防止 OOM） */
    private static final int PAGE_SIZE = 500;

    private final DataSource dataSource;
    private final ReplicationGroupStore replicationGroupStore;
    private final ReplicaSyncTaskStore taskStore;
    private final String namenodeHost;
    private final int namenodePort;
    private final EventLoopGroup workerGroup;

    /** 对账策略配置（窗口/限速等，MVP 使用硬编码默认值） */
    private final SyncPolicy policy;

    /** stale-IN_FLIGHT reaper 阈值（分钟），对齐 ReplicaPullWorker HARD_TIMEOUT（10min 拉 + 20min 余量） */
    private static final int STALE_INFLIGHT_MINUTES = 30;

    private ScheduledExecutorService scheduler;
    private java.util.concurrent.ExecutorService dispatchExecutor;
    private volatile boolean running = false;
    /** 标志 startup-recovery 是否已执行（避免重复触发，I1 修复） */
    private volatile boolean startupRecoveryDone = false;

    /** 并发控制信号量（动态调整核心/软截止模式） */
    private volatile Semaphore concurrencySemaphore = new Semaphore(CORE_CONCURRENCY);

    public ReplicaSyncScheduler(DataSource dataSource,
                                 ReplicationGroupStore replicationGroupStore,
                                 String namenodeHost, int namenodePort,
                                 EventLoopGroup workerGroup,
                                 SyncPolicy policy) {
        this.dataSource = dataSource;
        this.replicationGroupStore = replicationGroupStore;
        this.taskStore = new ReplicaSyncTaskStore(dataSource);
        this.namenodeHost = namenodeHost;
        this.namenodePort = namenodePort;
        this.workerGroup = workerGroup;
        this.policy = (policy != null) ? policy : SyncPolicy.DEFAULT;
    }

    /**
     * 启动：仅注册 cron 调度 + IN_FLIGHT 回退。**不立即派发** startup-recovery 任务。
     * <p>
     * I1 修复（决策 10 时序）：startup-recovery 派发延后到首次 fetchDataNodesFromRegistry 完成后，
     * 由 NameNodeServer 调用 {@link #runStartupRecovery()} 触发。否则 NodeAddressResolver 映射为空，
     * resolveNodeAddress 返回 null → 所有 startup-recovery 任务 markFailed、retry_count++、
     * NameNode 重启 4 次后全进告警。
     */
    public void start() {
        scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("ReplicaSyncScheduler"));
        dispatchExecutor = java.util.concurrent.Executors.newFixedThreadPool(
                CORE_CONCURRENCY, new DaemonThreadFactory("ReplicaSync-Dispatch"));

        // 启动恢复第一步：IN_FLIGHT → PENDING（不派发，等 runStartupRecovery）
        try {
            int resetCount = taskStore.resetInFlightToPending();
            LOG.info("ReplicaSyncScheduler 启动恢复: {} 个 IN_FLIGHT 任务回退为 PENDING（派发延后到首次 discovery）", resetCount);
        } catch (SQLException e) {
            LOG.error("ReplicaSyncScheduler resetInFlightToPending 失败", e);
        }

        // cron 触发核心窗口（默认凌晨 1 点，§7.1）+ stale reaper（I2）+ 手动触发轮询
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // I2：stale-IN_FLIGHT reaper（每 tick 先回收挂起任务）
                reapStaleInFlight();

                // 手动触发轮询：检查 replication_control.manual_sync_requested
                pollManualSync();

                int hour = java.time.LocalTime.now().getHour();
                // 核心窗口 01:00 触发，软截止 03:00
                if (hour == 1 && !running) {
                    LOG.info("ReplicaSyncScheduler 核心窗口触发（01:00），开始对账");
                    runReconciliation();
                } else if (hour >= SOFT_DEADLINE_HOUR && running) {
                    // 软截止：降并发但不中断在途任务
                    concurrencySemaphore = new Semaphore(SOFT_DEADLINE_CONCURRENCY);
                    LOG.info("ReplicaSyncScheduler 软截止（03:00+），并发降为 {}", SOFT_DEADLINE_CONCURRENCY);
                }
            } catch (Exception e) {
                LOG.error("ReplicaSyncScheduler cron 调度异常", e);
            }
        }, 1, 1, TimeUnit.MINUTES);

        LOG.info("ReplicaSyncScheduler 已启动，核心窗口 {} 点，软截止 {} 点（startup-recovery 待首次 discovery 触发）",
                policy.coreWindowStartHour, SOFT_DEADLINE_HOUR);
    }

    /**
     * 启动恢复派发（I1 + S8）：由 NameNodeServer 在首次 fetchDataNodesFromRegistry 成功后调用。
     * <p>
     * 此刻 NodeAddressResolver 映射已建立，resolveNodeAddress 可正常解析 node_id→host:port。
     * <ul>
     *   <li>派发已回退的 PENDING 任务（决策 10 startup-recovery）</li>
     *   <li>S8：追加一次轻量 computeDiffAndEnqueue（非 01:00 启动也当天补救新差集）+ 派发当前 PENDING</li>
     * </ul>
     * 幂等：startupRecoveryDone 标志保证只执行一次。
     */
    public synchronized void runStartupRecovery() {
        if (startupRecoveryDone) {
            return;
        }
        startupRecoveryDone = true;
        LOG.info("ReplicaSyncScheduler runStartupRecovery 触发（首次 discovery 完成，映射已就绪）");

        try {
            // S8：追加一次差集计算（非核心窗口启动也能当天补救）
            int newTasks = computeDiffAndEnqueue();
            LOG.info("ReplicaSyncScheduler 启动差集计算完成，新生成 {} 个任务", newTasks);

            // 派发所有 PENDING 任务（含 startup-recovery 回退的 + 差集新入队的）
            List<ReplicaSyncTask> pendingTasks = taskStore.findPending();
            LOG.info("ReplicaSyncScheduler 启动恢复派发: {} 个 PENDING 任务", pendingTasks.size());
            dispatchTasks(pendingTasks);
        } catch (Exception e) {
            LOG.error("ReplicaSyncScheduler runStartupRecovery 异常", e);
        }
    }

    /**
     * I2：stale-IN_FLIGHT reaper。回收 update_time 早于阈值且仍 IN_FLIGHT 的任务（拉取失败/COMMIT 丢失）。
     * <p>
     * markFailed 后 retry_count++，回 PENDING；retry_count<4 的任务下一轮 dispatchTasks 自愈重派，
     * retry_count>=4 进告警。阈值 30 分钟远超单文件拉取时间，不会误伤正常进行中的任务。
     */
    private void reapStaleInFlight() {
        try {
            List<ReplicaSyncTask> stale = taskStore.findStaleInFlight(STALE_INFLIGHT_MINUTES);
            if (stale.isEmpty()) {
                return;
            }
            LOG.warn("ReplicaSyncScheduler stale reaper: 发现 {} 个 IN_FLIGHT 任务超 {} 分钟，回退重派", stale.size(), STALE_INFLIGHT_MINUTES);
            for (ReplicaSyncTask task : stale) {
                try {
                    taskStore.markFailed(task.getTaskId());
                } catch (SQLException e) {
                    LOG.warn("ReplicaSyncScheduler stale reaper: markFailed 失败 (taskId={})", task.getTaskId(), e);
                }
            }
            // 立即派发回退的任务（retry_count<4 的会重新派发）
            dispatchTasks(taskStore.findPending());
        } catch (SQLException e) {
            LOG.error("ReplicaSyncScheduler stale reaper 异常", e);
        }
    }

    /**
     * 轮询 replication_control.manual_sync_requested（手动触发对账）。
     * <p>
     * Registry 写入 manual_sync_requested=1（POST /api/replication/sync），
     * NameNode 每分钟 tick 检测到后执行 runReconciliation + 清除信号。
     * <p>
     * 跨进程实现：NameNode 直接 SQL 读写 replication_control 表
     *（ReplicationControlDao 在 registry 模块，NameNode 无法复用，用内联 SQL 替代）。
     */
    private void pollManualSync() {
        try (java.sql.Connection conn = dataSource.getConnection()) {
            // 查询
            boolean requested = false;
            try (java.sql.PreparedStatement stmt = conn.prepareStatement(
                    "SELECT manual_sync_requested FROM replication_control WHERE id = 1");
                 java.sql.ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    requested = rs.getInt(1) == 1;
                }
            }
            if (!requested) {
                return;
            }

            LOG.info("ReplicaSyncScheduler: 检测到手动触发对账请求，开始执行");
            // 清除信号
            try (java.sql.PreparedStatement stmt = conn.prepareStatement(
                    "UPDATE replication_control SET manual_sync_requested = 0, requested_at = NULL WHERE id = 1")) {
                stmt.executeUpdate();
            }
            // 执行对账（不在 scheduler cron 线程上直接跑，避免阻塞 tick）
            dispatchExecutor.execute(() -> {
                try {
                    runReconciliation();
                } catch (Exception e) {
                    LOG.error("ReplicaSyncScheduler: 手动触发对账异常", e);
                }
            });
        } catch (SQLException e) {
            LOG.warn("ReplicaSyncScheduler: 轮询 replication_control 失败", e);
        }
    }

    /**
     * 关闭：停止 cron 调度器 + dispatch 线程池 + workerGroup（S1）。不强制中断在途任务（决策 U4-N）。
     */
    public void shutdown() {
        running = false;
        if (dispatchExecutor != null && !dispatchExecutor.isShutdown()) {
            dispatchExecutor.shutdownNow();
        }
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow();
        }
        // S1：优雅关闭传给 scheduler 的 workerGroup（出站连 DataNode 发 PULL_CMD）
        if (workerGroup != null && !workerGroup.isShuttingDown()) {
            workerGroup.shutdownGracefully();
        }
        LOG.info("ReplicaSyncScheduler 已关闭");
    }

    /**
     * 获取任务存储（供 NameNodeHandler COMMIT 登记时使用）。
     */
    public ReplicaSyncTaskStore getTaskStore() {
        return taskStore;
    }

    /**
     * 核心对账流程（§7.2 + M6）。
     * <p>
     * 差集计算：遍历 file_metadata（分页），对每个 (storage_id, file_hash, replication_factor, primary_node)，
     * expected = replication_factor（行内快照，M6），actual = file_location 中该 hash 的 ACTIVE 副本数。
     * actual >= expected → 跳过；缺则生成 PENDING 任务。
     * <p>
     * MVP 简化（写进报告）：
     * <ul>
     *   <li>物理文件存在性校验二期；本阶段做元数据差集</li>
     *   <li>软截止仅降并发数，速率固定默认（DataNode 侧 50MB/s 不变）</li>
     *   <li>重试1次简化为"每轮对账把 PENDING 任务派一遍"（含 retry_count<4 的失败任务），下一轮覆盖</li>
     * </ul>
     */
    public void runReconciliation() {
        running = true;
        concurrencySemaphore = new Semaphore(CORE_CONCURRENCY);

        try {
            // 第一步：差集计算 → 生成 PENDING 任务
            int newTasks = computeDiffAndEnqueue();
            LOG.info("ReplicaSyncScheduler 差集计算完成，新生成 {} 个任务", newTasks);

            // 第二步：派发所有 PENDING 任务（含启动恢复 + 差集新入队的）
            List<ReplicaSyncTask> pendingTasks = taskStore.findPending();
            LOG.info("ReplicaSyncScheduler 待派发任务: {}", pendingTasks.size());
            dispatchTasks(pendingTasks);

        } catch (Exception e) {
            LOG.error("ReplicaSyncScheduler 对账异常", e);
        } finally {
            running = false;
        }
    }

    /**
     * 差集计算（§7.2 + M6）：分页遍历 file_metadata，找出副本不足的文件并入队。
     * <p>
     * source 节点判定：file_location 中该 hash 的源节点，优先 PRIMARY(replica_role=0)，
     * primary 副本丢失时 fallback 到任意 ACTIVE 副本节点（保证 primary 丢失仍可补齐）。
     * 一致性校验 MVP：仅做元数据差集；孤儿文件记告警不删（§10.3）。
     *
     * @return 新入队的任务数
     */
    private int computeDiffAndEnqueue() throws SQLException {
        int newTaskCount = 0;
        int offset = 0;

        while (true) {
            // 分页查 file_metadata（防止全量加载 OOM）
            List<FileMetaRow> page = queryFileMetadataPage(offset, PAGE_SIZE);
            if (page.isEmpty()) {
                break;
            }

            for (FileMetaRow row : page) {
                // expected = 行内 replication_factor 快照（M6）
                int expected = row.replicationFactor;
                if (expected <= 1) {
                    continue; // 单副本无需对账
                }

                // actual = file_location 中该 hash 的 ACTIVE(status=1) 副本数
                int actual = countActiveReplicas(row.fileHash);

                if (actual >= expected) {
                    continue; // 副本充足
                }

                // 缺失：选 source（组内任意持有节点）+ target（组内未持有者）
                // 修复：source 优先 PRIMARY，primary 副本丢失时 fallback 到任意 ACTIVE 副本。
                // 原 findPrimaryNode 严格查 replica_role=0，primary 丢失时返回 null 导致对账放弃，
                // 违背冗余存储目标（primary 丢失恰是最需补齐的场景）。
                String sourceNode = findSourceNode(row.fileHash);
                if (sourceNode == null) {
                    LOG.warn("ReplicaSyncScheduler: file_hash={} 无任何 ACTIVE 副本，跳过（所有副本都已失效）", row.fileHash);
                    continue;
                }

                List<String> existingNodes = findExistingNodes(row.fileHash);
                List<String> targets = chooseTargets(sourceNode, existingNodes, expected - actual);
                if (targets.isEmpty()) {
                    LOG.warn("ReplicaSyncScheduler: file_hash={} 无可用目标节点（组内均已持有或组定义缺失）", row.fileHash);
                    continue;
                }

                for (String target : targets) {
                    ReplicaSyncTask task = new ReplicaSyncTask();
                    task.setTaskId(UUID.randomUUID().toString());
                    task.setFileHash(row.fileHash);
                    task.setSourceNode(sourceNode);
                    task.setTargetNode(target);
                    task.setFileSize(row.fileSize);
                    try {
                        taskStore.upsertPending(task);
                        newTaskCount++;
                    } catch (SQLException e) {
                        LOG.warn("ReplicaSyncScheduler: upsertPending 失败 (hash={}, target={})",
                                row.fileHash, target, e);
                    }
                }
            }

            offset += PAGE_SIZE;
        }

        // 孤儿文件告警（§10.3 + §7.2 一致性校验 MVP）：检测但仅记日志，不自动删除
        logOrphanFiles();

        return newTaskCount;
    }

    /**
     * 派发 PENDING 任务：连目标 DataNode 发 PULL_CMD。
     * <p>
     * 对每个 PENDING 任务：用 Semaphore 控并发；连目标 DataNode（NettyClientBootstrap），
     * 发 PULL_CMD payload {@code fileHash|sourceAddr|namenodeAddr}。
     * 收到非 ERROR ACK → markInFlight。失败（连接失败/ERROR ACK）→ markFailed。
     * <p>
     * retry_count >= 4 的任务不自动派发（§7.8 告警阈值），等手动重试。
     */
    private void dispatchTasks(List<ReplicaSyncTask> tasks) {
        for (ReplicaSyncTask task : tasks) {
            // §7.8：retry_count >= 4 的任务不自动派发，进告警列表等手动重试
            if (task.getRetryCount() >= ReplicaSyncTaskStore.ALERT_RETRY_THRESHOLD) {
                continue;
            }

            // CAS 抢占 PENDING → IN_FLIGHT
            try {
                boolean claimed = taskStore.markInFlight(task.getTaskId());
                if (!claimed) {
                    continue; // 已被其他线程抢占
                }
            } catch (SQLException e) {
                LOG.warn("ReplicaSyncScheduler: markInFlight 失败 (taskId={})", task.getTaskId(), e);
                continue;
            }

            // 并发控制
            try {
                concurrencySemaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("ReplicaSyncScheduler: 并发等待被中断");
                break;
            }

            // 异步派发（不阻塞调度线程，使用共享 dispatchExecutor）
            dispatchExecutor.execute(() -> {
                try {
                    dispatchOne(task);
                } finally {
                    concurrencySemaphore.release();
                }
            });
        }
    }

    /**
     * 派发单个任务：连目标 DataNode，发 PULL_CMD，等 ACK。
     */
    private void dispatchOne(ReplicaSyncTask task) {
        // 解析 target node_id → host:port
        String targetAddr = resolveNodeAddress(task.getTargetNode());
        String sourceAddr = resolveNodeAddress(task.getSourceNode());

        if (targetAddr == null) {
            LOG.warn("ReplicaSyncScheduler: 目标节点 {} 地址无法解析，任务失败", task.getTargetNode());
            markFailedQuietly(task.getTaskId());
            return;
        }
        if (sourceAddr == null) {
            LOG.warn("ReplicaSyncScheduler: 源节点 {} 地址无法解析，任务失败", task.getSourceNode());
            markFailedQuietly(task.getTaskId());
            return;
        }

        // PULL_CMD payload: fileHash|sourceAddr|namenodeAddr
        String namenodeAddr = namenodeHost + ":" + namenodePort;
        String payload = task.getFileHash() + "|" + sourceAddr + "|" + namenodeAddr;

        SyncAckHandler handler = new SyncAckHandler();
        Bootstrap b = NettyClientBootstrap.createWithHandler(workerGroup, 5000, handler);

        try {
            Channel channel = NettyClientBootstrap.connectSync(b,
                    targetAddr.split(":")[0], Integer.parseInt(targetAddr.split(":")[1]), 6000);
            try {
                Packet request = new Packet();
                request.setCommandType(CommandType.DATA_REPLICA_PULL_CMD);
                request.setToken(SecurityConfig.getToken());
                request.setData(payload.getBytes(StandardCharsets.UTF_8));
                channel.writeAndFlush(request);

                // 等待 ACK（10s 超时，PULL_CMD 是信令不是数据传输）
                boolean success = handler.waitForAck(10, TimeUnit.SECONDS);
                if (success) {
                    LOG.info("ReplicaSyncScheduler: PULL_CMD 已发送到目标 {} (hash={})，任务 IN_FLIGHT",
                            task.getTargetNode(), task.getFileHash());
                    // markInFlight 已在 dispatchTasks 中完成
                } else {
                    LOG.warn("ReplicaSyncScheduler: PULL_CMD 被目标 {} 拒绝 (hash={})，任务失败",
                            task.getTargetNode(), task.getFileHash());
                    markFailedQuietly(task.getTaskId());
                }
            } finally {
                channel.close().sync();
            }
        } catch (Exception e) {
            LOG.warn("ReplicaSyncScheduler: 连接目标 {} 失败 (hash={})，任务失败: {}",
                    task.getTargetNode(), task.getFileHash(), e.getMessage());
            markFailedQuietly(task.getTaskId());
        }
    }

    private void markFailedQuietly(String taskId) {
        try {
            taskStore.markFailed(taskId);
        } catch (SQLException e) {
            LOG.error("ReplicaSyncScheduler: markFailed 失败 (taskId={})", taskId, e);
        }
    }

    /**
     * 解析 node_id → host:port。
     * 优先从 ReplicationGroupStore/dataNodes 映射获取，fallback NodeAddressResolver。
     */
    private String resolveNodeAddress(String nodeId) {
        // NodeAddressResolver 维护 node_id → host:port 的映射
        String addr = NodeAddressResolver.resolve(nodeId);
        if (addr != null && !addr.equals(nodeId) && addr.contains(":")) {
            return addr;
        }
        return null;
    }

    // ---- 差集计算 SQL 辅助 ----

    /**
     * 分页查询 file_metadata（防 OOM）。
     */
    private List<FileMetaRow> queryFileMetadataPage(int offset, int limit) throws SQLException {
        String sql = "SELECT file_hash, file_size, replication_factor FROM file_metadata" +
                " WHERE replication_factor > 1 ORDER BY file_hash LIMIT ? OFFSET ?";
        List<FileMetaRow> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, limit);
            stmt.setInt(2, offset);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    FileMetaRow row = new FileMetaRow();
                    row.fileHash = rs.getString("file_hash");
                    row.fileSize = rs.getLong("file_size");
                    row.replicationFactor = rs.getInt("replication_factor");
                    result.add(row);
                }
            }
        }
        return result;
    }

    /**
     * 统计 file_location 中该 hash 的 ACTIVE(status=1) 副本数。
     */
    private int countActiveReplicas(String fileHash) throws SQLException {
        String sql = "SELECT COUNT(*) FROM file_location WHERE file_hash = ? AND status = 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, fileHash);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    /**
     * 找 file_location 中该 hash 的源节点（用于对账拉取）。
     * <p>
     * 优先 PRIMARY(replica_role=0)；primary 副本丢失时 fallback 到任意 ACTIVE 副本节点。
     * <p>
     * 修复：原 findPrimaryNode 仅查 role=0，primary 副本丢失时返回 null 导致对账放弃补齐。
     * primary 丢失恰是最需要补齐的场景，应当用现存的 secondary 副本作为 source 拉取到缺失节点。
     */
    private String findSourceNode(String fileHash) throws SQLException {
        // 优先 PRIMARY
        String primarySql = "SELECT datanode_id FROM file_location WHERE file_hash = ? AND replica_role = 0 AND status = 1 LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(primarySql)) {
            stmt.setString(1, fileHash);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        // fallback：任意 ACTIVE 副本节点（primary 已丢失，用现有 secondary 作 source）
        String fallbackSql = "SELECT datanode_id FROM file_location WHERE file_hash = ? AND status = 1 LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(fallbackSql)) {
            stmt.setString(1, fileHash);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    /**
     * 找 file_location 中该 hash 的所有已持有节点（ACTIVE，不限 role）。
     */
    private List<String> findExistingNodes(String fileHash) throws SQLException {
        String sql = "SELECT datanode_id FROM file_location WHERE file_hash = ? AND status = 1";
        List<String> nodes = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, fileHash);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    nodes.add(rs.getString(1));
                }
            }
        }
        return nodes;
    }

    /**
     * 选 target 节点：组内除已持有节点外，优先未持有者。
     * <p>
     * 从 ReplicationGroupStore 获取 primary 所在组的成员，排除已持有节点，
     * 取前 missing 个。
     */
    private List<String> chooseTargets(String primaryNode, List<String> existingNodes, int missing) {
        if (replicationGroupStore == null) {
            return Collections.emptyList();
        }

        ReplicationGroup group = replicationGroupStore.getGroupByNodeId(primaryNode);
        if (group == null) {
            return Collections.emptyList();
        }

        Set<String> existing = new HashSet<>(existingNodes);
        List<String> targets = new ArrayList<>();
        for (String member : group.getNodeIds()) {
            if (!existing.contains(member)) {
                targets.add(member);
                if (targets.size() >= missing) {
                    break;
                }
            }
        }
        return targets;
    }

    /**
     * 孤儿文件告警（§10.3 + §7.2 一致性校验 MVP）：检测但仅记日志，不自动删除。
     * <p>
     * MVP 简化：全量扫描 file_location 与 DataNode 实际文件对比不在本阶段实现。
     * 此处仅做简单检测——file_location 有行但 datanode_id 为 NULL 的异常行。
     */
    private void logOrphanFiles() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM file_location WHERE datanode_id IS NULL")) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    LOG.warn("ReplicaSyncScheduler: 发现 {} 条 datanode_id 为 NULL 的 file_location 行（疑似孤儿/异常），需人工检查",
                            rs.getInt(1));
                }
            }
        } catch (SQLException e) {
            LOG.warn("ReplicaSyncScheduler: 孤儿文件检测失败", e);
        }
    }

    // ---- 内部类 ----

    /** file_metadata 行快照（差集计算用） */
    private static class FileMetaRow {
        String fileHash;
        long fileSize;
        int replicationFactor;
    }

    /** PULL_CMD ACK 等待 Handler */
    private static class SyncAckHandler extends SimpleChannelInboundHandler<Packet> {
        private final java.util.concurrent.BlockingQueue<Packet> queue = new java.util.concurrent.LinkedBlockingQueue<>();
        private volatile boolean channelClosed = false;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
            queue.offer(msg);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            channelClosed = true;
            if (queue.isEmpty()) {
                Packet err = new Packet();
                err.setCommandType(CommandType.ERROR);
                err.setData("连接已断开".getBytes(StandardCharsets.UTF_8));
                queue.offer(err);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            channelClosed = true;
            if (queue.isEmpty()) {
                Packet err = new Packet();
                err.setCommandType(CommandType.ERROR);
                err.setData(("连接异常: " + cause.getMessage()).getBytes(StandardCharsets.UTF_8));
                queue.offer(err);
            }
            ctx.close();
        }

        /**
         * 等待 ACK。非 ERROR 即视为成功（DataNode 接受 PULL_CMD 并启动 ReplicaPullWorker）。
         */
        boolean waitForAck(long timeout, TimeUnit unit) throws InterruptedException {
            Packet p = queue.poll(timeout, unit);
            if (p == null) {
                return false;
            }
            return p.getCommandType() != CommandType.ERROR;
        }
    }

    /**
     * 对账策略配置（MVP 硬编码默认值，Phase 6 Dashboard 可动态修改）。
     */
    public static class SyncPolicy {
        /** 核心窗口起始小时（24h 制） */
        public final int coreWindowStartHour;
        /** 核心窗口并发任务数 */
        public final int coreConcurrency;

        public static final SyncPolicy DEFAULT = new SyncPolicy(1, CORE_CONCURRENCY);

        public SyncPolicy(int coreWindowStartHour, int coreConcurrency) {
            this.coreWindowStartHour = coreWindowStartHour;
            this.coreConcurrency = coreConcurrency;
        }
    }
}
