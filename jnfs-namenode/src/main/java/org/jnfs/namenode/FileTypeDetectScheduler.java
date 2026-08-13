package org.jnfs.namenode;

import cn.hutool.cache.CacheUtil;
import cn.hutool.cache.impl.TimedCache;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.jnfs.common.CommandType;
import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.FileTypeDetector;
import org.jnfs.common.NettyClientBootstrap;
import org.jnfs.common.NodeAddressResolver;
import org.jnfs.common.Packet;
import org.jnfs.common.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 后台文件类型嗅探 + 文件大小回填调度器（JDBC 模式：mysql / h2 共用）。
 * <p>
 * <b>定位</b>：完全独立于上传/下载主链路。上传提交时类型已按扩展名即时落库
 * （{@link FileTypeDetector#fromFilename}），本调度器仅兜底两类缺口：
 * <ul>
 *   <li>{@code file_type IS NULL}：无扩展名 / 扩展名不可靠的文件，向持有副本的 DataNode
 *       读文件<b>解密后</b>的头部 ≤8KB（{@link CommandType#DATA_HEAD_READ_REQUEST}），
 *       Tika 内容嗅探后回写类型；</li>
 *   <li>{@code file_size IS NULL}：旧数据从未写入大小（迁移 V7 已把历史 0 归一为 NULL），
 *       用 DataNode 返回的逻辑长度回填。</li>
 * </ul>
 * <p>
 * <b>节流</b>：单 daemon 线程，每批 20 行、10s 一轮（连续空批退避至 60s），
 * 每文件仅读 8KB —— 对存储/下载链路零影响。
 * <p>
 * <b>出队保证</b>：处理成功（Tika 无可识别类型时回退扩展名、再回退 {@value #UNKNOWN_TYPE}）
 * 即不再匹配队列条件，避免同一批行空转；连接失败的行进入 10 分钟跳过集，下轮重试。
 */
public class FileTypeDetectScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(FileTypeDetectScheduler.class);

    /** 无可识别类型时的兜底标签（写入后即出队，展示层显示"未知"） */
    static final String UNKNOWN_TYPE = "unknown";

    /** 每批处理行数 */
    private static final int BATCH_SIZE = 20;
    /** 常规轮询间隔（秒） */
    private static final long INTERVAL_SECONDS = 10;
    /** 连续空批后退避的最大跳过轮数（10s × 6 = 60s 等效） */
    private static final int IDLE_SKIP_CYCLES = 6;
    /** 首次启动延迟（秒）：等待 discovery 建立 node_id → host:port 映射 */
    private static final long INITIAL_DELAY_SECONDS = 30;
    /** 失败跳过集 TTL（毫秒）：连接失败的行 10 分钟内不重试，防死节点空转 */
    private static final long FAIL_SKIP_TTL_MS = 10 * 60 * 1000;
    /** 连接 DataNode 超时（毫秒） */
    private static final int CONNECT_TIMEOUT_MS = 5000;
    /** 等待头部响应超时（秒） */
    private static final long HEAD_WAIT_TIMEOUT_SECONDS = 5;

    private final DataSource dataSource;
    /** 出站连接 EventLoopGroup：生命周期由本调度器接管（shutdown 时优雅关闭） */
    private final EventLoopGroup workerGroup;

    /** Tika 检测器（线程安全，单实例复用） */
    private final Detector detector = new DefaultDetector();

    /** 连接失败跳过集：hash → 占位（10 分钟 TTL，定时清理） */
    private final TimedCache<String, Boolean> failSkip = CacheUtil.newTimedCache(FAIL_SKIP_TTL_MS);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private ScheduledExecutorService scheduler;
    /** 空闲退避：剩余跳过轮数（>0 时本轮直接跳过） */
    private int skipCycles = 0;

    public FileTypeDetectScheduler(DataSource dataSource, EventLoopGroup workerGroup) {
        this.dataSource = dataSource;
        this.workerGroup = workerGroup;
        failSkip.schedulePrune(60 * 1000);
    }

    public void start() {
        scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("NameNode-FileTypeDetect"));
        scheduler.scheduleWithFixedDelay(this::runOneBatch,
                INITIAL_DELAY_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOG.info("文件类型嗅探调度器已启动（每 {}s 一批 {} 行，空闲退避）", INTERVAL_SECONDS, BATCH_SIZE);
    }

    public void shutdown() {
        running.set(false);
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        // workerGroup 由本调度器独占并接管生命周期（Netty 线程非 daemon，必须显式关闭）
        workerGroup.shutdownGracefully();
        LOG.info("文件类型嗅探调度器已关闭");
    }

    /** 每轮处理：查批 → 逐行处理。任何异常仅记日志，不中断调度循环。 */
    private void runOneBatch() {
        if (!running.get()) {
            return;
        }
        if (skipCycles > 0) {
            skipCycles--;
            return;
        }
        List<Row> rows = queryBatch();
        if (rows.isEmpty()) {
            skipCycles = IDLE_SKIP_CYCLES; // 队列清空，退避 60s
            return;
        }
        skipCycles = 0;
        for (Row row : rows) {
            try {
                processRow(row);
            } catch (Exception e) {
                LOG.debug("文件类型嗅探处理失败: hash={}", row.hash, e);
            }
        }
    }

    /** 队列查询：类型未知或大小未知的行（最旧优先，保证存量逐步消化） */
    private List<Row> queryBatch() {
        List<Row> rows = new ArrayList<>();
        String sql = "SELECT storage_id, file_hash, filename, file_type, file_size "
                + "FROM file_metadata WHERE file_type IS NULL OR file_size IS NULL "
                + "ORDER BY create_time ASC LIMIT " + BATCH_SIZE;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                long size = rs.getLong("file_size");
                rows.add(new Row(
                        rs.getString("storage_id"),
                        rs.getString("file_hash"),
                        rs.getString("filename"),
                        rs.getString("file_type"),
                        rs.wasNull() ? null : size));
            }
        } catch (SQLException e) {
            LOG.warn("查询类型嗅探队列失败，本轮跳过", e);
        }
        return rows;
    }

    private void processRow(Row row) {
        if (failSkip.containsKey(row.hash)) {
            return; // 失败跳过集内，等待 TTL 后重试
        }

        // 1. 找首个 ACTIVE 副本地址（primary 优先）
        String addr = queryReplicaAddress(row.hash);
        if (addr == null) {
            LOG.debug("文件 {} 无可用副本地址（可能节点离线），10 分钟后重试", row.hash);
            failSkip.put(row.hash, true);
            return;
        }

        // 2. 一次性连接 DataNode 读头（仿 ReplicaSyncScheduler 模式）
        HeadReadHandler handler = new HeadReadHandler();
        Bootstrap b = NettyClientBootstrap.createWithHandler(workerGroup, CONNECT_TIMEOUT_MS, handler);
        try {
            Channel channel = NettyClientBootstrap.connectSync(b,
                    addr.split(":")[0], Integer.parseInt(addr.split(":")[1]), 6000);
            try {
                Packet request = new Packet();
                request.setCommandType(CommandType.DATA_HEAD_READ_REQUEST);
                request.setToken(SecurityConfig.getToken());
                request.setData(row.hash.getBytes(StandardCharsets.UTF_8));
                channel.writeAndFlush(request);

                Packet resp = handler.waitResponse(HEAD_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (resp == null || resp.getCommandType() != CommandType.DATA_HEAD_READ_RESPONSE
                        || resp.getData() == null || resp.getData().length < 8) {
                    LOG.debug("头部读取失败/超时: hash={}, addr={}", row.hash, addr);
                    failSkip.put(row.hash, true);
                    return;
                }
                applyHeadRead(row, resp.getData());
            } finally {
                channel.close().sync();
            }
        } catch (Exception e) {
            LOG.debug("连接 DataNode 读头失败: hash={}, addr={}: {}", row.hash, addr, e.getMessage());
            failSkip.put(row.hash, true);
        }
    }

    /**
     * 解析头部响应并回写。
     * <p>
     * 类型策略：仅当行内 file_type 为 NULL 时才做 Tika 嗅探（不覆盖已按扩展名落库的标签）：
     * Tika MIME 可映射 → 用之；否则回退扩展名；再回退 {@value #UNKNOWN_TYPE}（保证出队）。
     * 大小策略：DataNode 返回的逻辑长度恒写入（空文件写回 0，同样出队）。
     */
    private void applyHeadRead(Row row, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        long logicalLength = buf.getLong();
        if (logicalLength < 0) {
            logicalLength = 0;
        }
        byte[] plainHead = new byte[buf.remaining()];
        buf.get(plainHead);

        String newType = null;
        if (row.fileType == null) {
            if (plainHead.length > 0) {
                Metadata meta = new Metadata();
                meta.set(TikaCoreProperties.RESOURCE_NAME_KEY, row.filename);
                try {
                    org.apache.tika.mime.MediaType mediaType =
                            detector.detect(TikaInputStream.get(plainHead), meta);
                    if (mediaType != null) {
                        newType = FileTypeDetector.fromMime(mediaType.toString());
                    }
                } catch (Exception e) {
                    LOG.debug("Tika 检测失败: hash={}", row.hash, e);
                }
            }
            if (newType == null) {
                newType = FileTypeDetector.fromFilename(row.filename);
            }
            if (newType == null) {
                newType = UNKNOWN_TYPE;
            }
        }

        // COALESCE(NULLIF(?,''), file_type)：newType 为 null（已有类型的行只修大小）时保留原值
        String sql = "UPDATE file_metadata SET "
                + "file_type = COALESCE(NULLIF(?,''), file_type), file_size = ? WHERE file_hash = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, newType);
            stmt.setLong(2, logicalLength);
            stmt.setString(3, row.hash);
            int updated = stmt.executeUpdate();
            if (updated > 0) {
                LOG.debug("嗅探回写: hash={}, type={}, size={}", row.hash, newType, logicalLength);
            }
        } catch (SQLException e) {
            LOG.warn("嗅探结果回写失败: hash={}", row.hash, e);
            failSkip.put(row.hash, true);
        }
    }

    /**
     * 查该 hash 的首个 ACTIVE 副本地址（primary 优先）。
     * 返回可连接的 host:port；datanode_id（UUID）经 NodeAddressResolver 解析，
     * 解析失败回退 datanode_addr（host:port 兼容旧数据）；均不可用返回 null。
     */
    private String queryReplicaAddress(String hash) {
        String sql = "SELECT datanode_id, datanode_addr FROM file_location "
                + "WHERE file_hash = ? AND status = 1 ORDER BY replica_role ASC LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                String datanodeId = rs.getString("datanode_id");
                String datanodeAddr = rs.getString("datanode_addr");
                String addr = (datanodeId != null) ? NodeAddressResolver.resolve(datanodeId) : null;
                if (addr != null && NodeAddressResolver.isHostPort(addr)) {
                    return addr;
                }
                if (datanodeAddr != null && NodeAddressResolver.isHostPort(datanodeAddr)) {
                    return datanodeAddr;
                }
                return null;
            }
        } catch (SQLException e) {
            LOG.warn("查询副本地址失败: hash={}", hash, e);
            return null;
        }
    }

    /** 队列行值对象 */
    private static final class Row {
        final String storageId;
        final String hash;
        final String filename;
        final String fileType;
        final Long fileSize;

        Row(String storageId, String hash, String filename, String fileType, Long fileSize) {
            this.storageId = storageId;
            this.hash = hash;
            this.filename = filename;
            this.fileType = fileType;
            this.fileSize = fileSize;
        }
    }

    /**
     * 头部响应 Handler：CompletableFuture 同步等待（一次一连接，无并发问题）。
     * channelInactive 时补发 null 唤醒等待线程，防止连接被对端关闭后永久阻塞。
     */
    private static class HeadReadHandler extends SimpleChannelInboundHandler<Packet> {

        private final CompletableFuture<Packet> promise = new CompletableFuture<>();

        Packet waitResponse(long timeout, TimeUnit unit) {
            try {
                return promise.get(timeout, unit);
            } catch (Exception e) {
                promise.cancel(true);
                return null;
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet packet) {
            if (packet.getCommandType() == CommandType.DATA_HEAD_READ_RESPONSE
                    || packet.getCommandType() == CommandType.ERROR) {
                promise.complete(packet);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            promise.complete(null);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            promise.completeExceptionally(cause);
            ctx.close();
        }
    }
}
