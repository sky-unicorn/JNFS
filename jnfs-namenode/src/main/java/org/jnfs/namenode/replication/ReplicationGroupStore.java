package org.jnfs.namenode.replication;

import org.jnfs.common.DaemonThreadFactory;
import org.jnfs.common.replication.ReplicationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 冗余组配置缓存（JDBC 模式 mysql/h2 共用）。
 * <p>
 * 设计文档 §9.3：冗余组配置持久化到 {@code replication_group} 表，不依赖 Registry 内存。
 * NameNode 启动时同步加载一次，并通过后台守护线程定期 refresh（默认 30s），保证 Dashboard 改组后 NameNode 能感知。
 * <p>
 * 线程安全：groups 字段 volatile，refresh 整体替换引用；读取方拿快照。
 * file 模式已退役；未构造本类（{@code replicationGroupStore == null}）即降级单副本。
 */
public class ReplicationGroupStore {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationGroupStore.class);

    /** 默认刷新间隔（秒） */
    private static final long DEFAULT_REFRESH_INTERVAL_SECONDS = 30;

    private final DataSource dataSource;
    private final long refreshIntervalSeconds;
    private final ScheduledExecutorService scheduler;

    /** 缓存的冗余组列表（volatile 替换，读取方拿快照） */
    private volatile List<ReplicationGroup> groups = Collections.emptyList();

    public ReplicationGroupStore(DataSource dataSource) {
        this(dataSource, DEFAULT_REFRESH_INTERVAL_SECONDS);
    }

    /**
     * @param dataSource            元数据库数据源（来自 JdbcMetadataManager.getDataSource()）
     * @param refreshIntervalSeconds 定时刷新间隔（秒）
     */
    public ReplicationGroupStore(DataSource dataSource, long refreshIntervalSeconds) {
        this.dataSource = dataSource;
        this.refreshIntervalSeconds = refreshIntervalSeconds;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("ReplicationGroupStore-Refresh"));
    }

    /**
     * 启动：先同步 refresh 一次（加载缓存），再注册定时刷新。
     */
    public void start() {
        try {
            refresh();
        } catch (Exception e) {
            LOG.error("ReplicationGroupStore 初始加载失败，先以空组运行，等待下次定时刷新", e);
        }
        scheduler.scheduleAtFixedRate(() -> {
            try {
                refresh();
            } catch (Exception e) {
                LOG.error("ReplicationGroupStore 定时刷新失败", e);
            }
        }, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
        LOG.info("ReplicationGroupStore 已启动，刷新间隔 {}s", refreshIntervalSeconds);
    }

    /**
     * 从 mysql 全量加载冗余组配置，volatile 替换缓存。
     * 空表/无组：缓存置为空列表。
     */
    public void refresh() throws SQLException {
        String sql = "SELECT group_id, group_name, node_ids FROM replication_group";
        List<ReplicationGroup> loaded = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                ReplicationGroup g = new ReplicationGroup();
                g.setGroupId(rs.getString("group_id"));
                g.setGroupName(rs.getString("group_name"));
                g.setNodeIds(parseNodeIds(rs.getString("node_ids")));
                loaded.add(g);
            }
        }
        this.groups = Collections.unmodifiableList(loaded);
        LOG.info("ReplicationGroupStore 加载 {} 个冗余组", loaded.size());
    }

    /**
     * 返回 nodeId 所属的首个冗余组，无则 null。
     */
    public ReplicationGroup getGroupByNodeId(String nodeId) {
        if (nodeId == null) {
            return null;
        }
        for (ReplicationGroup g : groups) {
            for (String n : g.getNodeIds()) {
                if (nodeId.equals(n)) {
                    return g;
                }
            }
        }
        return null;
    }

    /**
     * 返回全部冗余组快照（不可变）。
     */
    public List<ReplicationGroup> getAllGroups() {
        return groups;
    }

    /**
     * 关闭刷新调度器。
     */
    public void shutdown() {
        scheduler.shutdownNow();
        LOG.info("ReplicationGroupStore 已关闭");
    }

    /**
     * 解析 node_ids 字段（逗号分隔）为 List。null/空串 → 空列表。
     */
    private List<String> parseNodeIds(String nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = nodeIds.split(",");
        List<String> result = new ArrayList<>(parts.length);
        for (String p : parts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }
}
