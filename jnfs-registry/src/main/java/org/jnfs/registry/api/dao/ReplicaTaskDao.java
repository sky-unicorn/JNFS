package org.jnfs.registry.api.dao;

import org.jnfs.common.replication.ReplicaSyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 对账任务 DAO（replica_sync_task 读侧 + 决策 11 resetRetryCount，决策 9）。
 * <p>
 * Registry 进程只读 replica_sync_task 表（NameNode 写入）。
 * resetRetryCount（决策 11：手动重试重置计数器）在 Registry 侧独立实现同样的 SQL，
 * 因为 NameNode 的 ReplicaSyncTaskStore 是跨进程实例，Registry 无法共享。
 */
public class ReplicaTaskDao {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaTaskDao.class);

    /** §7.8 告警阈值：retry_count >= 4 */
    public static final int ALERT_RETRY_THRESHOLD = 4;

    private final DataSource dataSource;

    public ReplicaTaskDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 同步进度汇总（§16.7） */
    public static class Summary {
        public long totalPending;     // status IN (PENDING, IN_FLIGHT)
        public long syncedToday;      // S1 修复：status=DONE AND update_time >= CURDATE()（完成时间，跨午夜任务正确计入完成日）
        public long failed;           // retry_count >= 4
        public long currentJobs;      // status=IN_FLIGHT
    }

    /** 同步进度汇总 */
    public Summary summary() throws SQLException {
        String sql = "SELECT" +
                " SUM(CASE WHEN status IN (0,1) THEN 1 ELSE 0 END) AS total_pending," +
                " SUM(CASE WHEN status = 2 AND update_time >= CURDATE() THEN 1 ELSE 0 END) AS synced_today," +
                " SUM(CASE WHEN retry_count >= ? THEN 1 ELSE 0 END) AS failed," +
                " SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS current_jobs" +
                " FROM replica_sync_task";
        Summary s = new Summary();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, ALERT_RETRY_THRESHOLD);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    s.totalPending = rs.getLong("total_pending");
                    s.syncedToday = rs.getLong("synced_today");
                    s.failed = rs.getLong("failed");
                    s.currentJobs = rs.getLong("current_jobs");
                }
            }
        }
        return s;
    }

    /** 查询失败任务（status IN PENDING/IN_FLIGHT 且 retry_count>0，排除已告警的 >=4） */
    public List<ReplicaSyncTask> listFailed() throws SQLException {
        String sql = "SELECT task_id, file_hash, source_node, target_node, status, retry_count, file_size," +
                " create_time, update_time FROM replica_sync_task" +
                " WHERE retry_count > 0 AND retry_count < ? AND status IN (0,1)" +
                " ORDER BY update_time DESC LIMIT 100";
        return queryList(sql, ALERT_RETRY_THRESHOLD);
    }

    /** 查询告警任务（retry_count >= 4） */
    public List<ReplicaSyncTask> listAlerts() throws SQLException {
        String sql = "SELECT task_id, file_hash, source_node, target_node, status, retry_count, file_size," +
                " create_time, update_time FROM replica_sync_task WHERE retry_count >= ?" +
                " ORDER BY update_time DESC LIMIT 100";
        return queryList(sql, ALERT_RETRY_THRESHOLD);
    }

    /**
     * 决策 11：重置 retry_count 为 0，回 PENDING 重新派发（运维介入后重新开始 4 次窗口）。
     */
    public boolean resetRetryCount(String taskId) throws SQLException {
        String sql = "UPDATE replica_sync_task SET retry_count = 0, status = 0, update_time = NOW() WHERE task_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, taskId);
            return stmt.executeUpdate() > 0;
        }
    }

    private List<ReplicaSyncTask> queryList(String sql, int threshold) throws SQLException {
        List<ReplicaSyncTask> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, threshold);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        }
        return result;
    }

    private ReplicaSyncTask mapRow(ResultSet rs) throws SQLException {
        ReplicaSyncTask t = new ReplicaSyncTask();
        t.setTaskId(rs.getString("task_id"));
        t.setFileHash(rs.getString("file_hash"));
        t.setSourceNode(rs.getString("source_node"));
        t.setTargetNode(rs.getString("target_node"));
        t.setStatus(rs.getInt("status"));
        t.setRetryCount(rs.getInt("retry_count"));
        t.setFileSize(rs.getLong("file_size"));
        Timestamp ct = rs.getTimestamp("create_time");
        if (ct != null) t.setCreateTime(ct.getTime());
        Timestamp ut = rs.getTimestamp("update_time");
        if (ut != null) t.setUpdateTime(ut.getTime());
        return t;
    }
}
