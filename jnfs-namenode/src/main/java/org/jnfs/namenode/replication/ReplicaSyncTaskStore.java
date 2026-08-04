package org.jnfs.namenode.replication;

import org.jnfs.common.replication.ReplicaSyncTask;
import org.jnfs.common.replication.SyncTaskStatus;
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
 * 对账同步任务存储（mysql 模式专用，决策 10：任务持久化解决 I6）。
 * <p>
 * 读写 {@code replica_sync_task} 表。基于 {@link javax.sql.DataSource}（连接池保证线程安全），
 * 每个方法独立事务（DML 事务有效，非 DDL 隐式提交场景）。
 * <p>
 * 状态流转（§7.7 + §7.8）：
 * <ul>
 *   <li>{@link #upsertPending}：插入新任务（PENDING, retry=0），{@code INSERT ... ON DUPLICATE KEY UPDATE}
 *       保证同一 (file_hash, target_node) 重复入队不新增行（uk_hash_target 去重）</li>
 *   <li>{@link #markInFlight}：PENDING→IN_FLIGHT（CAS 式 UPDATE WHERE status=PENDING）</li>
 *   <li>{@link #markDone}：→DONE</li>
 *   <li>{@link #markFailed}：回 PENDING 且 retry_count++（原子 UPDATE）</li>
 * </ul>
 * file 模式不构造本类（单副本无对账需求）。
 */
public class ReplicaSyncTaskStore {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicaSyncTaskStore.class);

    /** §7.8 告警阈值：连续失败 4 次进入告警列表，不再自动派发 */
    public static final int ALERT_RETRY_THRESHOLD = 4;

    private final DataSource dataSource;

    public ReplicaSyncTaskStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 插入新任务（status=PENDING, retry_count=0）。
     * <p>
     * 使用 {@code INSERT ... ON DUPLICATE KEY UPDATE}（uk_hash_target 去重）：
     * 同一 (file_hash, target_node) 重复入队不新增行，仅保留首条。
     * <p>
     * 注意：ON DUPLICATE KEY 触发时不更新已有行的 status/retry_count——
     * 已存在任务保持原状态（避免覆盖 IN_FLIGHT/DONE 的进度）。
     *
     * @param task 待入队任务（taskId/fileHash/sourceNode/targetNode/fileSize 必填）
     */
    public void upsertPending(ReplicaSyncTask task) throws SQLException {
        // ON DUPLICATE KEY UPDATE 仅刷新 update_time，不动 status/retry_count，
        // 保证重复入队幂等且不覆盖在途/已完成任务的进度。
        String sql = "INSERT INTO replica_sync_task" +
                " (task_id, file_hash, source_node, target_node, status, retry_count, file_size, create_time, update_time)" +
                " VALUES (?, ?, ?, ?, ?, 0, ?, NOW(), NOW())" +
                " ON DUPLICATE KEY UPDATE update_time = NOW()";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, task.getTaskId());
            stmt.setString(2, task.getFileHash());
            stmt.setString(3, task.getSourceNode());
            stmt.setString(4, task.getTargetNode());
            stmt.setInt(5, SyncTaskStatus.PENDING.getCode());
            stmt.setLong(6, task.getFileSize());
            stmt.executeUpdate();
        }
    }

    /**
     * 查询待处理任务（status IN PENDING/IN_FLIGHT），启动恢复用。
     */
    public List<ReplicaSyncTask> findPending() throws SQLException {
        String sql = "SELECT task_id, file_hash, source_node, target_node, status, retry_count, file_size," +
                " create_time, update_time FROM replica_sync_task" +
                " WHERE status IN (?, ?)";
        List<ReplicaSyncTask> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.PENDING.getCode());
            stmt.setInt(2, SyncTaskStatus.IN_FLIGHT.getCode());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        }
        return result;
    }

    /**
     * PENDING→IN_FLIGHT（CAS 式）：仅当当前 status=PENDING 才更新成功。
     *
     * @return true=抢占成功，false=已被其他线程抢占或状态已变
     */
    public boolean markInFlight(String taskId) throws SQLException {
        String sql = "UPDATE replica_sync_task SET status = ?, update_time = NOW()" +
                " WHERE task_id = ? AND status = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.IN_FLIGHT.getCode());
            stmt.setString(2, taskId);
            stmt.setInt(3, SyncTaskStatus.PENDING.getCode());
            return stmt.executeUpdate() > 0;
        }
    }

    /**
     * 标记完成（DONE）。副本 COMMIT 登记成功后调用。
     */
    public void markDone(String taskId) throws SQLException {
        String sql = "UPDATE replica_sync_task SET status = ?, update_time = NOW() WHERE task_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.DONE.getCode());
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    /**
     * 标记失败：status 回 PENDING + retry_count = retry_count + 1（原子 UPDATE）。
     * <p>
     * §7.8：失败后下一轮对账重新派发；retry_count 累计达 4 进告警列表。
     */
    public void markFailed(String taskId) throws SQLException {
        String sql = "UPDATE replica_sync_task SET status = ?, retry_count = retry_count + 1, update_time = NOW()" +
                " WHERE task_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.PENDING.getCode());
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    /**
     * 重置 retry_count 为 0（决策 11：Dashboard 手动重试视为运维介入，重新开始 4 次窗口）。
     * 同时回 PENDING 以便重新派发。
     */
    public void resetRetryCount(String taskId) throws SQLException {
        String sql = "UPDATE replica_sync_task SET retry_count = 0, status = ?, update_time = NOW()" +
                " WHERE task_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.PENDING.getCode());
            stmt.setString(2, taskId);
            stmt.executeUpdate();
        }
    }

    /**
     * 查询告警任务（retry_count >= {@value #ALERT_RETRY_THRESHOLD}，§7.8）。
     * 这些任务不再自动派发，等运维手动重试（resetRetryCount）。
     */
    public List<ReplicaSyncTask> findAlerts() throws SQLException {
        String sql = "SELECT task_id, file_hash, source_node, target_node, status, retry_count, file_size," +
                " create_time, update_time FROM replica_sync_task WHERE retry_count >= ?";
        List<ReplicaSyncTask> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, ALERT_RETRY_THRESHOLD);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        }
        return result;
    }

    /**
     * 查询 stale-IN_FLIGHT 任务（I2 stale reaper）。
     * <p>
     * update_time 早于 NOW() - INTERVAL minutes MINUTE 且仍 IN_FLIGHT 的任务
     * 表示拉取失败/COMMIT 丢失后 NameNode 侧任务永久挂起，需回收自愈。
     * <p>
     * 阈值 30 分钟远超单文件拉取时间（ReplicaPullWorker HARD_TIMEOUT 10min + 20min 余量），
     * 不会误伤正常进行中的任务。
     *
     * @param minutes 超时分钟数
     * @return stale IN_FLIGHT 任务列表
     */
    public List<ReplicaSyncTask> findStaleInFlight(int minutes) throws SQLException {
        String sql = "SELECT task_id, file_hash, source_node, target_node, status, retry_count, file_size," +
                " create_time, update_time FROM replica_sync_task" +
                " WHERE status = ? AND update_time < NOW() - INTERVAL ? MINUTE";
        List<ReplicaSyncTask> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.IN_FLIGHT.getCode());
            stmt.setInt(2, minutes);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    result.add(mapRow(rs));
                }
            }
        }
        return result;
    }

    /**
     * 将 IN_FLIGHT 任务批量回退为 PENDING（NameNode 启动恢复用，决策 10）。
     * 崩溃前 IN_FLIGHT 的任务视为中断，重新派发。
     *
     * @return 回退的行数
     */
    public int resetInFlightToPending() throws SQLException {
        String sql = "UPDATE replica_sync_task SET status = ?, update_time = NOW() WHERE status = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, SyncTaskStatus.PENDING.getCode());
            stmt.setInt(2, SyncTaskStatus.IN_FLIGHT.getCode());
            return stmt.executeUpdate();
        }
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
