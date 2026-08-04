package org.jnfs.registry.api.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 对账控制信号 DAO（replication_control 单行，手动触发对账）。
 * <p>
 * Registry 写入 manual_sync_requested=1，NameNode 轮询检测后执行对账并清除。
 * 跨进程信号：Registry 进程写，NameNode 进程读+清。
 */
public class ReplicationControlDao {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationControlDao.class);

    private final DataSource dataSource;

    public ReplicationControlDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 请求手动对账（set manual_sync_requested=1, requested_at=NOW()） */
    public void requestManualSync() throws SQLException {
        String sql = "UPDATE replication_control SET manual_sync_requested = 1, requested_at = NOW() WHERE id = 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }

    /** 清除手动对账信号（NameNode 消费后调用） */
    public void clearManualSync() throws SQLException {
        String sql = "UPDATE replication_control SET manual_sync_requested = 0, requested_at = NULL WHERE id = 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }

    /** 查询是否已请求手动对账 */
    public boolean isManualSyncRequested() throws SQLException {
        String sql = "SELECT manual_sync_requested FROM replication_control WHERE id = 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             java.sql.ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1) == 1;
            }
        }
        return false;
    }
}
