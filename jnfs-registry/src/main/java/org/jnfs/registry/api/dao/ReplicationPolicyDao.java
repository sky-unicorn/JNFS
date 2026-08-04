package org.jnfs.registry.api.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 同步策略配置 DAO（replication_policy 单行，决策 9）。
 * <p>
 * 单行设计（id=1），GET/PUT 操作固定读写该行。
 */
public class ReplicationPolicyDao {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationPolicyDao.class);

    private final DataSource dataSource;

    public ReplicationPolicyDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 策略值对象（所有字段 public，JSON 序列化用） */
    public static class Policy {
        public String syncWindowStart;
        public String syncWindowEnd;
        public String softDeadline;
        public int rateLimitMbps;
        public int maxConcurrency;
    }

    /** 读取策略（id=1） */
    public Policy get() throws SQLException {
        String sql = "SELECT sync_window_start, sync_window_end, soft_deadline, rate_limit_mbps, max_concurrency" +
                " FROM replication_policy WHERE id = 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                Policy p = new Policy();
                p.syncWindowStart = rs.getString("sync_window_start");
                p.syncWindowEnd = rs.getString("sync_window_end");
                p.softDeadline = rs.getString("soft_deadline");
                p.rateLimitMbps = rs.getInt("rate_limit_mbps");
                p.maxConcurrency = rs.getInt("max_concurrency");
                return p;
            }
        }
        // 不存在时返回默认值
        Policy p = new Policy();
        p.syncWindowStart = "01:00";
        p.syncWindowEnd = "03:00";
        p.softDeadline = "03:00";
        p.rateLimitMbps = 50;
        p.maxConcurrency = 4;
        return p;
    }

    /** 更新策略（id=1） */
    public void update(Policy p) throws SQLException {
        String sql = "UPDATE replication_policy SET sync_window_start=?, sync_window_end=?, soft_deadline=?," +
                " rate_limit_mbps=?, max_concurrency=? WHERE id = 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, p.syncWindowStart);
            stmt.setString(2, p.syncWindowEnd);
            stmt.setString(3, p.softDeadline);
            stmt.setInt(4, p.rateLimitMbps);
            stmt.setInt(5, p.maxConcurrency);
            stmt.executeUpdate();
        }
    }
}
