package org.jnfs.registry.api.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * 节点排空状态 DAO（node_drain 表，§6.1）。
 * <p>
 * 构造注入 {@link DataSource}（元数据库，与 ReplicationGroupDao 同源）。
 * 表结构由 NameNode 侧迁移框架保证（MysqlV3ToV4），本类只读写不建表。
 */
public class NodeDrainDao {

    private static final Logger LOG = LoggerFactory.getLogger(NodeDrainDao.class);

    private final DataSource dataSource;

    public NodeDrainDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 写入/更新节点排空状态。
     * <p>
     * drain=true → INSERT ... ON DUPLICATE KEY UPDATE drain_status=1, drain_at=NOW()
     * drain=false → UPDATE SET drain_status=0, drain_at=NULL WHERE node_id=?（命中 0 行无妨）
     *
     * @param drain  true=排空，false=恢复
     * @param nodeId 节点 ID
     */
    public void upsert(boolean drain, String nodeId) throws SQLException {
        if (drain) {
            String sql = "INSERT INTO node_drain (node_id, drain_status, drain_at) VALUES (?, 1, NOW()) "
                    + "ON DUPLICATE KEY UPDATE drain_status = 1, drain_at = NOW()";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, nodeId);
                stmt.executeUpdate();
            }
        } else {
            String sql = "UPDATE node_drain SET drain_status = 0, drain_at = NULL WHERE node_id = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, nodeId);
                stmt.executeUpdate();
            }
        }
    }

    /**
     * 查询所有 DRAINING 状态的节点。
     *
     * @return nodeId → drainAt（毫秒时间戳，null 安全）的映射
     */
    public Map<String, Long> listDraining() throws SQLException {
        String sql = "SELECT node_id, drain_at FROM node_drain WHERE drain_status = 1";
        Map<String, Long> result = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String nodeId = rs.getString("node_id");
                Timestamp drainAt = rs.getTimestamp("drain_at");
                result.put(nodeId, drainAt != null ? drainAt.getTime() : null);
            }
        }
        return result;
    }
}
