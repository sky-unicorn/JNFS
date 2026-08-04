package org.jnfs.registry.api.dao;

import org.jnfs.common.replication.ReplicationGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 冗余组配置 DAO（replication_group 表，决策 9：Registry 读写元数据库）。
 * <p>
 * 构造注入 {@link DataSource}（元数据库，非 jnfs_registry 用户库）。
 * 表结构由 NameNode 侧 MySQLMetadataManager / 迁移框架保证，本类只读写不建表。
 * <p>
 * node_ids 逗号分隔 ↔ {@code List<String>} 转换。
 */
public class ReplicationGroupDao {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationGroupDao.class);

    private final DataSource dataSource;

    public ReplicationGroupDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** 查询全部冗余组 */
    public List<ReplicationGroup> listAll() throws SQLException {
        String sql = "SELECT group_id, group_name, node_ids, create_time, update_time FROM replication_group";
        List<ReplicationGroup> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                result.add(mapRow(rs));
            }
        }
        return result;
    }

    /** 按 groupId 查询，无则 null */
    public ReplicationGroup getById(String groupId) throws SQLException {
        String sql = "SELECT group_id, group_name, node_ids, create_time, update_time FROM replication_group WHERE group_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, groupId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? mapRow(rs) : null;
            }
        }
    }

    /** 插入新组 */
    public void insert(ReplicationGroup group) throws SQLException {
        String sql = "INSERT INTO replication_group (group_id, group_name, node_ids) VALUES (?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, group.getGroupId());
            stmt.setString(2, group.getGroupName() == null ? "" : group.getGroupName());
            stmt.setString(3, String.join(",", group.getNodeIds()));
            stmt.executeUpdate();
        }
    }

    /** 更新组的 node_ids（group_name 一并更新） */
    public void update(String groupId, List<String> nodeIds, String groupName) throws SQLException {
        String sql = "UPDATE replication_group SET node_ids = ?, group_name = ? WHERE group_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, String.join(",", nodeIds));
            stmt.setString(2, groupName == null ? "" : groupName);
            stmt.setString(3, groupId);
            stmt.executeUpdate();
        }
    }

    /** 删除组，返回是否删除成功 */
    public boolean delete(String groupId) throws SQLException {
        String sql = "DELETE FROM replication_group WHERE group_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, groupId);
            return stmt.executeUpdate() > 0;
        }
    }

    /**
     * 查询所有组中已使用的 node_id（用于校验组间不重叠）。
     */
    public List<String> listAllUsedNodeIds() throws SQLException {
        String sql = "SELECT node_ids FROM replication_group";
        List<String> used = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                used.addAll(parseNodeIds(rs.getString("node_ids")));
            }
        }
        return used;
    }

    private ReplicationGroup mapRow(ResultSet rs) throws SQLException {
        ReplicationGroup g = new ReplicationGroup();
        g.setGroupId(rs.getString("group_id"));
        g.setGroupName(rs.getString("group_name"));
        g.setNodeIds(parseNodeIds(rs.getString("node_ids")));
        Timestamp ct = rs.getTimestamp("create_time");
        if (ct != null) g.setCreateTime(ct.getTime());
        Timestamp ut = rs.getTimestamp("update_time");
        if (ut != null) g.setUpdateTime(ut.getTime());
        return g;
    }

    /** 解析 node_ids 字段（逗号分隔）为 List */
    private List<String> parseNodeIds(String nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        for (String p : nodeIds.split(",")) {
            String t = p.trim();
            if (!t.isEmpty()) {
                result.add(t);
            }
        }
        return result;
    }
}
