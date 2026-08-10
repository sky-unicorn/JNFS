package org.jnfs.registry.api.dao;

import org.jnfs.common.NodeRegistryDdl;
import org.jnfs.common.migration.JdbcDialect;
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
 * 节点注册持久化 DAO（node_registry 表）。
 * <p>
 * Registry 启动时从本表加载历史节点到内存（显示为离线直到心跳刷新），并在每次
 * register/heartbeat 时同步 upsert，使节点列表跨 Registry 重启不丢失——既可展示在线，
 * 也可展示离线（离线判定走实时心跳缓存，落盘的 last_heartbeat 仅作历史展示）。
 * <p>
 * 表结构由迁移链（MysqlV0ToV1 + JdbcV5ToV6）/ NameNode 兜底建表 / Registry 启动自建
 * 共同保证（DDL 单一来源 {@code NodeRegistryDdl}）。本类只读写不建表。
 * <p>
 * SQL 方言：{@code INSERT ... ON DUPLICATE KEY UPDATE} 在 mysql 原生支持、H2 MariaDB
 * 模式亦支持（与 {@link NodeDrainDao#upsert} 同一用法，零分支）。
 */
public class NodeRegistryDao {

    private static final Logger LOG = LoggerFactory.getLogger(NodeRegistryDao.class);

    private final DataSource dataSource;

    public NodeRegistryDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * 确保 node_registry 表达到 V6 schema（含 free_space 列）。
     * <p>
     * 进程解耦：Registry 启动顺序早于 NameNode（start.sh 先启 registry），且单机 H2 共享
     * NameNode 的 H2 文件库时，旧部署的 node_registry 表可能仅 V5 schema（无 free_space）。
     * NameNode 的迁移链（JdbcV5ToV6）负责版本号推进 + 列补齐，但若 Registry 先启动而
     * NameNode 还没跑迁移，Registry 必须自己保证表结构可用，否则 listAll / upsert 即时报错。
     * <p>
     * 操作（幂等，INV-3）：
     * <ol>
     *   <li>CREATE TABLE IF NOT EXISTS（含 free_space，DDL 单一来源 NodeRegistryDdl）</li>
     *   <li>若表存在但缺 free_space → ALTER TABLE ADD COLUMN（与 NameNode 迁移链 V5→V6 同样的
     *       列名/类型/默认值/AFTER 位置，经 {@link JdbcDialect#columnExists} 判定跳过）</li>
     * </ol>
     * NameNode 后续启动跑迁移链时，columnExists(free_space)=true 自动跳过 ALTER，幂等无副作用。
     *
     * @param dataSource 共享数据源
     * @param dialect    方言（按 mode 取 H2Dialect / MysqlDialect）
     */
    public static void ensureSchema(DataSource dataSource, JdbcDialect dialect) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            // 1. 表不存在则建（含 free_space 的完整 V6 schema）
            conn.createStatement().executeUpdate(NodeRegistryDdl.createTableDdl());
            // 2. 表存在但缺 free_space（V5 遗留）→ 补列
            if (!dialect.columnExists(conn, "node_registry", "free_space")) {
                conn.createStatement().executeUpdate(
                        "ALTER TABLE `node_registry` "
                                + "ADD COLUMN `free_space` BIGINT NOT NULL DEFAULT 0 "
                                + "COMMENT 'DataNode剩余空间(字节); NameNode=0' "
                                + "AFTER `last_heartbeat`"
                );
                LOG.info("NodeRegistryDao: V5->V6 补列 node_registry.free_space（Registry 先启动场景）");
            }
        }
    }

    /**
     * Upsert 节点注册记录。
     * <p>
     * INSERT ... ON DUPLICATE KEY UPDATE：新节点插入，已存在节点刷新 host/port/free_space/last_heartbeat。
     * node_type 也随心跳更新（栅格缓解单机部署下 NameNode 与 DataNode 共享 APP_HOME 导致的
     * node_id 冲突——node_registry 反映最后心跳的角色，重启后 loadPersistedNodes 能按最新角色分类）。
     * 分布式部署下 node_id 全局唯一，node_type 恒为首次注册值，更新无副作用。
     *
     * @param nodeId              节点唯一标识
     * @param nodeType            DATANODE / NAMENODE
     * @param host                IP 地址
     * @param port                端口
     * @param freeSpace           剩余空间（字节；NameNode 传 0）
     * @param lastHeartbeatEpochMs 最后心跳时间（毫秒时间戳）
     */
    public void upsert(String nodeId, String nodeType, String host, int port,
                       long freeSpace, long lastHeartbeatEpochMs) throws SQLException {
        String sql = "INSERT INTO `node_registry` "
                + "(`node_id`, `node_type`, `host`, `port`, `free_space`, `last_heartbeat`) "
                + "VALUES (?, ?, ?, ?, ?, ?) "
                + "ON DUPLICATE KEY UPDATE "
                + "`node_type` = VALUES(`node_type`), "
                + "`host` = VALUES(`host`), "
                + "`port` = VALUES(`port`), "
                + "`free_space` = VALUES(`free_space`), "
                + "`last_heartbeat` = VALUES(`last_heartbeat`)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, nodeId);
            stmt.setString(2, nodeType);
            stmt.setString(3, host);
            stmt.setInt(4, port);
            stmt.setLong(5, freeSpace);
            stmt.setTimestamp(6, new Timestamp(lastHeartbeatEpochMs));
            stmt.executeUpdate();
        }
    }

    /**
     * 查询全部节点注册记录（Registry 启动时加载到内存）。
     *
     * @return NodeRecord 列表（离线节点也在内，状态待心跳刷新）
     */
    public List<NodeRecord> listAll() throws SQLException {
        String sql = "SELECT `node_id`, `node_type`, `host`, `port`, `free_space`, `last_heartbeat` "
                + "FROM `node_registry`";
        List<NodeRecord> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("last_heartbeat");
                result.add(new NodeRecord(
                        rs.getString("node_id"),
                        rs.getString("node_type"),
                        rs.getString("host"),
                        rs.getInt("port"),
                        rs.getLong("free_space"),
                        ts != null ? ts.getTime() : 0L
                ));
            }
        }
        return result;
    }

    /**
     * 节点注册记录值对象（不可变）。
     */
    public static final class NodeRecord {
        public final String nodeId;
        public final String nodeType;  // "DATANODE" / "NAMENODE"
        public final String host;
        public final int port;
        public final long freeSpace;
        public final long lastHeartbeatMs;

        public NodeRecord(String nodeId, String nodeType, String host, int port,
                          long freeSpace, long lastHeartbeatMs) {
            this.nodeId = nodeId;
            this.nodeType = nodeType;
            this.host = host;
            this.port = port;
            this.freeSpace = freeSpace;
            this.lastHeartbeatMs = lastHeartbeatMs;
        }
    }
}
