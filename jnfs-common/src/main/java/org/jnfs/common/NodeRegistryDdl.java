package org.jnfs.common;

/**
 * node_registry 表 DDL（单一来源）。
 * <p>
 * 被以下三处共同引用，保证 fresh 部署、迁移升级与 Registry 侧自建产出完全一致的 schema，
 * 避免重复内联 DDL 导致 schema 漂移：
 * <ul>
 *   <li>NameNode 迁移链 {@code MysqlV0ToV1#createNodeRegistryIfNotExists}</li>
 *   <li>NameNode 兜底建表 {@code MySQLMetadataManager#ensureNonAnchorTables}</li>
 *   <li>Registry 启动自建（进程解耦：registry 先于 namenode 启动时，node_registry 可能尚不存在）</li>
 * </ul>
 * 与 {@code mysql/jnfs.sql} 终态保持一致。
 */
public final class NodeRegistryDdl {

    private NodeRegistryDdl() {
        // 工具类，禁止实例化
    }

    /**
     * node_registry 完整建表 DDL（含 free_space 列，V6 新增）。
     * <p>
     * CREATE TABLE IF NOT EXISTS 天然幂等（INV-3）。
     * H2 MariaDB 模式零分支兼容（ENGINE/CHARSET/反引号/KEY/COMMENT），与锚点表同理。
     *
     * @return 建表 DDL 语句
     */
    public static String createTableDdl() {
        return "CREATE TABLE IF NOT EXISTS `node_registry` ("
                + "`node_id` VARCHAR(128) NOT NULL COMMENT '节点唯一标识', "
                + "`node_type` VARCHAR(20) NOT NULL COMMENT '节点类型: DATANODE / NAMENODE', "
                + "`host` VARCHAR(100) NOT NULL COMMENT '节点IP地址', "
                + "`port` INT NOT NULL COMMENT '节点端口', "
                + "`last_heartbeat` DATETIME NOT NULL COMMENT '最后心跳时间', "
                + "`free_space` BIGINT NOT NULL DEFAULT 0 COMMENT 'DataNode剩余空间(字节); NameNode=0', "
                + "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP, "
                + "PRIMARY KEY (`node_id`), "
                + "KEY `idx_type` (`node_type`)"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='节点注册表'";
    }
}
