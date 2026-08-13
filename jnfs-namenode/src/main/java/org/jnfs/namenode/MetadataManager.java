package org.jnfs.namenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 元数据管理器抽象基类
 * <p>
 * file 模式（WAL 日志）已退役，运行时不再存在非 JDBC 实例。本类仅保留契约方法签名，
 * 具体实现下沉到 {@link JdbcMetadataManager}（mysql / h2 共享的 JDBC 逻辑）。
 * <p>
 * file 相关的 WAL 实现（recover 全量灌内存、logAddFile 追加写、backfillNodeIds 日志原子重写）
 * 已随 file 模式退役删除。FILE 仅作为迁移链中规整日志格式的前置步骤使用
 * （{@code MigrationRunner.run(FILE, ...)}），不经 {@code MetadataManager}。
 */
public abstract class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

    // ==================== 查询契约（子类实现） ====================

    /**
     * 按 hash 查询文件元数据（含全部副本位置）。
     *
     * @param hash 文件 hash
     * @return 元数据实体或 null（不存在 / 无可用副本）
     */
    public abstract MetadataCacheManager.MetadataEntry queryByHash(String hash);

    /**
     * 按 storageId 反查 hash。
     *
     * @param storageId 存储ID
     * @return 文件 hash 或 null
     */
    public abstract String queryHashByStorageId(String storageId);

    /**
     * 检查文件是否存在（用于集群协同秒传判定）。
     *
     * @param hash 文件 hash
     * @return true=存在
     */
    public abstract boolean isFileExist(String hash);

    // ==================== 上传锁契约（子类实现） ====================

    /**
     * 尝试获取文件上传分布式锁。
     *
     * @param hash   文件 hash
     * @param nodeId 持锁节点标识
     * @return true=获取成功，false=已被锁定
     */
    public abstract boolean tryAcquireUploadLock(String hash, String nodeId);

    /**
     * 释放文件上传锁。
     *
     * @param hash 文件 hash
     */
    public abstract void releaseUploadLock(String hash);

    // ==================== 持久化契约（子类实现） ====================

    /**
     * 恢复元数据到内存 maps。
     * <p>
     * <b>JDBC 模式不调用此方法</b>：走懒加载（cache miss → queryByHash），此处实现为 no-op。
     * 保留契约方法签名供未来非 JDBC 实现复用。
     *
     * @param filenameToHash 文件名->Hash 映射
     * @param hashToStorage  Hash->存储地址 映射
     * @param hashToId       Hash->存储编号 映射
     * @param persistedHashes 已持久化的 Hash 集合（用于去重）
     */
    public abstract void recover(Map<String, String> filenameToHash,
                                 Map<String, String> hashToStorage,
                                 Map<String, String> hashToId,
                                 Set<String> persistedHashes) throws IOException;

    /**
     * 持久化文件元数据 + 全部副本位置。
     *
     * @param filename          文件名
     * @param hash              文件 hash
     * @param storageId         存储 ID
     * @param replicationFactor 目标副本数（1=单副本，2/3=组内节点数）
     * @param fileSize          文件大小（字节，NULL=未知）；Driver 提交协议带真实大小
     * @param fileType          文件类型标签（扩展名识别，NULL=未知）
     * @param locations         全部副本位置（PRIMARY 恒首位）
     */
    public abstract void logAddFile(String filename, String hash, String storageId,
                                    int replicationFactor, Long fileSize, String fileType,
                                    List<ReplicaLocation> locations) throws IOException;

    // ==================== 能力探测 ====================

    /**
     * 底层是否 JDBC 支撑（mysql / h2）。
     * <p>
     * file 模式已退役，运行时不存在非 JDBC 实例。本方法供 {@link NameNodeHandler} 做能力判断：
     * {@code true} 表示支持多副本 JOIN 查询、分布式上传锁、在线 backfill 等 JDBC 能力。
     */
    public boolean isJdbcBacked() {
        return false;
    }

    /**
     * 返回内部 JDBC 数据源；非 JDBC 实现返回 null。
     * <p>
     * 供 {@link NameNodeServer} 复用连接池构造冗余组件（{@code ReplicationGroupStore} /
     * {@code ReplicaSyncScheduler}）以及 {@link NameNodeHandler#insertReplicaLocation} 登记副本行。
     */
    public DataSource getDataSource() {
        return null;
    }

    // ==================== 在线回填（JDBC 实现覆写） ====================

    /**
     * JDBC 模式在线补全 file_location.datanode_id（设计文档 §4.9.2）。
     * <p>
     * 利用 Registry 拉取的 host:port → node_id 映射，把 file_location 中 datanode_addr=host:port
     * 且 datanode_id IS NULL 的记录补上 node_id。file 模式已退役，默认 no-op。
     *
     * @return 被补全的记录数，0=无补全
     */
    public int backfillDataNodeIds() {
        LOG.debug("[MetadataManager] 非 JDBC 模式，backfillDataNodeIds no-op");
        return 0;
    }

    /**
     * file 模式在线回填 namenode_meta.log 的 node_id（设计文档 §4.9.4）。
     * <p>
     * file 模式已退役，默认 no-op 返回 -1。保留契约签名。
     *
     * @return 被替换的行数，-1=未执行
     */
    public int backfillNodeIds() {
        LOG.debug("[MetadataManager] file 模式已退役，backfillNodeIds no-op");
        return -1;
    }
}
