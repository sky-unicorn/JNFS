package org.jnfs.namenode;

import org.jnfs.common.DataDirResolver;
import org.jnfs.common.NodeAddressResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;

/**
 * 元数据管理器
 * 负责元数据的持久化存储 (Write-Ahead Log / Append Only Log) 和恢复
 */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

    private static final String METADATA_FILE = "namenode_meta.log";
    private final File logFile;

    public MetadataManager() {
        this.logFile = DataDirResolver.resolve(METADATA_FILE);
    }

    /**
     * 根据Hash查询元数据 (新增接口，用于按需加载)
     * @param hash 文件Hash
     * @return 元数据实体或null
     */
    public MetadataCacheManager.MetadataEntry queryByHash(String hash) {
        // 默认实现：遍历全量恢复的内存Map (File模式下通常由Handler维护全量Map，这里暂不操作)
        // 注意：由于 MetadataManager 基类原本只负责 IO，不负责持有数据，
        // 在新架构下，File模式需要在这里实现简单的文件扫描或依赖外部索引。
        // 为了简化 File 模式兼容性，我们假设 File 模式依然使用全量内存加载，
        // 所以这个方法在 File 模式下可能不会被频繁调用，或者直接返回 null 让缓存层失效。
        return null; 
    }

    /**
     * 根据 StorageId 查询 Hash (新增接口，用于反向索引回源)
     * @param storageId 存储ID
     * @return 文件Hash或null
     */
    public String queryHashByStorageId(String storageId) {
        return null; // 默认返回 null，子类覆盖
    }

    /**
     * 恢复元数据到内存
     * @param filenameToHash 文件名->Hash 映射
     * @param hashToStorage Hash->存储地址 映射
     * @param hashToId Hash->存储编号 映射
     * @param idToHash 存储编号->Hash 映射 (新增，用于反向查找)
     * @param persistedHashes 已持久化ID的Hash集合 (用于去重)
     */
    public void recover(Map<String, String> filenameToHash,
                        Map<String, String> hashToStorage,
                        Map<String, String> hashToId,
                        Set<String> persistedHashes) throws IOException {
        if (!logFile.exists()) {
            LOG.info("[MetadataManager] 元数据日志不存在，启动为空状态");
            return;
        }

        LOG.info("[MetadataManager] 正在恢复元数据...");
        int count = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 格式: ADD|filename|hash|address|storageId
                // 5 字段必须齐全，不允许兼容旧格式（旧数据必须先经过迁移）
                String[] parts = line.split("\\|");
                if (parts.length == 5 && "ADD".equals(parts[0])) {
                    String filename = parts[1];
                    String hash = parts[2];
                    String address = parts[3];
                    String storageId = parts[4];

                    filenameToHash.put(filename, hash);
                    hashToStorage.put(hash, address);
                    hashToId.put(hash, storageId);
                    persistedHashes.add(hash);
                    count++;
                } else {
                    throw new IOException("日志格式错误，缺少 storageId。请先执行数据迁移。行内容: " + line);
                }
            }
        }
        LOG.info("[MetadataManager] 恢复完成，共加载 {} 条记录", count);
    }

    /**
     * 持久化记录一条新文件元数据
     */
    public synchronized void logAddFile(String filename, String hash, String address, String storageId) throws IOException {
        String record = String.format("ADD|%s|%s|%s|%s", filename, hash, address, storageId);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
            writer.write(record);
            writer.newLine();
            writer.flush();
        }
    }

    /**
     * 尝试获取文件上传锁 (用于集群协同)
     * @param hash 文件Hash
     * @param nodeId 节点标识
     * @return true=获取成功, false=已被锁定
     */
    public boolean tryAcquireUploadLock(String hash, String nodeId) {
        return true; // 默认文件模式无需分布式锁，直接返回成功 (依靠本地内存锁)
    }

    /**
     * 释放文件上传锁
     * @param hash 文件Hash
     */
    public void releaseUploadLock(String hash) {
        // 默认不操作
    }

    /**
     * 检查文件是否存在 (用于集群协同)
     * @param hash 文件Hash
     * @return true=存在
     */
    public boolean isFileExist(String hash) {
        return false; // 默认仅依赖内存检查，返回false让上层检查内存
    }

    /**
     * File 模式: 根据 Registry 提供的 host:port -> node_id 映射，在线回填
     * namenode_meta.log 中历史行的 host:port 为 node_id
     *
     * 对应设计文档 §4.9.4: file 模式对应处理
     * 将日志中的旧格式 host:port 替换为 node_id，生成新日志文件
     *
     * 语义正确性：
     * NameNode 从 Registry 拉取的 DataNode 列表包含 node_id|host:port 映射，
     * 当前在线的 DataNode 证明了"我的 node_id 就是当前这个地址"，
     * 因此 namenode_meta.log 中所有 host:port = 该地址的行，替换为 node_id 是正确的。
     *
     * @return 被替换的行数，-1 表示未执行（日志文件不存在）
     */
    public synchronized int backfillNodeIds() {
        if (!logFile.exists()) {
            LOG.info("[MetadataManager] 元数据日志不存在，跳过 node_id 回填");
            return -1;
        }

        LOG.info("[MetadataManager] 开始在线回填 node_id...");
        int replacedCount = 0;
        int totalLines = 0;
        File tmpFile = new File(logFile.getAbsolutePath() + ".tmp");

        // 注意：rename 必须在 reader/writer 句柄关闭后执行。
        // Windows 下被占用文件无法重命名，故把重命名放在 try-with-resources 块外。
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile))) {

            String line;
            while ((line = reader.readLine()) != null) {
                totalLines++;
                String[] parts = line.split("\\|");
                // 新格式: ADD|filename|hash|node_id|storageId (5 parts)
                // 旧格式: ADD|filename|hash|host:port|storageId (5 parts, 但第4字段是 host:port)
                if (parts.length == 5 && "ADD".equals(parts[0])) {
                    String address = parts[3]; // 可能是 node_id 或 host:port

                    // 如果是 host:port 格式，尝试通过 NodeAddressResolver 查找 node_id
                    if (NodeAddressResolver.isHostPort(address)) {
                        String nodeId = NodeAddressResolver.getNodeId(address);
                        if (!nodeId.equals(address)) {
                            // 找到了映射：host:port -> node_id
                            parts[3] = nodeId;
                            line = String.join("|", parts);
                            replacedCount++;
                        }
                        // 如果找不到映射（nodeId == address），保持原样
                    }
                    // 如果已经是 node_id 格式（非 host:port），保持不变
                }
                writer.write(line);
                writer.newLine();
            }
            writer.flush();

        } catch (IOException e) {
            LOG.error("[MetadataManager] node_id 回填失败", e);
            tmpFile.delete();
            return 0;
        }

        // 句柄已关闭，执行原子替换
        try {
            if (!tmpFile.renameTo(logFile)) {
                Files.move(tmpFile.toPath(), logFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            }
        } catch (IOException e) {
            LOG.error("[MetadataManager] node_id 回填失败: 无法重命名临时文件: {}", e.getMessage());
            tmpFile.delete();
            return 0;
        }

        LOG.info("[MetadataManager] node_id 回填完成: {}/{} 行已替换", replacedCount, totalLines);
        return replacedCount;
    }
}
