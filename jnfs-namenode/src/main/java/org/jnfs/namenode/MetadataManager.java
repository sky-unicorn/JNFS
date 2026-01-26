package org.jnfs.namenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 元数据管理器
 * 负责元数据的持久化存储 (Write-Ahead Log / Append Only Log) 和恢复
 */
public class MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

    private static final String METADATA_FILE = "namenode_meta.log";
    private final File logFile;

    public MetadataManager() {
        this.logFile = new File(METADATA_FILE);
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
                        Set<String> persistedHashes) {
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
                String[] parts = line.split("\\|");
                if (parts.length >= 4 && "ADD".equals(parts[0])) {
                    String filename = parts[1];
                    String hash = parts[2];
                    String address = parts[3];
                    String storageId;

                    if (parts.length >= 5) {
                        storageId = parts[4];
                        persistedHashes.add(hash);
                    } else {
                        // 旧数据兼容
                        storageId = hashToId.computeIfAbsent(hash, k -> UUID.randomUUID().toString());
                    }

                    filenameToHash.put(filename, hash);
                    hashToStorage.put(hash, address);
                    hashToId.put(hash, storageId);
                    count++;
                }
            }
        } catch (IOException e) {
            LOG.error("[MetadataManager] 恢复元数据时发生错误", e);
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
}
