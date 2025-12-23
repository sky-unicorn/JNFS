package org.jnfs.namenode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 元数据管理器
 * 负责元数据的持久化存储 (Write-Ahead Log / Append Only Log) 和恢复
 */
public class MetadataManager {

    private static final String METADATA_FILE = "namenode_meta.log";
    private final File logFile;

    public MetadataManager() {
        this.logFile = new File(METADATA_FILE);
    }

    /**
     * 恢复元数据到内存
     * @param filenameToHash 文件名->Hash 映射
     * @param hashToStorage Hash->存储地址 映射
     * @param hashToId Hash->存储编号 映射
     * @param persistedHashes 已持久化ID的Hash集合 (用于去重)
     */
    public void recover(Map<String, String> filenameToHash, 
                        Map<String, String> hashToStorage,
                        Map<String, String> hashToId,
                        Set<String> persistedHashes) {
        if (!logFile.exists()) {
            System.out.println("[MetadataManager] 元数据日志不存在，启动为空状态");
            return;
        }

        System.out.println("[MetadataManager] 正在恢复元数据...");
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
                        // 标记该 Hash 的 ID 已经持久化
                        persistedHashes.add(hash);
                    } else {
                        // 旧数据没有 ID，检查内存中是否已有，没有则生成
                        storageId = hashToId.computeIfAbsent(hash, k -> UUID.randomUUID().toString());
                    }

                    filenameToHash.put(filename, hash);
                    hashToStorage.put(hash, address);
                    hashToId.put(hash, storageId);
                    count++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("[MetadataManager] 恢复元数据时发生错误: " + e.getMessage());
        }
        System.out.println("[MetadataManager] 恢复完成，共加载 " + count + " 条记录");
    }

    /**
     * 持久化记录一条新文件元数据
     */
    public synchronized void logAddFile(String filename, String hash, String address, String storageId) {
        String record = String.format("ADD|%s|%s|%s|%s", filename, hash, address, storageId);
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
            writer.write(record);
            writer.newLine();
            writer.flush(); 
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("[MetadataManager] 写入元数据日志失败: " + e.getMessage());
        }
    }
}
