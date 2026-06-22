package org.jnfs.namenode.migration;

import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * File 模式 V0 → V1 迁移步骤
 * <p>
 * 动作：
 * 1. 读 namenode_meta.log 全部行
 * 2. 对旧格式行 ADD|filename|hash|host:port（无 storageId）：
 *    - 同一 hash 已分配过 storageId 则复用，否则 UUID.randomUUID() 生成一次
 *    - 同 hash 内去重，保证 INV-2（storage_id 全局唯一）
 * 3. 把所有行统一重写为新格式 ADD|filename|hash|host:port|storageId
 * 4. 原子写入：先写 namenode_meta.log.tmp，然后 renameTo 覆盖
 * <p>
 * 幂等性保证：
 * - 重跑时，凡是已经包含 storageId 的行直接保留，不为它重新分配
 * - 中途崩溃的恢复：临时文件残留则删除，原文件未替换则按"全新迁移"重跑
 */
public class FileV0ToV1 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(FileV0ToV1.class);

    private static final String METADATA_FILE = "namenode_meta.log";
    private static final String METADATA_TMP = "namenode_meta.log.tmp";

    @Override
    public int fromVersion() {
        return 0;
    }

    @Override
    public int toVersion() {
        return 1;
    }

    @Override
    public boolean supports(StorageMode mode) {
        return mode == StorageMode.FILE;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        File dataDir = ctx.dataDir();
        File logFile = new File(dataDir, METADATA_FILE);
        File tmpFile = new File(dataDir, METADATA_TMP);

        if (!logFile.exists()) {
            LOG.info("FileV0ToV1: {} 不存在，跳过迁移（空状态）", logFile);
            return null;
        }

        // 1. 读取所有行
        List<String> lines;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8))) {
            lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        LOG.info("FileV0ToV1: 读取到 {} 行", lines.size());

        // 2. 解析行并补全 storageId
        // hash -> storageId 映射（用于同 hash 复用 storageId，保证 INV-2）
        Map<String, String> hashToStorageId = new LinkedHashMap<>();
        int allocatedCount = 0;
        int alreadyComplete = 0;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            String[] parts = line.split("\\|");
            if (parts.length < 4 || !"ADD".equals(parts[0])) {
                LOG.warn("FileV0ToV1: 跳过无法识别的行 {}: {}", i, line);
                continue;
            }

            String filename = parts[1];
            String hash = parts[2];
            String address = parts[3];
            String storageId;

            if (parts.length >= 5 && !parts[4].isEmpty()) {
                // 新格式：已有 storageId，直接保留（幂等）
                storageId = parts[4];
                hashToStorageId.putIfAbsent(hash, storageId);
                alreadyComplete++;
            } else {
                // 旧格式：需要分配 storageId
                storageId = hashToStorageId.get(hash);
                if (storageId == null) {
                    // INV-1: storage_id 一旦分配永不改变
                    // UUID.randomUUID 只在首次迁移时执行一次
                    storageId = UUID.randomUUID().toString();
                    hashToStorageId.put(hash, storageId);
                    allocatedCount++;
                }
            }

            // 重写行（保留 host:port 地址，不替换为 node_id — 见设计文档 §4.9）
            lines.set(i, "ADD|" + filename + "|" + hash + "|" + address + "|" + storageId);
        }

        LOG.info("FileV0ToV1: {} 行已包含 storageId, {} 行分配了新 storageId", alreadyComplete, allocatedCount);

        if (allocatedCount == 0) {
            LOG.info("FileV0ToV1: 无需分配新 storageId，日志格式已是最新，跳过重写");
            return null;
        }

        // 3. 写入临时文件
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
            writer.flush();
        }

        // 4. fsync 确保数据落盘
        try (FileOutputStream fos = new FileOutputStream(tmpFile, true)) {
            fos.getFD().sync();
        }

        // 5. 原子替换
        if (!tmpFile.renameTo(logFile)) {
            java.nio.file.Files.move(tmpFile.toPath(), logFile.toPath(),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }

        // 6. 清理可能的残留临时文件
        if (tmpFile.exists()) {
            tmpFile.delete();
        }

        LOG.info("FileV0ToV1: 迁移完成，新增 {} 个 storageId", allocatedCount);
        return null;
    }
}
