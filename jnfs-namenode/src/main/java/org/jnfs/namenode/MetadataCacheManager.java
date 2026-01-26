package org.jnfs.namenode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 元数据缓存管理器
 * 使用 Caffeine 实现 LRU 缓存，作为一级存储
 * 持久层 (MySQL/File) 作为二级存储
 */
public class MetadataCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheManager.class);

    // 缓存配置
    private final boolean enabled;
    private final long maxSize;
    private final String writePolicy; // sync 或 async

    // 缓存容器
    // Key: Hash, Value: MetadataEntry (包含 filename, address, storageId)
    private final Cache<String, MetadataEntry> metaCache;
    
    // 反向索引缓存 (Key: storageId, Value: hash)
    // 用于 handleDownloadLocRequest 通过 storageId 查找 Hash
    private final Cache<String, String> idToHashCache;
    
    // 底层持久化管理器
    private final MetadataManager metadataManager;

    public MetadataCacheManager(MetadataManager metadataManager, boolean enabled, long maxSize, String writePolicy) {
        this.metadataManager = metadataManager;
        this.enabled = enabled;
        this.maxSize = maxSize;
        this.writePolicy = writePolicy;

        if (enabled) {
            LOG.info("初始化元数据缓存: MaxSize={}, Policy={}", maxSize, writePolicy);
            this.metaCache = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .recordStats() // 开启统计
                    .removalListener((RemovalListener<String, MetadataEntry>) (key, value, cause) -> {
                         if (cause.wasEvicted()) {
                             LOG.debug("元数据被缓存淘汰: Hash={}", key);
                         }
                    })
                    .build();
            
            // 初始化反向索引缓存 (容量与主缓存保持一致或略大)
            this.idToHashCache = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .build();
        } else {
            this.metaCache = null;
            this.idToHashCache = null;
        }
    }

    /**
     * 获取元数据 (Read-Through)
     */
    public MetadataEntry get(String hash) {
        if (!enabled) {
            return metadataManager.queryByHash(hash);
        }

        // 1. 查缓存
        MetadataEntry entry = metaCache.getIfPresent(hash);
        if (entry != null) {
            return entry;
        }

        // 2. 查持久层
        entry = metadataManager.queryByHash(hash);
        if (entry != null) {
            // 回填缓存
            metaCache.put(hash, entry);
            // 同时回填反向索引
            if (idToHashCache != null && entry.storageId != null) {
                idToHashCache.put(entry.storageId, hash);
            }
        }
        return entry;
    }
    
    /**
     * 根据 StorageId 获取 Hash (新增接口)
     */
    public String getHashByStorageId(String storageId) {
        if (!enabled) {
            // 如果没开启缓存，直接查持久层
            return metadataManager.queryHashByStorageId(storageId);
        }
        
        // 1. 查反向索引缓存
        String hash = idToHashCache.getIfPresent(storageId);
        if (hash != null) {
            return hash;
        }
        
        // 2. 如果缓存没命中，查持久层 (Cache Miss Handler)
        hash = metadataManager.queryHashByStorageId(storageId);
        if (hash != null) {
            // 回填反向索引
            idToHashCache.put(storageId, hash);
        }
        return hash;
    }

    /**
     * 保存元数据 (Write-Through)
     */
    public void put(String filename, String hash, String address, String storageId) {
        // 1. 先持久化
        // 目前仅实现同步写入 (Sync)，异步写入需引入队列和Worker
        try {
            metadataManager.logAddFile(filename, hash, address, storageId);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Metadata persistence failed", e);
        }

        // 2. 更新缓存
        if (enabled) {
            MetadataEntry entry = new MetadataEntry(filename, hash, address, storageId);
            metaCache.put(hash, entry);
            idToHashCache.put(storageId, hash);
        }
    }
    
    /**
     * 仅更新缓存 (用于启动恢复时)
     */
    public void putCacheOnly(String hash, MetadataEntry entry) {
        if (enabled && entry != null) {
            metaCache.put(hash, entry);
            if (entry.storageId != null) {
                idToHashCache.put(entry.storageId, hash);
            }
        }
    }

    /**
     * 元数据实体类
     */
    public static class MetadataEntry {
        public final String filename;
        public final String hash;
        public final String address;
        public final String storageId;

        public MetadataEntry(String filename, String hash, String address, String storageId) {
            this.filename = filename;
            this.hash = hash;
            this.address = address;
            this.storageId = storageId;
        }
    }
}
