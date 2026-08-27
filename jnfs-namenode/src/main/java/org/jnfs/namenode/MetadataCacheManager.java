package org.jnfs.namenode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 元数据缓存管理器
 * 使用 Caffeine 实现 LRU 缓存，作为一级存储
 * 持久层 (MySQL/File) 作为二级存储
 */
public class MetadataCacheManager {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataCacheManager.class);

    // 缓存配置
    private final boolean enabled;

    // 缓存容器
    // Key: Hash, Value: MetadataEntry (包含 filename, storageId, locations 副本列表)
    private final Cache<String, MetadataEntry> metaCache;
    
    // 反向索引缓存 (Key: storageId, Value: hash)
    // 用于 handleDownloadLocRequest 通过 storageId 查找 Hash
    private final Cache<String, String> idToHashCache;
    
    // 底层持久化管理器
    private final MetadataManager metadataManager;

    public MetadataCacheManager(MetadataManager metadataManager, boolean enabled, long maxSize) {
        this.metadataManager = metadataManager;
        this.enabled = enabled;

        if (enabled) {
            LOG.info("初始化元数据缓存: MaxSize={}", maxSize);
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
     *
     * @param storageId         存储ID
     * @param replicationFactor 目标副本数（1=单副本，2/3=组内节点数），写入 file_metadata.replication_factor
     * @param fileSize          文件大小（字节，NULL=未知），写入 file_metadata.file_size
     * @param fileType          文件类型标签（扩展名识别，NULL=未知），写入 file_metadata.file_type
     * @param locations         全部副本位置（PRIMARY 恒在首位；file 模式为单元素列表）
     */
    public void put(String filename, String hash, String storageId,
                    int replicationFactor, Long fileSize, String fileType,
                    List<ReplicaLocation> locations) {
        // 1. 先持久化
        // 目前仅实现同步写入 (Sync)，异步写入需引入队列和Worker
        try {
            metadataManager.logAddFile(filename, hash, storageId, replicationFactor, fileSize, fileType, locations);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Metadata persistence failed", e);
        }

        // 2. 更新缓存
        if (enabled) {
            MetadataEntry entry = new MetadataEntry(filename, hash, storageId, fileSize, fileType, locations);
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
     * 失效指定 hash 的缓存（供 DATA_REPLICA_COMMIT 登记后调用）。
     * <p>
     * COMMIT 登记新副本行后，旧缓存中的 locations 列表已过时（缺少新副本），
     * 必须失效以触发下次 get() 时从持久层重新加载。
     * <p>
     * 同时清除 idToHashCache 对应项（若存在），保证反向索引一致性。
     *
     * @param hash 文件 hash
     */
    public void invalidate(String hash) {
        if (!enabled || hash == null) {
            return;
        }
        MetadataEntry old = metaCache.getIfPresent(hash);
        metaCache.invalidate(hash);
        if (old != null && old.storageId != null) {
            idToHashCache.invalidate(old.storageId);
        }
    }

    /**
     * 元数据实体类
     * <p>
     * 一个 entry 对应一个文件 hash，内含该文件的全部副本位置（{@link ReplicaLocation}）。
     * per-file 字段（filename/hash/storageId）对同一 hash 的所有副本行相同。
     * 缓存泛型不变（Cache&lt;String, MetadataEntry&gt;），get() 返回类型不变，调用方改动最小。
     */
    public static class MetadataEntry {

        /** 副本排序：role ASC（PRIMARY 优先），status DESC（ACTIVE 优先） */
        private static final Comparator<ReplicaLocation> LOCATION_ORDER =
                Comparator.comparingInt(ReplicaLocation::getRole)
                        .thenComparing(Comparator.comparingInt(ReplicaLocation::getStatus).reversed());

        public final String filename;
        public final String hash;
        public final String storageId;
        /** 文件大小（字节）；NULL=未知（旧数据/尚未回填） */
        public final Long fileSize;
        /** 文件类型标签；NULL=未知（无扩展名且尚未内容嗅探） */
        public final String fileType;
        /** 全部副本位置（不可变，按 role ASC/status DESC 排序，PRIMARY+ACTIVE 恒在首位） */
        public final List<ReplicaLocation> locations;

        /**
         * @param locations 全部副本位置；构造时会做防御性拷贝并按 role ASC/status DESC 排序。
         *                  传 null 或空列表表示无副本（getPrimaryLocation 返回 null）。
         */
        public MetadataEntry(String filename, String hash, String storageId, List<ReplicaLocation> locations) {
            this(filename, hash, storageId, null, null, locations);
        }

        /**
         * 全量构造：额外携带 fileSize / fileType（可 null）。
         *
         * @param locations 全部副本位置；构造时会做防御性拷贝并按 role ASC/status DESC 排序。
         *                  传 null 或空列表表示无副本（getPrimaryLocation 返回 null）。
         */
        public MetadataEntry(String filename, String hash, String storageId, Long fileSize,
                             String fileType, List<ReplicaLocation> locations) {
            this.filename = filename;
            this.hash = hash;
            this.storageId = storageId;
            this.fileSize = fileSize;
            this.fileType = fileType;
            if (locations == null || locations.isEmpty()) {
                this.locations = Collections.emptyList();
            } else {
                List<ReplicaLocation> sorted = new ArrayList<>(locations);
                sorted.sort(LOCATION_ORDER);
                this.locations = Collections.unmodifiableList(sorted);
            }
        }

        /**
         * 返回主副本位置（locations 首位）。locations 为空返回 null。
         * 供需要单地址的调用方（秒传/下载单地址场景）使用。
         */
        public ReplicaLocation getPrimaryLocation() {
            return locations.isEmpty() ? null : locations.get(0);
        }

        /**
         * 返回主副本的 nodeId。无副本返回 null。
         */
        public String getPrimaryNodeId() {
            ReplicaLocation primary = getPrimaryLocation();
            return primary == null ? null : primary.getNodeId();
        }

        /** 副本数量 */
        public int getLocationCount() {
            return locations.size();
        }
    }
}
