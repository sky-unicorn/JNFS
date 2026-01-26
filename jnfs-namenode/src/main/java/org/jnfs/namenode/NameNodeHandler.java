package org.jnfs.namenode;

import cn.hutool.cache.CacheUtil;
import cn.hutool.cache.impl.TimedCache;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NameNode 业务处理器
 * 处理客户端的元数据请求
 *
 * 升级：支持动态初始化 MetadataManager
 * 修复：引入 TimedCache 解决 pendingUploads 死锁问题
 */
@ChannelHandler.Sharable
public class NameNodeHandler extends SimpleChannelInboundHandler<Packet> {

    private static final Logger LOG = LoggerFactory.getLogger(NameNodeHandler.class);

    private static final String VALID_TOKEN = "jnfs-secure-token-2025";

    // 移除旧的静态全量 Map
    // private static final Map<String, String> filenameToHash = new ConcurrentHashMap<>();
    // private static final Map<String, String> hashToStorage = new ConcurrentHashMap<>();
    // private static final Map<String, String> hashToId = new ConcurrentHashMap<>();
    // private static final Map<String, String> idToHash = new ConcurrentHashMap<>();
    
    // 仅保留 persistedHashes 用于快速判重 (优化点：如果数据量过大，这个Set也应该移除，改用 BloomFilter 或 Cache)
    // 但为了兼容 File 模式的逻辑，暂时保留，但在 MySQL 模式且启用 Cache 时，不应过度依赖它
    private static final Set<String> persistedHashes = ConcurrentHashMap.newKeySet();

    // Key: Hash, Value: Timestamp (虽然Value不重要)
    // 过期时间设置为 10 分钟 (600,000 ms)
    private static final TimedCache<String, Boolean> pendingUploads = CacheUtil.newTimedCache(10 * 60 * 1000);

    static {
        // 启动定时清理任务，每分钟检查一次过期
        pendingUploads.schedulePrune(60 * 1000);
    }

    // 引入缓存管理器
    private static MetadataCacheManager cacheManager;

    // NameNode 唯一标识 (用于分布式锁)
    private static final String NODE_ID = UUID.randomUUID().toString();

    // 不再 final，不再静态初始化
    private static MetadataManager metadataManager;

    // 活跃的 DataNode 列表 (包含 freeSpace 信息)
    private static final List<String> dataNodes = new ArrayList<>();

    // 负载均衡器
    private static final LoadBalancer loadBalancer = new WeightedRandomStrategy();

    // 锁分段数组，用于减小锁粒度 (128个分段锁)
    private static final Object[] SEGMENT_LOCKS = new Object[128];
    static {
        for (int i = 0; i < SEGMENT_LOCKS.length; i++) {
            SEGMENT_LOCKS[i] = new Object();
        }
    }

    /**
     * 获取分段锁
     */
    private Object getLock(String key) {
        return SEGMENT_LOCKS[Math.abs(key.hashCode() % SEGMENT_LOCKS.length)];
    }

    /**
     * 初始化元数据管理器 (由 NameNodeServer 启动时调用)
     */
    public static void initMetadataManager(MetadataManager manager, MetadataCacheManager cache) {
        metadataManager = manager;
        cacheManager = cache;
        
        // 恢复数据到缓存 (预热)
        // 注意：这里为了兼容，我们构建临时的 Map 接收 recover 数据，然后灌入 Cache
        // 对于 MySQL 模式，如果数据量巨大，recover 应该被禁用或改为 limit 加载
        Map<String, String> f2h = new HashMap<>();
        Map<String, String> h2s = new HashMap<>();
        Map<String, String> h2id = new HashMap<>();
        
        // 只有 File 模式或者配置了强制预热才执行全量 recover
        // 这里做一个简单的判断：如果是 MySQL 模式，我们假设不再全量 recover，除非明确要求
        boolean isFileMode = !(manager instanceof MySQLMetadataManager);
        
        if (isFileMode) {
             LOG.info("File模式: 执行全量元数据恢复...");
             manager.recover(f2h, h2s, h2id, persistedHashes);
             
             // 灌入 Cache
             for (Map.Entry<String, String> entry : h2id.entrySet()) {
                 String hash = entry.getKey();
                 String storageId = entry.getValue();
                 String address = h2s.get(hash);
                 // 由于 filenameToHash 是多对一，这里反向查找有点麻烦，暂且简化
                 // 实际上 Cache 主要以 Hash 为 Key
                 cacheManager.putCacheOnly(hash, new MetadataCacheManager.MetadataEntry(
                     "loaded_from_file", hash, address, storageId
                 ));
             }
        } else {
             LOG.info("MySQL模式: 跳过全量内存恢复，启用懒加载");
        }
    }

    public static void initDataNodes(List<String> nodes) {
        dataNodes.clear();
        if (nodes != null) {
            dataNodes.addAll(nodes);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        if (!validateToken(packet.getToken())) {
            LOG.warn("安全拦截: 无效的 Token - {}", ctx.channel().remoteAddress());
            sendResponse(ctx, CommandType.ERROR, "Authentication Failed: Invalid Token".getBytes(StandardCharsets.UTF_8));
            return;
        }

        CommandType type = packet.getCommandType();
        switch (type) {
            case NAMENODE_CHECK_EXISTENCE:
                handleCheckExistence(ctx, packet);
                break;
            case NAMENODE_PRE_UPLOAD:
                handlePreUpload(ctx, packet);
                break;
            case NAMENODE_REQUEST_UPLOAD_LOC:
                handleUploadLocRequest(ctx);
                break;
            case NAMENODE_COMMIT_FILE:
                handleCommitFile(ctx, packet);
                break;
            case NAMENODE_REQUEST_DOWNLOAD_LOC:
                handleDownloadLocRequest(ctx, packet);
                break;
            default:
                sendResponse(ctx, CommandType.ERROR, "未知命令".getBytes(StandardCharsets.UTF_8));
        }
    }

    private boolean validateToken(String token) {
        return VALID_TOKEN.equals(token);
    }

    private void handleCheckExistence(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);
        
        // 1. 查缓存/持久层
        MetadataCacheManager.MetadataEntry entry = cacheManager.get(hash);

        if (entry != null) {
            LOG.info("命中秒传: Hash={}", hash);
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, entry.address.getBytes(StandardCharsets.UTF_8));
        } else {
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_NOT_EXIST, "Not Found".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handlePreUpload(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);

        synchronized (getLock(hash)) {
            // 1. 查缓存/持久层
            MetadataCacheManager.MetadataEntry entry = cacheManager.get(hash);
            if (entry != null) {
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, entry.address.getBytes(StandardCharsets.UTF_8));
                return;
            }

            // 集群协同检查 (仅在 MySQL 模式下有效)
            if (metadataManager != null) {
                // 1. 检查集群中是否已存在 (防止多节点重复上传)
                // 注意: cacheManager.get 其实已经包含了这个逻辑 (如果 cache 没命中会去查 DB)
                // 但这里可能存在并发间隙，所以 tryAcquireUploadLock 依然重要
                
                // 2. 尝试获取分布式锁
                if (!metadataManager.tryAcquireUploadLock(hash, NODE_ID)) {
                    LOG.info("获取集群锁失败 (正在上传中): Hash={}", hash);
                    sendResponse(ctx, CommandType.NAMENODE_RESPONSE_WAIT, "Cluster-Waiting".getBytes(StandardCharsets.UTF_8));
                    return;
                }
            }

            if (pendingUploads.containsKey(hash)) {
                LOG.info("并发上传冲突，通知等待: Hash={}", hash);
                // 回滚分布式锁
                if (metadataManager != null) {
                    metadataManager.releaseUploadLock(hash);
                }
                sendResponse(ctx, CommandType.NAMENODE_RESPONSE_WAIT, "Waiting".getBytes(StandardCharsets.UTF_8));
                return;
            }

            pendingUploads.put(hash, true);
            LOG.info("允许上传: Hash={}", hash);
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_ALLOW, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleUploadLocRequest(ChannelHandlerContext ctx) {
        if (dataNodes.isEmpty()) {
            sendResponse(ctx, CommandType.ERROR, "无可用 DataNode".getBytes(StandardCharsets.UTF_8));
            return;
        }

        // 使用负载均衡策略选择最佳节点
        String selectedNode = loadBalancer.select(dataNodes);

        if (selectedNode != null) {
            // 响应中只包含 host:port，不需要 freeSpace
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_UPLOAD_LOC, selectedNode.getBytes(StandardCharsets.UTF_8));
        } else {
            sendResponse(ctx, CommandType.ERROR, "选择 DataNode 失败".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleCommitFile(ChannelHandlerContext ctx, Packet packet) {
        String data = new String(packet.getData(), StandardCharsets.UTF_8);
        String[] parts = data.split("\\|");
        if (parts.length != 3) {
            sendResponse(ctx, CommandType.ERROR, "格式错误".getBytes(StandardCharsets.UTF_8));
            return;
        }

        String filename = parts[0];
        String hash = parts[1];
        String address = parts[2];
        String storageId;

        // 1. 快速检查：如果已存在，直接返回
        // 优化：先查缓存
        MetadataCacheManager.MetadataEntry existing = cacheManager.get(hash);
        if (existing != null) {
             LOG.info("忽略重复元数据提交 (已存在): {}", filename);
             sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, existing.storageId.getBytes(StandardCharsets.UTF_8));
             return;
        }

        synchronized (getLock(hash)) {
            // 双重检查
            existing = cacheManager.get(hash);
            if (existing != null) {
                 sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, existing.storageId.getBytes(StandardCharsets.UTF_8));
                 return;
            }

            pendingUploads.remove(hash);
            
            // 注意：这里可能需要从 hashToId 里面取，但现在没有全量 hashToId 了
            // 所以我们总是生成新的 ID，或者依赖数据库去重
            // 为了简单，我们生成新的 ID，如果在数据库插入时冲突（Hash已存在），则需要处理
            // 但因为前面已经 cacheManager.get(hash) 判重过了，这里冲突概率很低（除非并发）
            storageId = UUID.randomUUID().toString();

            // 持久化到 MySQL 或 文件，并更新缓存
            try {
                if (cacheManager != null) {
                    cacheManager.put(filename, hash, address, storageId);
                }
            } catch (Exception e) {
                LOG.error("元数据提交失败: {}", filename, e);
                // [FIXED] Critical: 如果提交失败，必须释放分布式锁，否则用户需等待 30 分钟
                if (metadataManager != null) {
                    metadataManager.releaseUploadLock(hash);
                }
                sendResponse(ctx, CommandType.ERROR, "Metadata Persistence Failed".getBytes(StandardCharsets.UTF_8));
                return;
            }
            
            // 兼容性保留
            // persistedHashes.add(hash); // [FIXED] Removed to prevent memory leak

            LOG.info("文件已注册并持久化: {}, ID: {}", filename, storageId);
        }

        sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, storageId.getBytes(StandardCharsets.UTF_8));
    }

    private void handleDownloadLocRequest(ChannelHandlerContext ctx, Packet packet) {
        String storageId = new String(packet.getData(), StandardCharsets.UTF_8);
        
        // 尝试通过 storageId 获取 hash (利用反向索引)
        String hash = cacheManager.getHashByStorageId(storageId);
        
        // 如果没找到，可能 storageId 本身就是 hash (兼容旧客户端或特殊情况)
        if (hash == null) {
            hash = storageId;
        }

        MetadataCacheManager.MetadataEntry entry = cacheManager.get(hash);
        
        if (entry != null) {
            String response = entry.filename + "|" + entry.hash + "|" + entry.address;
            sendResponse(ctx, CommandType.NAMENODE_RESPONSE_DOWNLOAD_LOC, response.getBytes(StandardCharsets.UTF_8));
            return;
        }

        sendResponse(ctx, CommandType.ERROR, "文件不存在".getBytes(StandardCharsets.UTF_8));
    }

    private void sendResponse(ChannelHandlerContext ctx, CommandType type, byte[] data) {
        Packet response = new Packet();
        response.setCommandType(type);
        response.setData(data);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("NameNodeHandler异常", cause);
        ctx.close();
    }
}
