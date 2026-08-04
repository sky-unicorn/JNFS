package org.jnfs.namenode;

import cn.hutool.cache.CacheUtil;
import cn.hutool.cache.impl.TimedCache;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.NettyHandlerHelper;
import org.jnfs.common.NodeAddressResolver;
import org.jnfs.common.Packet;
import org.jnfs.common.SegmentedLocks;
import org.jnfs.common.replication.ReplicaRole;
import org.jnfs.common.replication.ReplicationGroup;
import org.jnfs.common.replication.ReplicaStatus;
import org.jnfs.namenode.replication.ReplicationGroupStore;
import org.jnfs.namenode.replication.ReplicaSyncScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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

    // 冗余组配置缓存（mysql 模式专用，file 模式为 null）
    private static ReplicationGroupStore replicationGroupStore;

    // 夜间对账同步调度器（mysql 模式专用，file 模式为 null）
    // 用于 DATA_REPLICA_COMMIT 登记后 markDone(taskId) + 缓存失效
    private static ReplicaSyncScheduler replicaSyncScheduler;

    // 活跃的 DataNode 列表 (包含 freeSpace 信息)
    // 使用 volatile + Copy-On-Write 思想 (不可变快照) 解决并发读写问题
    private static volatile List<String> dataNodes = Collections.emptyList();

    // 负载均衡器
    private static final LoadBalancer loadBalancer = new WeightedRandomStrategy();

    // 分段锁 (使用通用工具类)
    private static final SegmentedLocks LOCKS = new SegmentedLocks(128);

    // File 模式: node_id 回填是否已执行过 (仅执行一次)
    private static final AtomicBoolean nodeIdBackfillDone = new AtomicBoolean(false);

    /**
     * 初始化元数据管理器 (由 NameNodeServer 启动时调用)
     */
    public static void initMetadataManager(MetadataManager manager, MetadataCacheManager cache) throws java.io.IOException {
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
                 // file 模式单副本：address 作为 primary 的 nodeId
                 List<ReplicaLocation> locs = Collections.singletonList(
                         new ReplicaLocation(address, ReplicaRole.PRIMARY.getCode(), ReplicaStatus.ACTIVE.getCode()));
                 cacheManager.putCacheOnly(hash, new MetadataCacheManager.MetadataEntry(
                     "loaded_from_file", hash, storageId, locs
                 ));
             }
        } else {
             LOG.info("MySQL模式: 跳过全量内存恢复，启用懒加载");
        }
    }

    public static void initDataNodes(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            dataNodes = Collections.emptyList();
        } else {
            // 生成新列表并设为不可变，替换引用 (Atomic Snapshot)
            List<String> snapshot = new ArrayList<>(nodes);
            dataNodes = Collections.unmodifiableList(snapshot);
        }

        // §4.9.4: File 模式下，首次从 Registry 拿到 DataNode 列表后，
        // 利用 host:port -> node_id 映射在线回填 namenode_meta.log
        if (metadataManager != null && nodes != null && !nodes.isEmpty()
                && nodeIdBackfillDone.compareAndSet(false, true)) {
            if (metadataManager instanceof MySQLMetadataManager) {
                // MySQL 模式: §4.9.2 在线补全 file_location.dentanode_id
                ((MySQLMetadataManager) metadataManager).backfillDataNodeIds();
            } else {
                // File 模式: §4.9.4 在线回填 namenode_meta.log 中的 host:port
                metadataManager.backfillNodeIds();
            }
        }
    }

    /**
     * 初始化冗余组配置缓存（由 NameNodeServer 启动时调用）。
     * mysql 模式传入 store 实例；file 模式传 null（冗余短路，走单副本）。
     */
    public static void initReplicationGroupStore(ReplicationGroupStore store) {
        replicationGroupStore = store;
        if (store != null) {
            LOG.info("NameNodeHandler: 冗余组缓存已注入（mysql 模式，多副本启用）");
        } else {
            LOG.info("NameNodeHandler: 冗余组缓存为 null（file 模式，单副本短路）");
        }
    }

    /**
     * 初始化对账同步调度器（由 NameNodeServer 启动时调用）。
     * mysql 模式传入 scheduler 实例；file 模式传 null（对账短路）。
     */
    public static void initReplicaSyncScheduler(ReplicaSyncScheduler scheduler) {
        replicaSyncScheduler = scheduler;
        if (scheduler != null) {
            LOG.info("NameNodeHandler: 对账同步调度器已注入（mysql 模式）");
        } else {
            LOG.info("NameNodeHandler: 对账同步调度器为 null（file 模式，对账短路）");
        }
    }

    /**
     * 选择副本目标节点（§6.1）。
     * <p>
     * 返回组内目标节点 node_id 列表（primary 恒首位）。
     * <ul>
     *   <li>mysql 模式：primary 由加权随机选出，若 primary 在冗余组内则返回组内全部成员（primary 恒首）</li>
     *   <li>file 模式（store==null）或 primary 不在任何组内：返回 [primary]（M3 降级单副本）</li>
     *   <li>无可用节点（dataNodes 为空 / selectNodeId 返回 null）：返回空列表</li>
     * </ul>
     *
     * @param fileHash 文件 hash（用于排除已持有该文件的节点；新文件可传 null）
     * @return 目标 node_id 列表（primary 恒首位），空列表表示无可用节点
     */
    private static List<String> selectReplicaTargets(String fileHash) {
        // file 模式 / 无 DataNode：直接返回单 primary 或空
        if (dataNodes.isEmpty()) {
            return Collections.emptyList();
        }

        // 构建候选 node_id 列表（排除已持有该文件的节点）
        Set<String> existingNodeIds = Collections.emptySet();
        if (fileHash != null && cacheManager != null) {
            MetadataCacheManager.MetadataEntry entry = cacheManager.get(fileHash);
            if (entry != null && !entry.locations.isEmpty()) {
                existingNodeIds = new HashSet<>();
                for (ReplicaLocation loc : entry.locations) {
                    existingNodeIds.add(loc.getNodeId());
                }
            }
        }

        // 过滤候选：排除已持有该文件的节点
        List<String> candidates = new ArrayList<>();
        for (String nodeInfo : dataNodes) {
            String[] parts = nodeInfo.split("\\|");
            String nodeId = parts[0]; // node_id 恒 parts[0]（nodeId|host|port 或 nodeId|host:port 均如此）
            if (!existingNodeIds.contains(nodeId)) {
                candidates.add(nodeInfo);
            }
        }

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // primary = 加权随机选出
        String primaryNodeId = loadBalancer.selectNodeId(candidates);
        if (primaryNodeId == null) {
            return Collections.emptyList();
        }

        // file 模式（store==null）或 primary 不在任何组内：返回 [primary]
        if (replicationGroupStore == null) {
            return Collections.singletonList(primaryNodeId);
        }

        ReplicationGroup group = replicationGroupStore.getGroupByNodeId(primaryNodeId);
        if (group == null) {
            return Collections.singletonList(primaryNodeId); // M3 降级单副本
        }

        // 组内全部成员，primary 恒首位
        List<String> members = group.getNodeIds();
        List<String> targets = new ArrayList<>();
        targets.add(primaryNodeId);
        for (String m : members) {
            if (!m.equals(primaryNodeId)) {
                targets.add(m);
            }
        }
        return targets;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Packet packet) throws Exception {
        if (!NettyHandlerHelper.validateToken(packet.getToken())) {
            LOG.warn("安全拦截: 无效的 Token - {}", ctx.channel().remoteAddress());
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "Authentication Failed: Invalid Token".getBytes(StandardCharsets.UTF_8));
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
                handleUploadLocRequest(ctx, packet);
                break;
            case NAMENODE_COMMIT_FILE:
                handleCommitFile(ctx, packet);
                break;
            case NAMENODE_REQUEST_DOWNLOAD_LOC:
                handleDownloadLocRequest(ctx, packet);
                break;
            case DATA_REPLICA_COMMIT:
                handleReplicaCommit(ctx, packet);
                break;
            default:
                NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "未知命令".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleCheckExistence(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);

        // 1. 查缓存/持久层
        MetadataCacheManager.MetadataEntry entry = cacheManager.get(hash);

        if (entry != null) {
            LOG.info("命中秒传: Hash={}", hash);
            // entry.getPrimaryNodeId() 返回 primary 的 node_id，需解析为 host:port 返回给客户端
            String hostPort = NodeAddressResolver.resolve(entry.getPrimaryNodeId());
            NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, hostPort.getBytes(StandardCharsets.UTF_8));
        } else {
            NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_NOT_EXIST, "Not Found".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handlePreUpload(ChannelHandlerContext ctx, Packet packet) {
        String hash = new String(packet.getData(), StandardCharsets.UTF_8);

        synchronized (LOCKS.getLock(hash)) {
            // 1. 查缓存/持久层
            MetadataCacheManager.MetadataEntry entry = cacheManager.get(hash);
            if (entry != null) {
                // entry.getPrimaryNodeId() 返回 primary 的 node_id，需解析为 host:port 返回给客户端
                String hostPort = NodeAddressResolver.resolve(entry.getPrimaryNodeId());
                NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_EXIST, hostPort.getBytes(StandardCharsets.UTF_8));
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
                    NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_WAIT, "Cluster-Waiting".getBytes(StandardCharsets.UTF_8));
                    return;
                }
            }

            if (pendingUploads.containsKey(hash)) {
                LOG.info("并发上传冲突，通知等待: Hash={}", hash);
                // 回滚分布式锁
                if (metadataManager != null) {
                    metadataManager.releaseUploadLock(hash);
                }
                NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_WAIT, "Waiting".getBytes(StandardCharsets.UTF_8));
                return;
            }

            pendingUploads.put(hash, true);
            LOG.info("允许上传: Hash={}", hash);
            NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_ALLOW, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * 处理上传位置请求（§6.1 + §15.3 破坏性变更）。
     * <p>
     * 从 packet data 解析 fileHash（容错：data 为空/null 时 hash=null，不排除节点）。
     * 调用 selectReplicaTargets 获取组内目标节点列表（primary 恒首位）。
     * 响应格式：{@code primary_host:port|sec1_host:port|sec2_host:port}（多段 | 分隔）。
     * <p>
     * 破坏性变更：响应从单 host:port 改为多段 | 分隔，Driver 须同步升级（§15.3）。
     */
    private void handleUploadLocRequest(ChannelHandlerContext ctx, Packet packet) {
        // 解析 fileHash（容错：空 data → hash=null，不排除任何节点）
        String fileHash = null;
        if (packet.getData() != null && packet.getData().length > 0) {
            fileHash = new String(packet.getData(), StandardCharsets.UTF_8);
            if (fileHash.isEmpty()) {
                fileHash = null;
            }
        }

        List<String> targets = selectReplicaTargets(fileHash);
        if (targets.isEmpty()) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "无可用 DataNode".getBytes(StandardCharsets.UTF_8));
            return;
        }

        // 构建 nodeId → host:port 映射（从 dataNodes 列表直接解析，避免 NodeAddressResolver 竞态）
        Map<String, String> nodeIdToAddr = buildNodeIdToAddressMap();

        // 拼接响应：primary|sec1|sec2（每个段为 host:port）
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < targets.size(); i++) {
            String nodeId = targets.get(i);
            String hostPort = nodeIdToAddr.get(nodeId);
            if (hostPort == null) {
                // 映射中无此 node_id（节点可能已下线但 store 缓存未刷新），跳过
                LOG.warn("handleUploadLocRequest: node_id {} 在当前 dataNodes 映射中不存在，跳过", nodeId);
                continue;
            }
            if (sb.length() > 0) {
                sb.append("|");
            }
            sb.append(hostPort);
        }

        if (sb.length() == 0) {
            // 所有 target 都无法解析地址
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "无可用 DataNode".getBytes(StandardCharsets.UTF_8));
            return;
        }

        NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_UPLOAD_LOC, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 从 dataNodes 列表构建 nodeId → host:port 映射。
     * 格式：{@code node_id|host:port|freeSpace} 或旧格式 {@code host:port|freeSpace}。
     * 旧格式下 node_id == host:port。
     */
    private static Map<String, String> buildNodeIdToAddressMap() {
        Map<String, String> map = new HashMap<>();
        for (String nodeInfo : dataNodes) {
            String[] parts = nodeInfo.split("\\|");
            if (parts.length == 3) {
                map.put(parts[0], parts[1]); // node_id → host:port
            } else if (parts.length == 2) {
                map.put(parts[0], parts[0]); // 旧格式：node_id == host:port
            }
        }
        return map;
    }

    /**
     * 处理文件提交（§6.3 + §15.3 破坏性变更）。
     * <p>
     * 新格式：{@code filename|hash|addr1,addr2,addr3}（addr 用 {@code ,} 分隔，外层 {@code |}）。
     * <ul>
     *   <li>首个 addr = PRIMARY（role=0, status=ACTIVE），其余 = SECONDARY（role=1, status=ACTIVE）</li>
     *   <li>replicationFactor 按 primary 所在冗余组大小快照（§5.1 + M6）；无组/file 模式 = 1</li>
     * </ul>
     * 破坏性变更：去掉了 {@code parts.length != 3} 校验，改为 {@code parts.length >= 3 && parts[2] 非空}。
     */
    private void handleCommitFile(ChannelHandlerContext ctx, Packet packet) {
        String data = new String(packet.getData(), StandardCharsets.UTF_8);
        String[] parts = data.split("\\|");
        if (parts.length < 3 || parts[2] == null || parts[2].isEmpty()) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "格式错误".getBytes(StandardCharsets.UTF_8));
            return;
        }

        String filename = parts[0];
        String hash = parts[1];
        // 解析成功的节点地址列表（addr 为 host:port，由 Driver 转换为 node_id 存储）
        // parts[2] 非空保证 addrs 至少含一个元素（",".split(",") 恒返回 ≥1 元素）
        String[] addrs = parts[2].split(",");

        // 每个 addr → nodeId；构造 locations（首个=PRIMARY，其余=SECONDARY，全部 ACTIVE）
        List<ReplicaLocation> locations = new ArrayList<>(addrs.length);
        String primaryNodeId = null;
        for (int i = 0; i < addrs.length; i++) {
            String addr = addrs[i].trim();
            if (addr.isEmpty()) {
                continue;
            }
            String nodeId = NodeAddressResolver.getNodeId(addr);
            int role = (primaryNodeId == null) ? ReplicaRole.PRIMARY.getCode() : ReplicaRole.SECONDARY.getCode();
            if (primaryNodeId == null) {
                primaryNodeId = nodeId;
            }
            locations.add(new ReplicaLocation(nodeId, role, ReplicaStatus.ACTIVE.getCode()));
        }

        if (locations.isEmpty() || primaryNodeId == null) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "格式错误".getBytes(StandardCharsets.UTF_8));
            return;
        }

        // replicationFactor 快照：primary 所在冗余组大小；无组/file 模式 = 1（§5.1 + M6）
        int replicationFactor = 1;
        if (replicationGroupStore != null) {
            ReplicationGroup group = replicationGroupStore.getGroupByNodeId(primaryNodeId);
            if (group != null) {
                replicationFactor = group.getNodeIds().size();
            }
        }

        String storageId;

        // 1. 快速检查：如果已存在，直接返回
        MetadataCacheManager.MetadataEntry existing = cacheManager.get(hash);
        if (existing != null) {
             LOG.info("忽略重复元数据提交 (已存在): {}", filename);
             NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, existing.storageId.getBytes(StandardCharsets.UTF_8));
             return;
        }

        synchronized (LOCKS.getLock(hash)) {
            // 双重检查
            existing = cacheManager.get(hash);
            if (existing != null) {
                 NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, existing.storageId.getBytes(StandardCharsets.UTF_8));
                 return;
            }

            pendingUploads.remove(hash);
            storageId = UUID.randomUUID().toString();

            // 持久化到 MySQL 或 文件，并更新缓存（存储 node_id 而非 host:port）
            try {
                if (cacheManager != null) {
                    cacheManager.put(filename, hash, storageId, replicationFactor, locations);
                }
            } catch (Exception e) {
                LOG.error("元数据提交失败: {}", filename, e);
                // 提交失败必须释放分布式锁，否则用户需等待 30 分钟
                if (metadataManager != null) {
                    metadataManager.releaseUploadLock(hash);
                }
                NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "Metadata Persistence Failed".getBytes(StandardCharsets.UTF_8));
                return;
            }

            LOG.info("文件已注册并持久化: {}, ID: {}, 副本数: {}/{}", filename, storageId, locations.size(), replicationFactor);
        }

        NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT, storageId.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理下载位置请求（§8.1 + §15.3 破坏性变更）。
     * <p>
     * 返回有序副本列表：{@code filename|hash|primary|replica1|replica2}。
     * <ul>
     *   <li>从 entry.locations 过滤 status=ACTIVE 的副本（§8.1：CORRUPT/未就绪不返回）</li>
     *   <li>entry.locations 已按 role ASC, status DESC 排序，primary（ACTIVE）恒首位</li>
     *   <li>每个 nodeId 解析为 host:port（优先 dataNodes 映射，fallback NodeAddressResolver）</li>
     * </ul>
     * 只有 primary ACTIVE 时返回 {@code filename|hash|primary}。entry 为 null → ERROR。
     */
    private void handleDownloadLocRequest(ChannelHandlerContext ctx, Packet packet) {
        String storageId = new String(packet.getData(), StandardCharsets.UTF_8);

        // 尝试通过 storageId 获取 hash (利用反向索引)
        String hash = cacheManager.getHashByStorageId(storageId);

        // 如果没找到，可能 storageId 本身就是 hash (兼容旧客户端或特殊情况)
        if (hash == null) {
            hash = storageId;
        }

        MetadataCacheManager.MetadataEntry entry = cacheManager.get(hash);

        if (entry == null) {
            NettyHandlerHelper.sendResponse(ctx, CommandType.ERROR, "文件不存在".getBytes(StandardCharsets.UTF_8));
            return;
        }

        // 构建 nodeId → host:port 映射（优先 dataNodes 直查，避免 NodeAddressResolver 竞态）
        Map<String, String> nodeIdToAddr = buildNodeIdToAddressMap();

        StringBuilder sb = new StringBuilder();
        sb.append(entry.filename).append("|").append(entry.hash);

        // entry.locations 已按 role ASC/status DESC 排序；只返回 ACTIVE 副本
        for (ReplicaLocation loc : entry.locations) {
            if (!loc.isActive()) {
                continue; // §8.1：CORRUPT/未就绪副本不返回
            }
            String hostPort = nodeIdToAddr.get(loc.getNodeId());
            if (hostPort == null) {
                // dataNodes 映射缺失（节点临时不在列表），fallback 到 NodeAddressResolver
                // 注意：NodeAddressResolver 可能返回过时地址（节点已下线但缓存未更新），
                // Driver 侧故障转移兜底——连接失败后自动切换下一副本
                hostPort = NodeAddressResolver.resolve(loc.getNodeId());
            }
            // 防御：resolve fallback 返回原始 nodeId（UUID 格式，不是 host:port），
            // 说明该副本的节点已下线且无法解析地址，跳过此副本避免 Driver 解析越界。
            if (hostPort != null && !NodeAddressResolver.isHostPort(hostPort)) {
                LOG.warn("handleDownloadLocRequest: 跳过无法解析地址的副本 nodeId={}", loc.getNodeId());
                continue;
            }
            if (hostPort != null) {
                sb.append("|").append(hostPort);
            }
        }

        NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_DOWNLOAD_LOC, sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 处理副本提交（§7.4 DATA_REPLICA_COMMIT：目标 DataNode 拉取完成后登记 ACTIVE）。
     * <p>
     * payload = {@code fileHash|nodeId}（Phase 4 已定）。解析两段。
     * <p>
     * 登记 file_location 行：{@code INSERT ... ON DUPLICATE KEY UPDATE status=1}（§7.5 幂等）。
     * 对账补齐的副本 role=1 SECONDARY，status=1 ACTIVE。
     * <p>
     * 缓存一致性：登记后调用 {@link MetadataCacheManager#invalidate(String)} 失效该 hash 缓存，
     * 下次 get() 从持久层重新加载（含新副本行）。
     * <p>
     * 同时将对账任务标记为 DONE（scheduler.markDone）。
     * <p>
     * file 模式（scheduler==null）不处理此命令（理论上不会收到）。
     */
    private void handleReplicaCommit(ChannelHandlerContext ctx, Packet packet) {
        String data = new String(packet.getData(), StandardCharsets.UTF_8);
        String[] parts = data.split("\\|");
        if (parts.length < 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            NettyHandlerHelper.sendError(ctx, "非法的 DATA_REPLICA_COMMIT 格式");
            return;
        }

        String fileHash = parts[0];
        String nodeId = parts[1];

        // file 模式不处理（理论上不会收到，但安全起见）
        if (!(metadataManager instanceof MySQLMetadataManager)) {
            NettyHandlerHelper.sendError(ctx, "file 模式不支持副本提交");
            return;
        }

        try {
            // 登记 file_location 行（INSERT ON DUPLICATE KEY UPDATE，§7.5 幂等）
            // 对账补齐的副本 role=1 SECONDARY，status=1 ACTIVE
            insertReplicaLocation(fileHash, nodeId);

            // 缓存失效：下次 get() 从持久层重新加载含新副本的 entry
            if (cacheManager != null) {
                cacheManager.invalidate(fileHash);
            }

            // 对账任务标记为 DONE
            if (replicaSyncScheduler != null) {
                markTaskDoneByHashAndTarget(fileHash, nodeId);
            }

            LOG.info("副本提交登记成功: hash={}, nodeId={}", fileHash, nodeId);
            // 回 ACK（Phase 4 CommitResponseHandler 只判非 ERROR，任一非 ERROR 包即可）
            NettyHandlerHelper.sendResponse(ctx, CommandType.NAMENODE_RESPONSE_COMMIT,
                    "OK".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            LOG.error("副本提交登记失败: hash={}, nodeId={}", fileHash, nodeId, e);
            NettyHandlerHelper.sendError(ctx, "副本提交登记失败: " + e.getMessage());
        }
    }

    /**
     * INSERT file_location 行（ON DUPLICATE KEY UPDATE status=1, replica_role=1）。
     * <p>
     * §7.5 幂等：uk_hash_node 保证同一 (file_hash, datanode_id) 唯一；
     * 重复执行仅更新 status=1（已 ACTIVE 的行无副作用）。
     */
    private void insertReplicaLocation(String fileHash, String nodeId) throws Exception {
        // 必须是 MySQL 模式（调用方已检查）
        MySQLMetadataManager mysqlMgr = (MySQLMetadataManager) metadataManager;
        javax.sql.DataSource ds = mysqlMgr.getDataSource();

        String sql = "INSERT INTO file_location (file_hash, datanode_id, status, replica_role, create_time)" +
                " VALUES (?, ?, 1, 1, NOW())" +
                " ON DUPLICATE KEY UPDATE status = 1, replica_role = 1";
        try (java.sql.Connection conn = ds.getConnection();
             java.sql.PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, fileHash);
            stmt.setString(2, nodeId);
            stmt.executeUpdate();
        }
    }

    /**
     * 根据 (fileHash, targetNode) 查找对账任务并标记为 DONE。
     * <p>
     * DATA_REPLICA_COMMIT 不带 taskId，通过 (fileHash, nodeId) 反查任务。
     */
    private void markTaskDoneByHashAndTarget(String fileHash, String nodeId) {
        try {
            // 查找该 (fileHash, targetNode) 对应的 IN_FLIGHT 任务
            org.jnfs.namenode.replication.ReplicaSyncTaskStore taskStore =
                    replicaSyncScheduler.getTaskStore();
            java.util.List<org.jnfs.common.replication.ReplicaSyncTask> pending =
                    taskStore.findPending();
            for (org.jnfs.common.replication.ReplicaSyncTask task : pending) {
                if (fileHash.equals(task.getFileHash()) && nodeId.equals(task.getTargetNode())) {
                    taskStore.markDone(task.getTaskId());
                    LOG.info("对账任务标记 DONE: taskId={}, hash={}, target={}",
                            task.getTaskId(), fileHash, nodeId);
                    return;
                }
            }
            // 未找到 IN_FLIGHT 任务可能是已完成或不在任务表中，记录日志即可
            LOG.debug("未找到 hash={}, target={} 的 IN_FLIGHT 对账任务", fileHash, nodeId);
        } catch (Exception e) {
            LOG.warn("标记对账任务 DONE 失败: hash={}, target={}", fileHash, nodeId, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("NameNodeHandler异常", cause);
        ctx.close();
    }
}
