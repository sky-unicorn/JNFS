package org.jnfs.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 节点地址解析器
 * 维护 node_id -> host:port 的本地缓存映射
 * 由 NameNode 从 Registry 同步更新
 *
 * 核心功能：
 * - 将 node_id 解析为当前 host:port
 * - 将 host:port 反查为 node_id
 * - 兼容旧数据（node_id 可能本身就是 host:port）
 */
public final class NodeAddressResolver {

    private static final Logger LOG = LoggerFactory.getLogger(NodeAddressResolver.class);

    // node_id -> host:port
    private static volatile Map<String, String> nodeIdToAddress = Collections.emptyMap();
    // host:port -> node_id (反向)
    private static volatile Map<String, String> addressToNodeId = Collections.emptyMap();

    private NodeAddressResolver() {
        // 工具类，禁止实例化
    }

    /**
     * 根据 node_id 获取当前 host:port
     * 如果 node_id 本身就是 host:port 格式（旧数据兼容），直接返回
     *
     * @param nodeIdOrAddress node_id 或 host:port
     * @return 当前 host:port，找不到时返回输入本身作为 fallback
     */
    public static String resolve(String nodeIdOrAddress) {
        if (nodeIdOrAddress == null) {
            return null;
        }
        // 如果已经是 host:port 格式，直接返回
        if (isHostPort(nodeIdOrAddress)) {
            return nodeIdOrAddress;
        }
        // 从缓存查找
        String addr = nodeIdToAddress.get(nodeIdOrAddress);
        if (addr != null) {
            return addr;
        }
        // fallback: 返回输入本身
        LOG.warn("无法解析 node_id: {}，使用原始值作为 fallback", nodeIdOrAddress);
        return nodeIdOrAddress;
    }

    /**
     * 根据 host:port 查找对应的 node_id
     * 如果缓存中找不到，返回 host:port 本身作为 fallback node_id
     *
     * @param address host:port 地址
     * @return node_id
     */
    public static String getNodeId(String address) {
        if (address == null) {
            return null;
        }
        String nodeId = addressToNodeId.get(address);
        if (nodeId != null) {
            return nodeId;
        }
        // fallback: 用地址本身作为 node_id
        return address;
    }

    /**
     * 全量更新映射（从 Registry 拉取 DataNode 列表后调用）
     * DataNode 列表格式: "node_id|host:port|freeSpace"
     *
     * @param nodeEntries 节点条目列表
     */
    public static void updateMappingFromDataNodes(List<String> nodeEntries) {
        if (nodeEntries == null || nodeEntries.isEmpty()) {
            nodeIdToAddress = Collections.emptyMap();
            addressToNodeId = Collections.emptyMap();
            return;
        }

        Map<String, String> newIdToAddr = new HashMap<>();
        Map<String, String> newAddrToId = new HashMap<>();

        for (String entry : nodeEntries) {
            String[] parts = entry.split("\\|");
            if (parts.length == 3) {
                // 新格式: node_id|host:port|freeSpace
                String nodeId = parts[0];
                String address = parts[1];
                newIdToAddr.put(nodeId, address);
                newAddrToId.put(address, nodeId);
            } else if (parts.length == 2) {
                // 旧格式兼容: host:port|freeSpace
                String address = parts[0];
                newIdToAddr.put(address, address);
                newAddrToId.put(address, address);
            }
        }

        nodeIdToAddress = Collections.unmodifiableMap(newIdToAddr);
        addressToNodeId = Collections.unmodifiableMap(newAddrToId);

        LOG.info("更新节点地址映射: {} 个节点", newIdToAddr.size());
    }

    /**
     * 判断字符串是否为 host:port 格式
     * 规则：包含冒号，冒号后为数字（端口）
     *
     * @param s 待判断字符串
     * @return true 如果是 host:port 格式
     */
    public static boolean isHostPort(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        int colonIndex = s.lastIndexOf(':');
        if (colonIndex <= 0 || colonIndex == s.length() - 1) {
            return false;
        }
        // 冒号后部分应为数字（端口）
        try {
            Integer.parseInt(s.substring(colonIndex + 1));
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
