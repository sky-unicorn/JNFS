package org.jnfs.namenode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 基于剩余空间的加权随机策略 (Weighted Random Strategy)
 * 节点被选中的概率与其剩余空间成正比。
 * 解决 "最大空间优先" 策略导致的单点性能瓶颈问题。
 *
 * 支持节点列表格式：
 * - 新格式: node_id|host:port|freeSpace
 * - 旧格式: host:port|freeSpace
 *
 * {@link #select} 返回 host:port；{@link #selectNodeId} 返回 node_id。
 * 两者复用同一加权选择内核（{@link #selectWeighted}），避免逻辑重复。
 */
public class WeightedRandomStrategy implements LoadBalancer {

    @Override
    public String select(List<String> nodes) {
        NodeInfo info = selectWeighted(nodes);
        return info == null ? null : info.address;
    }

    @Override
    public String selectNodeId(List<String> nodes) {
        NodeInfo info = selectWeighted(nodes);
        return info == null ? null : info.nodeId;
    }

    /**
     * 加权随机选择内核：解析候选、按 freeSpace 加权随机选中一个，返回解析后的节点信息。
     * <p>
     * 总空间为 0（无空间信息）时退化为纯随机。空列表返回 null。
     */
    private NodeInfo selectWeighted(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }

        long totalFreeSpace = 0;
        List<NodeInfo> weightedNodes = new ArrayList<>(nodes.size());

        // 1. 解析所有节点并计算总权重
        for (String nodeInfo : nodes) {
            String address;
            String nodeId;
            long freeSpace = 0;

            if (nodeInfo.contains("|")) {
                String[] parts = nodeInfo.split("\\|");
                if (parts.length == 3) {
                    // 新格式: node_id|host:port|freeSpace
                    nodeId = parts[0];
                    address = parts[1];
                    freeSpace = parseFreeSpace(parts[2]);
                } else {
                    // 旧格式: host:port|freeSpace
                    address = parts[0];
                    nodeId = parts[0]; // node_id 缺失，视为 == host:port
                    freeSpace = parseFreeSpace(parts[1]);
                }
            } else {
                address = nodeInfo;
                nodeId = nodeInfo;
                // 没有空间信息，权重设为0
            }

            weightedNodes.add(new NodeInfo(address, nodeId, freeSpace));
            totalFreeSpace += freeSpace;
        }

        // 2. 总空间为 0（所有节点都满了或无信息），退化为纯随机
        if (totalFreeSpace <= 0) {
            int randomIndex = ThreadLocalRandom.current().nextInt(nodes.size());
            return weightedNodes.get(randomIndex);
        }

        // 3. 加权随机选择：生成 [0, totalFreeSpace) 之间的随机数
        long randomValue = ThreadLocalRandom.current().nextLong(totalFreeSpace);
        long currentWeight = 0;
        for (NodeInfo node : weightedNodes) {
            currentWeight += node.weight;
            if (currentWeight > randomValue) {
                return node;
            }
        }

        // 兜底：并发修改或精度问题，返回最后一个
        return weightedNodes.get(weightedNodes.size() - 1);
    }

    private long parseFreeSpace(String s) {
        try {
            long v = Long.parseLong(s);
            return v < 0 ? 0 : v;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * 内部辅助类：保存解析后的节点信息（address、nodeId、weight）
     */
    private static class NodeInfo {
        final String address;  // host:port
        final String nodeId;   // node_id（旧格式下 == address）
        final long weight;     // freeSpace

        NodeInfo(String address, String nodeId, long weight) {
            this.address = address;
            this.nodeId = nodeId;
            this.weight = weight;
        }
    }
}
