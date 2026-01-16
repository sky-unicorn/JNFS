package org.jnfs.namenode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 基于剩余空间的加权随机策略 (Weighted Random Strategy)
 * 节点被选中的概率与其剩余空间成正比。
 * 解决 "最大空间优先" 策略导致的单点性能瓶颈问题。
 */
public class WeightedRandomStrategy implements LoadBalancer {

    @Override
    public String select(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }

        long totalFreeSpace = 0;
        List<NodeWeight> weightedNodes = new ArrayList<>(nodes.size());

        // 1. 解析所有节点并计算总权重
        for (String nodeInfo : nodes) {
            String address;
            long freeSpace = 0;

            if (nodeInfo.contains("|")) {
                String[] parts = nodeInfo.split("\\|");
                address = parts[0];
                try {
                    freeSpace = Long.parseLong(parts[1]);
                    // 过滤掉空间为负数的异常情况，视为0
                    if (freeSpace < 0) {
                        freeSpace = 0;
                    }
                } catch (NumberFormatException e) {
                    // 解析失败视为0
                }
            } else {
                address = nodeInfo;
                // 没有空间信息，权重设为0
            }

            weightedNodes.add(new NodeWeight(address, freeSpace));
            totalFreeSpace += freeSpace;
        }

        // 2. 如果总空间为0（所有节点都满了或没有信息），退化为纯随机选择
        if (totalFreeSpace <= 0) {
            int randomIndex = ThreadLocalRandom.current().nextInt(nodes.size());
            String randomNode = nodes.get(randomIndex);
            return randomNode.split("\\|")[0];
        }

        // 3. 加权随机选择
        // 生成 [0, totalFreeSpace) 之间的随机数
        long randomValue = ThreadLocalRandom.current().nextLong(totalFreeSpace);
        long currentWeight = 0;

        for (NodeWeight node : weightedNodes) {
            currentWeight += node.weight;
            if (currentWeight > randomValue) {
                return node.address;
            }
        }

        // 理论上不会走到这里，除非并发修改或精度问题，作为兜底返回最后一个
        return weightedNodes.get(weightedNodes.size() - 1).address;
    }

    /**
     * 内部辅助类，保存解析后的信息
     */
    private static class NodeWeight {
        String address;
        long weight;

        NodeWeight(String address, long weight) {
            this.address = address;
            this.weight = weight;
        }
    }
}
