package org.jnfs.namenode;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 最大剩余空间优先策略 (Max Free Space Strategy)
 * 优先选择剩余空间最大的节点，如果空间相同则随机
 */
public class MaxFreeSpaceStrategy implements LoadBalancer {

    @Override
    public String select(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return null;
        }

        String bestNode = null;
        long maxFreeSpace = -1;

        // 遍历所有节点，解析 "address|freeSpace"
        for (String nodeInfo : nodes) {
            String address;
            long freeSpace = 0;

            if (nodeInfo.contains("|")) {
                String[] parts = nodeInfo.split("\\|");
                address = parts[0];
                try {
                    freeSpace = Long.parseLong(parts[1]);
                } catch (NumberFormatException e) {
                    // ignore
                }
            } else {
                address = nodeInfo;
                // 兼容旧格式，没有 freeSpace 信息则视为 0 或随机处理
                // 这里暂且视为 0，优先选有信息的
            }

            if (freeSpace > maxFreeSpace) {
                maxFreeSpace = freeSpace;
                bestNode = address;
            } else if (freeSpace == maxFreeSpace) {
                // 空间相同，随机替换，避免总是打到同一个
                if (ThreadLocalRandom.current().nextBoolean()) {
                    bestNode = address;
                }
            }
        }
        
        // 如果所有节点都没有 freeSpace 信息 (旧版本兼容)，则随机选一个
        if (maxFreeSpace <= 0 && bestNode != null) {
             String randomInfo = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
             return randomInfo.split("\\|")[0];
        }

        return bestNode;
    }
}
