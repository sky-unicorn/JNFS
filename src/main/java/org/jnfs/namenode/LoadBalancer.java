package org.jnfs.namenode;

import java.util.List;

/**
 * 负载均衡策略接口
 */
public interface LoadBalancer {
    
    /**
     * 选择一个最佳的 DataNode
     * @param nodes 候选节点列表 (格式: address|freeSpace)
     * @return 选中的节点地址 (address)，如果没有合适节点返回 null
     */
    String select(List<String> nodes);
}
