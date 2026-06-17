package org.jnfs.namenode;

import java.util.List;

/**
 * 负载均衡策略接口
 */
public interface LoadBalancer {

    /**
     * 选择一个最佳的 DataNode
     * @param nodes 候选节点列表
     *              新格式: node_id|host:port|freeSpace
     *              旧格式: host:port|freeSpace
     * @return 选中的节点地址 (host:port)，如果没有合适节点返回 null
     */
    String select(List<String> nodes);
}
