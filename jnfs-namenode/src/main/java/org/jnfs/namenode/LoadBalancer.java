package org.jnfs.namenode;

import java.util.List;

/**
 * 负载均衡策略接口
 */
public interface LoadBalancer {

    /**
     * 选择一个最佳的 DataNode
     *
     * @param nodes 候选节点列表
     *              新格式: node_id|host:port|freeSpace
     *              旧格式: host:port|freeSpace
     * @return 选中的节点地址 (host:port)，如果没有合适节点返回 null
     */
    String select(List<String> nodes);

    /**
     * 选择一个最佳的 DataNode 并返回其 node_id（多副本冗余场景使用）。
     * <p>
     * 节点选择结果与 {@link #select(List)} 一致（同一加权随机策略），区别仅在返回字段：
     * 本方法返回 node_id（{@@code parts[0]}），便于上层按 node_id 组装冗余组副本目标。
     *
     * @param nodes 候选节点列表，格式 {@code node_id|host:port|freeSpace}
     *              （旧格式 {@code host:port|freeSpace} 视为 node_id==host:port）
     * @return 选中的 node_id，如果没有合适节点返回 null
     */
    String selectNodeId(List<String> nodes);
}
