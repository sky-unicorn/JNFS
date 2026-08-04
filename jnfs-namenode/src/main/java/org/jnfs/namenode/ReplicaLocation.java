package org.jnfs.namenode;

import org.jnfs.common.replication.ReplicaRole;
import org.jnfs.common.replication.ReplicaStatus;

/**
 * 副本位置值对象：单个副本的节点定位与角色/状态。
 * <p>
 * 字段语义：
 * <ul>
 *   <li>{@code nodeId}：存储 node_id（由 {@link org.jnfs.common.NodeAddressResolver} 解析为 host:port），
 *       兼容旧数据时可能为 host:port 字符串</li>
 *   <li>{@code role}：副本角色，取 {@link ReplicaRole#getCode()}（0=PRIMARY, 1=SECONDARY）</li>
 *   <li>{@code status}：副本状态，取 {@link ReplicaStatus#getCode()}（1=ACTIVE, 0=CORRUPT）</li>
 * </ul>
 * <p>
 * 不可变值对象。排序规则（与 {@link MetadataCacheManager.MetadataEntry} 一致）：
 * role ASC（PRIMARY 优先），status DESC（ACTIVE 优先）。
 */
public final class ReplicaLocation {

    private final String nodeId;
    private final int role;
    private final int status;

    /**
     * @param nodeId 节点标识（node_id）
     * @param role   副本角色（{@link ReplicaRole#getCode()}）
     * @param status 副本状态（{@link ReplicaStatus#getCode()}）
     */
    public ReplicaLocation(String nodeId, int role, int status) {
        this.nodeId = nodeId;
        this.role = role;
        this.status = status;
    }

    /**
     * 便利构造：默认 status=ACTIVE（用于上传链路新登记的副本）。
     *
     * @param nodeId 节点标识（node_id）
     * @param role   副本角色（{@link ReplicaRole#getCode()}）
     */
    public ReplicaLocation(String nodeId, int role) {
        this(nodeId, role, ReplicaStatus.ACTIVE.getCode());
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getRole() {
        return role;
    }

    public int getStatus() {
        return status;
    }

    /** 是否主副本（role=PRIMARY） */
    public boolean isPrimary() {
        return role == ReplicaRole.PRIMARY.getCode();
    }

    /** 副本是否已就位可读（status=ACTIVE） */
    public boolean isActive() {
        return status == ReplicaStatus.ACTIVE.getCode();
    }

    @Override
    public String toString() {
        return "ReplicaLocation{nodeId='" + nodeId + "', role=" + role + ", status=" + status + "}";
    }
}
