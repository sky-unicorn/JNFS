package org.jnfs.common.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 冗余组（Replication Group）域对象。
 * <p>
 * 2~3 个 DataNode 组成的集合，组内节点互备。副本数 = 组内节点数。
 * 配置持久化到 mysql {@code replication_group} 表（仅 mysql 集群模式启用）。
 * <p>
 * 字段与 {@code replication_group} 表一一对应：
 * <ul>
 *   <li>{@code nodeIds}：组成员 node_id 列表（2~3 个），DB 中以逗号分隔字符串存储</li>
 *   <li>{@code createTime / updateTime}：epoch 毫秒，DAO 负责 DATETIME ↔ long 转换</li>
 * </ul>
 * 注意：组成员列表不携带 role（PRIMARY/SECONDARY 是文件级副本角色，不存在节点级）。
 */
public class ReplicationGroup {

    private String groupId;
    private String groupName;
    private List<String> nodeIds;
    private long createTime;
    private long updateTime;

    public ReplicationGroup() {
        this.nodeIds = new ArrayList<>();
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * 返回组成员 node_id 列表（不可变视图，防止外部误改）。
     */
    public List<String> getNodeIds() {
        return Collections.unmodifiableList(nodeIds);
    }

    public void setNodeIds(List<String> nodeIds) {
        this.nodeIds = (nodeIds == null) ? new ArrayList<>() : new ArrayList<>(nodeIds);
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }
}
