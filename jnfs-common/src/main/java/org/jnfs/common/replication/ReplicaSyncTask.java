package org.jnfs.common.replication;

/**
 * 对账同步任务域对象。
 * <p>
 * NameNode 夜间对账同步器发现副本差集（实时写失败 / 历史遗留不一致）后落表的任务记录。
 * 持久化到 mysql {@code replica_sync_task} 表，NameNode 崩溃后可恢复（决策 10，解决 I6）。
 * <p>
 * 字段与 {@code replica_sync_task} 表一一对应：
 * <ul>
 *   <li>{@code status}：见 {@link SyncTaskStatus}（PENDING/IN_FLIGHT/DONE/FAILED）</li>
 *   <li>{@code retryCount}：累计失败次数，达 4 次告警；手动重试（决策 11）重置为 0</li>
 *   <li>{@code fileSize}：文件大小（字节），用于限速与超时计算</li>
 *   <li>{@code createTime / updateTime}：epoch 毫秒，DAO 负责 DATETIME ↔ long 转换</li>
 * </ul>
 */
public class ReplicaSyncTask {

    private String taskId;
    private String fileHash;
    private String sourceNode;
    private String targetNode;
    private int status;
    private int retryCount;
    private long fileSize;
    private long createTime;
    private long updateTime;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getFileHash() {
        return fileHash;
    }

    public void setFileHash(String fileHash) {
        this.fileHash = fileHash;
    }

    public String getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(String sourceNode) {
        this.sourceNode = sourceNode;
    }

    public String getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(String targetNode) {
        this.targetNode = targetNode;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
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
