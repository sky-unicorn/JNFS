package org.jnfs.common.replication;

/**
 * 同步任务状态枚举
 * <p>
 * PENDING(0)：待执行
 * IN_FLIGHT(1)：执行中
 * DONE(2)：已完成
 * FAILED(3)：失败（retry_count 达 4 次后告警）
 * <p>
 * 状态流转：PENDING → IN_FLIGHT → DONE；失败回 PENDING 且 retry_count++。
 */
public enum SyncTaskStatus {

    /** 待执行 */
    PENDING(0),
    /** 执行中 */
    IN_FLIGHT(1),
    /** 已完成 */
    DONE(2),
    /** 失败 */
    FAILED(3);

    private final int code;

    SyncTaskStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * 根据编码获取同步任务状态
     *
     * @param code 编码值
     * @return 对应的 SyncTaskStatus，未匹配则抛 IllegalArgumentException
     */
    public static SyncTaskStatus fromCode(int code) {
        for (SyncTaskStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown SyncTaskStatus code: " + code);
    }
}
