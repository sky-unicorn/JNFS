package org.jnfs.common.replication;

/**
 * 副本状态枚举
 * <p>
 * ACTIVE(1)：副本已就位（实时写成功或对账补齐），可对外服务。
 * CORRUPT(0)：副本损坏/丢失，不可读。
 */
public enum ReplicaStatus {

    /** 副本损坏/丢失 */
    CORRUPT(0),
    /** 副本已就位，可对外服务 */
    ACTIVE(1);

    private final int code;

    ReplicaStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * 根据编码获取副本状态
     *
     * @param code 编码值
     * @return 对应的 ReplicaStatus，未匹配则抛 IllegalArgumentException
     */
    public static ReplicaStatus fromCode(int code) {
        for (ReplicaStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown ReplicaStatus code: " + code);
    }
}
