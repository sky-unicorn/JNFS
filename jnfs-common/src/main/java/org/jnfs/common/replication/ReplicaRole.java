package org.jnfs.common.replication;

/**
 * 副本角色枚举
 * <p>
 * PRIMARY(0)：文件的主副本节点，写入口、读首选、夜间对账的源。每文件唯一。
 * SECONDARY(1)：文件的次副本节点，只读，实时写就位后可读。
 */
public enum ReplicaRole {

    /** 主副本 */
    PRIMARY(0),
    /** 次副本 */
    SECONDARY(1);

    private final int code;

    ReplicaRole(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * 根据编码获取副本角色
     *
     * @param code 编码值
     * @return 对应的 ReplicaRole，未匹配则抛 IllegalArgumentException
     */
    public static ReplicaRole fromCode(int code) {
        for (ReplicaRole role : values()) {
            if (role.code == code) {
                return role;
            }
        }
        throw new IllegalArgumentException("Unknown ReplicaRole code: " + code);
    }
}
