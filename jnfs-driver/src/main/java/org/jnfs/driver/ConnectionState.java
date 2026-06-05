package org.jnfs.driver;

/**
 * 连接状态枚举
 * 表示 JNFSDriver 与 Registry / NameNode 的连接结果
 */
public enum ConnectionState {

    /** 所有 Registry 和 NameNode 连接正常 */
    SUCCESS("所有连接正常"),

    /** 部分 Registry 不可达，但至少有一个可用且获取到了 NameNode */
    PARTIAL_SUCCESS("部分 Registry 不可达"),

    /** 所有 Registry 均不可达 */
    REGISTRY_UNREACHABLE("所有 Registry 不可达"),

    /** Registry 可达但未发现可用的 NameNode */
    NO_NAMENODE("未发现可用的 NameNode"),

    /** 认证 Token 无效 */
    TOKEN_INVALID("认证 Token 无效"),

    /** 连接超时 */
    TIMEOUT("连接超时");

    private final String description;

    ConnectionState(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
