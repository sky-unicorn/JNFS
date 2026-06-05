package org.jnfs.driver;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

/**
 * 连接状态信息
 * 包含连接结果、详细信息以及各组件的可达性
 */
public class ConnectionStatus {

    private final ConnectionState state;
    private final String message;
    private final List<InetSocketAddress> reachableRegistries;
    private final List<InetSocketAddress> unreachableRegistries;
    private final List<InetSocketAddress> discoveredNameNodes;

    public ConnectionStatus(ConnectionState state,
                            String message,
                            List<InetSocketAddress> reachableRegistries,
                            List<InetSocketAddress> unreachableRegistries,
                            List<InetSocketAddress> discoveredNameNodes) {
        this.state = state;
        this.message = message;
        this.reachableRegistries = reachableRegistries != null
                ? Collections.unmodifiableList(reachableRegistries) : Collections.emptyList();
        this.unreachableRegistries = unreachableRegistries != null
                ? Collections.unmodifiableList(unreachableRegistries) : Collections.emptyList();
        this.discoveredNameNodes = discoveredNameNodes != null
                ? Collections.unmodifiableList(discoveredNameNodes) : Collections.emptyList();
    }

    /** 连接状态 */
    public ConnectionState getState() {
        return state;
    }

    /** 详细描述信息 */
    public String getMessage() {
        return message;
    }

    /** 可达的 Registry 地址列表 */
    public List<InetSocketAddress> getReachableRegistries() {
        return reachableRegistries;
    }

    /** 不可达的 Registry 地址列表 */
    public List<InetSocketAddress> getUnreachableRegistries() {
        return unreachableRegistries;
    }

    /** 发现的 NameNode 地址列表 */
    public List<InetSocketAddress> getDiscoveredNameNodes() {
        return discoveredNameNodes;
    }

    /** 是否连接成功（SUCCESS 或 PARTIAL_SUCCESS 均视为可用） */
    public boolean isOk() {
        return state == ConnectionState.SUCCESS || state == ConnectionState.PARTIAL_SUCCESS;
    }

    @Override
    public String toString() {
        return "ConnectionStatus{" +
                "state=" + state +
                ", message='" + message + '\'' +
                ", reachableRegistries=" + reachableRegistries +
                ", unreachableRegistries=" + unreachableRegistries +
                ", discoveredNameNodes=" + discoveredNameNodes +
                '}';
    }
}
