package org.jnfs.common;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

/**
 * Netty ChannelPool 工具类
 * 提供连接池的标准创建逻辑
 *
 * 使用场景：
 * - NameNode / DataNode 与 Registry 之间的高频心跳
 * - 客户端 SDK 与 NameNode 之间的请求通道
 */
public final class ChannelPoolUtils {

    /**
     * 默认每个地址最大连接数
     */
    public static final int DEFAULT_MAX_CONNECTIONS_PER_KEY = 10;

    /**
     * 默认连接超时：5000ms
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

    private ChannelPoolUtils() {
        // 工具类，禁止实例化
    }

    /**
     * 创建使用默认 handler (CommonChannelPoolHandler) 的连接池 Map
     *
     * @param workerGroup 共享的 EventLoopGroup
     * @return ChannelPoolMap，Key 为目标地址
     */
    public static ChannelPoolMap<InetSocketAddress, SimpleChannelPool> createDefaultPoolMap(EventLoopGroup workerGroup) {
        return createDefaultPoolMap(workerGroup, DEFAULT_MAX_CONNECTIONS_PER_KEY, DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    /**
     * 创建使用默认 handler 的连接池 Map，指定每 Key 最大连接数
     */
    public static ChannelPoolMap<InetSocketAddress, SimpleChannelPool> createDefaultPoolMap(
            EventLoopGroup workerGroup, int maxConnectionsPerKey) {
        return createDefaultPoolMap(workerGroup, maxConnectionsPerKey, DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    /**
     * 创建使用默认 handler 的连接池 Map，指定每 Key 最大连接数和连接超时
     */
    public static ChannelPoolMap<InetSocketAddress, SimpleChannelPool> createDefaultPoolMap(
            EventLoopGroup workerGroup, int maxConnectionsPerKey, int connectTimeoutMillis) {
        return createPoolMap(workerGroup, new CommonChannelPoolHandler(), maxConnectionsPerKey, connectTimeoutMillis);
    }

    /**
     * 创建自定义 handler 的连接池 Map
     *
     * @param workerGroup 共享的 EventLoopGroup
     * @param handler     自定义 ChannelPoolHandler
     */
    public static ChannelPoolMap<InetSocketAddress, SimpleChannelPool> createPoolMap(
            EventLoopGroup workerGroup,
            io.netty.channel.pool.ChannelPoolHandler handler) {
        return createPoolMap(workerGroup, handler, DEFAULT_MAX_CONNECTIONS_PER_KEY, DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    /**
     * 创建自定义 handler 的连接池 Map，指定每 Key 最大连接数
     */
    public static ChannelPoolMap<InetSocketAddress, SimpleChannelPool> createPoolMap(
            EventLoopGroup workerGroup,
            io.netty.channel.pool.ChannelPoolHandler handler,
            int maxConnectionsPerKey) {
        return createPoolMap(workerGroup, handler, maxConnectionsPerKey, DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    /**
     * 创建自定义 handler 的连接池 Map，指定每 Key 最大连接数和连接超时
     */
    public static ChannelPoolMap<InetSocketAddress, SimpleChannelPool> createPoolMap(
            EventLoopGroup workerGroup,
            io.netty.channel.pool.ChannelPoolHandler handler,
            int maxConnectionsPerKey,
            int connectTimeoutMillis) {
        return new AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
            @Override
            protected SimpleChannelPool newPool(InetSocketAddress key) {
                Bootstrap b = new Bootstrap()
                        .group(workerGroup)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true);
                return new FixedChannelPool(b.remoteAddress(key), handler, maxConnectionsPerKey);
            }
        };
    }
}
