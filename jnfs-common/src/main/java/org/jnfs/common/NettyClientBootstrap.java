package org.jnfs.common;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.TimeUnit;

/**
 * Netty 客户端 Bootstrap 工具类
 * 统一客户端连接创建逻辑，避免各模块重复样板代码
 *
 * 使用场景：
 * - 客户端 SDK 的一次性连接 (Registry 探测、DataNode 上传/下载)
 * - 测试 / 调试工具的临时连接
 */
public final class NettyClientBootstrap {

    /**
     * 默认连接超时：5000ms
     */
    public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 5000;

    private NettyClientBootstrap() {
        // 工具类，禁止实例化
    }

    /**
     * 创建带标准协议编解码器的 Bootstrap
     *
     * @param group EventLoopGroup
     * @return 可继续链式调用 handler() 配置业务 Handler 的 Bootstrap
     */
    public static Bootstrap create(EventLoopGroup group) {
        return create(group, DEFAULT_CONNECT_TIMEOUT_MILLIS);
    }

    /**
     * 创建带标准协议编解码器的 Bootstrap，指定连接超时
     */
    public static Bootstrap create(EventLoopGroup group, int connectTimeoutMillis) {
        return new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
                .option(ChannelOption.TCP_NODELAY, true);
    }

    /**
     * 创建带指定业务 Handler 的 Bootstrap
     * 内部使用 ChannelInitializer 自动添加 PacketDecoder / PacketEncoder
     */
    public static Bootstrap createWithHandler(EventLoopGroup group, ChannelHandler... businessHandlers) {
        return createWithHandler(group, DEFAULT_CONNECT_TIMEOUT_MILLIS, businessHandlers);
    }

    /**
     * 创建带指定业务 Handler 的 Bootstrap，指定连接超时
     */
    public static Bootstrap createWithHandler(EventLoopGroup group, int connectTimeoutMillis, ChannelHandler... businessHandlers) {
        Bootstrap b = create(group, connectTimeoutMillis);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                ch.pipeline().addLast(new PacketDecoder());
                ch.pipeline().addLast(new PacketEncoder());
                if (businessHandlers != null) {
                    for (ChannelHandler h : businessHandlers) {
                        ch.pipeline().addLast(h);
                    }
                }
            }
        });
        return b;
    }

    /**
     * 同步连接到远端地址，附带默认超时等待
     *
     * @param b     Bootstrap
     * @param host  远端主机
     * @param port  远端端口
     * @param awaitMillis 等待连接成功的最大毫秒数 (例如 6000)
     * @return 已成功连接的 Channel
     * @throws Exception 连接失败抛出
     */
    public static Channel connectSync(Bootstrap b, String host, int port, long awaitMillis) throws Exception {
        ChannelFuture f = b.connect(host, port);
        boolean connected = f.awaitUninterruptibly(awaitMillis, TimeUnit.MILLISECONDS);
        if (!connected || !f.isSuccess()) {
            String reason = f.cause() != null ? f.cause().getMessage() : "连接超时";
            throw new java.io.IOException("连接失败 (" + host + ":" + port + "): " + reason);
        }
        return f.channel();
    }
}
