package org.jnfs.common;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;

import java.io.IOException;

/**
 * 通用 Netty ChannelPoolHandler
 * 统一处理连接的创建、获取、释放逻辑
 *
 * 使用场景：
 * - NameNode 客户端连接池
 * - DataNode 客户端连接池
 * - Registry 客户端连接池
 * - 任何使用 FixedChannelPool 复用 Netty 连接的场景
 */
public class CommonChannelPoolHandler implements ChannelPoolHandler {

    /**
     * 连接获取时是否检查活跃状态
     * 默认为 true；Registry 内部连接复用时通常可设置为 false
     */
    private final boolean checkActiveOnAcquire;

    public CommonChannelPoolHandler() {
        this(true);
    }

    public CommonChannelPoolHandler(boolean checkActiveOnAcquire) {
        this.checkActiveOnAcquire = checkActiveOnAcquire;
    }

    @Override
    public void channelReleased(Channel ch) throws Exception {
        // 连接释放回池时，flush 缓冲区确保数据发送完毕
        ch.flush();
    }

    @Override
    public void channelAcquired(Channel ch) throws Exception {
        // 如果启用活跃检查，且连接已断开，抛出异常让连接池创建新连接
        if (checkActiveOnAcquire && !ch.isActive()) {
            throw new IOException("连接已断开，需要重新创建");
        }
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
        // 新连接创建时初始化 Pipeline
        SocketChannel socketChannel = (SocketChannel) ch;
        socketChannel.pipeline().addLast(new PacketDecoder());
        socketChannel.pipeline().addLast(new PacketEncoder());
        // 注意：具体的业务 Handler (如 SyncHandler) 不能在这里添加，
        // 因为 ChannelPool 是共享的，而 Handler 可能是有状态的。
        // 应在获取连接后，动态 addLast，释放前 remove。
    }
}
