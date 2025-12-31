package org.jnfs.driver;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

/**
 * Netty ChannelPool 处理器
 * 负责连接的创建、配置和释放清理
 */
public class NameNodeChannelPoolHandler implements ChannelPoolHandler {

    @Override
    public void channelReleased(Channel ch) throws Exception {
        // 连接释放回池时，可以在这里做一些清理工作，比如 flush 缓冲区
        // 但对于长连接，通常不需要关闭
    }

    @Override
    public void channelAcquired(Channel ch) throws Exception {
        // 连接被取出时调用
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
        // 新连接创建时初始化 Pipeline
        SocketChannel socketChannel = (SocketChannel) ch;
        socketChannel.pipeline().addLast(new PacketDecoder());
        socketChannel.pipeline().addLast(new PacketEncoder());
        // 注意：具体的业务 Handler (如 SyncHandler) 不能在这里添加，
        // 因为 ChannelPool 是共享的，而 Handler 可能是有状态的。
        // 我们在获取连接后，动态 addLast，释放前 remove。
    }
}
