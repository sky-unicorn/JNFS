package org.jnfs.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class LeakTest {
    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025";

    public static void main(String[] args) throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                 }
             });

            ChannelFuture f = b.connect("localhost", 8080).sync();
            Channel channel = f.channel();

            String hash = "ABCD1234";
            byte[] nameBytes = hash.getBytes(StandardCharsets.UTF_8);

            ByteBuffer meta = ByteBuffer.allocate(8 + nameBytes.length);
            meta.putLong(1024 * 1024);
            meta.put(nameBytes);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(CLIENT_TOKEN);
            packet.setData(meta.array());

            channel.writeAndFlush(packet);

            // 发送一部分数据后立即强制断开，触发 channelInactive
            channel.writeAndFlush(Unpooled.buffer().writeBytes(new byte[64 * 1024]));
            channel.close();
            System.out.println("已模拟强制断开连接");

            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
