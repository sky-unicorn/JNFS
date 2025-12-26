package org.jnfs.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

public class SecurityTest {

    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025";

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                     ch.pipeline().addLast(new SimpleChannelInboundHandler<Packet>() {
                         @Override
                         protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
                             if (msg.getCommandType() == CommandType.ERROR) {
                                 System.out.println("收到错误响应: " + new String(msg.getData()));
                             } else {
                                 System.out.println("收到响应: " + msg.getCommandType());
                             }
                             ctx.close();
                         }
                     });
                 }
             });

            ChannelFuture f = b.connect("localhost", 8080).sync();
            Channel channel = f.channel();

            // 构造恶意文件名
            String maliciousName = "../../../malicious_file.txt";
            byte[] nameBytes = maliciousName.getBytes(StandardCharsets.UTF_8);
            
            // 构造上传包
            ByteBuffer metadataBuffer = ByteBuffer.allocate(8 + nameBytes.length);
            metadataBuffer.putLong(10); // file size
            metadataBuffer.put(nameBytes);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(CLIENT_TOKEN);
            packet.setData(metadataBuffer.array());

            System.out.println("发送恶意上传请求: " + maliciousName);
            channel.write(packet);
            channel.writeAndFlush(Unpooled.wrappedBuffer("HACKED".getBytes()));

            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
