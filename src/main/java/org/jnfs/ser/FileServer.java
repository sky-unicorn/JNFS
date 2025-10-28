package org.jnfs.ser;// FileServer.java

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.jnfs.comm.FileTransferCodec;
import org.jnfs.comm.FileTransferConstants;

import java.io.File;

public class FileServer {
    public void start() throws Exception {
        // 创建上传目录
        File uploadDir = new File(FileTransferConstants.UPLOAD_DIR);
        if (!uploadDir.exists()) {
            uploadDir.mkdirs();
        }

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).option(ChannelOption.SO_BACKLOG, 1024).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                            // 编码器链：先加长度字段，再序列化对象
                            .addLast(new FileTransferCodec.LengthFieldEncoder()).addLast(new FileTransferCodec.FileEncoder())
                            // 解码器链：先按长度分割帧，再反序列化对象
                            .addLast(new FileTransferCodec.FrameDecoder()).addLast(new FileTransferCodec.FileDecoder())
                            // 业务处理器
                            .addLast(new FileServerHandler());
                }
            }).childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture future = bootstrap.bind(FileTransferConstants.PORT).sync();
            System.out.println("文件服务器启动，监听端口: " + FileTransferConstants.PORT);
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new FileServer().start();
    }
}
