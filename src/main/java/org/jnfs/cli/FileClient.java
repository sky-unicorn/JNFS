package org.jnfs.cli;// FileClient.java

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.comm.FileTransferCodec;
import org.jnfs.comm.FileTransferConstants;

public class FileClient {
    private final String host;
    private final int port;
    private final String filePath;

    public FileClient(String host, int port, String filePath) {
        this.host = host;
        this.port = port;
        this.filePath = filePath;
    }

    public void start() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline()
                            // 编码器链
                            .addLast(new FileTransferCodec.LengthFieldEncoder()).addLast(new FileTransferCodec.FileEncoder())
                            // 解码器链
                            .addLast(new FileTransferCodec.FrameDecoder()).addLast(new FileTransferCodec.FileDecoder())
                            // 业务处理器
                            .addLast(new FileClientHandler(filePath));
                }
            });

            ChannelFuture future = bootstrap.connect(host, port).sync();
            future.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("用法: java FileClient <文件路径>");
            return;
        }

        new FileClient(FileTransferConstants.SERVER_HOST, FileTransferConstants.PORT, args[0]).start();
    }
}
