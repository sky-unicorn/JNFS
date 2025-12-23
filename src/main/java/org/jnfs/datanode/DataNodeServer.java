package org.jnfs.datanode;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

/**
 * DataNode 服务启动类
 * 负责实际的文件存储
 * 已针对高并发进行优化
 */
public class DataNodeServer {

    private final int port;

    public DataNodeServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        // 1. 线程组优化
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // 默认为 CPU * 2
        
        // 2. 业务线程池: 关键！
        // 文件 I/O (FileChannel.write) 虽然在 NIO 中很快，但在高负载下可能会有阻塞风险
        // 或者是处理复杂的粘包/大文件写入逻辑，剥离到独立线程池更安全
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(32);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                     // 将 DataNodeHandler 放入业务线程池执行
                     ch.pipeline().addLast(businessGroup, new DataNodeHandler());
                 }
             })
             // 3. TCP 参数调优
             .option(ChannelOption.SO_BACKLOG, 4096) // DataNode 可能承载更高并发，队列设大
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             // 增大读写缓冲区，适应大文件传输
             .childOption(ChannelOption.SO_RCVBUF, 1024 * 1024) // 1MB Recv Buffer
             .childOption(ChannelOption.SO_SNDBUF, 1024 * 1024); // 1MB Send Buffer

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS DataNode 启动成功 (High Concurrency Mode)，端口: " + port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            businessGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 8080;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        new DataNodeServer(port).run();
    }
}
