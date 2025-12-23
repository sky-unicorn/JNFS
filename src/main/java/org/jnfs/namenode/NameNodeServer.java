package org.jnfs.namenode;

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
 * NameNode 服务启动类
 * 负责管理元数据和调度 DataNode
 * 已针对高并发进行优化
 */
public class NameNodeServer {

    private final int port;

    public NameNodeServer(int port) {
        this.port = port;
    }

    public void run() throws Exception {
        // 1. 线程组优化: Boss负责连接，Worker负责IO读写
        // 默认线程数为 CPU核心数 * 2
        EventLoopGroup bossGroup = new NioEventLoopGroup(1); // Acceptor 线程通常1个就够
        EventLoopGroup workerGroup = new NioEventLoopGroup(); 
        
        // 2. 业务线程池: 将业务逻辑(含磁盘IO/持久化)从 Netty IO 线程中剥离
        // 防止元数据写入磁盘时阻塞网络请求
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(16);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                     // 将 NameNodeHandler 放入业务线程池执行
                     ch.pipeline().addLast(businessGroup, new NameNodeHandler());
                 }
             })
             // 3. TCP 参数调优
             .option(ChannelOption.SO_BACKLOG, 1024) // 增加半连接/全连接队列大小，防止突发连接拒绝
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true) // 禁用 Nagle 算法，降低小包延迟
             .childOption(ChannelOption.SO_RCVBUF, 64 * 1024)
             .childOption(ChannelOption.SO_SNDBUF, 64 * 1024);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS NameNode 启动成功 (High Concurrency Mode)，端口: " + port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            businessGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 9090; 
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        new NameNodeServer(port).run();
    }
}
