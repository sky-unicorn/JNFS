package org.jnfs.registry;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

/**
 * 注册中心服务 (Standalone)
 * 负责 DataNode 的注册、心跳维护，以及向 NameNode 提供服务发现
 * 
 * 升级：集成 Dashboard HTTP 服务
 */
public class RegistryServer {

    private final int port;
    private final int dashboardPort;

    public RegistryServer(int port, int dashboardPort) {
        this.port = port;
        this.dashboardPort = dashboardPort;
    }

    public void run() throws Exception {
        // 启动 Dashboard (独立线程)
        new Thread(() -> new DashboardServer(dashboardPort).start()).start();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                     ch.pipeline().addLast(new RegistryHandler());
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 128)
             .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS Registry Center 启动成功，RPC端口: " + port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        int port = 8000; // 注册中心 RPC 端口
        int dashboardPort = 8081; // Dashboard HTTP 端口
        new RegistryServer(port, dashboardPort).run();
    }
}
