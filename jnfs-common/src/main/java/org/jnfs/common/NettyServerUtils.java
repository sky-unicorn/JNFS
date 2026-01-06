package org.jnfs.common;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Netty 服务端启动工具类
 * 用于消除重复的 ServerBootstrap 样板代码
 */
public class NettyServerUtils {

    /**
     * 启动一个标准的 JNFS Netty 服务
     * @param name 服务名称 (用于日志)
     * @param port 监听端口
     * @param businessHandler 业务处理器
     * @param businessGroup 业务线程池 (可选，如果为 null 则在 IO 线程处理)
     */
    public static void start(String name, int port, ChannelHandler businessHandler, EventExecutorGroup businessGroup) {
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
                     if (businessGroup != null) {
                         ch.pipeline().addLast(businessGroup, businessHandler);
                     } else {
                         ch.pipeline().addLast(businessHandler);
                     }
                 }
             })
             .option(ChannelOption.SO_BACKLOG, 1024)
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .childOption(ChannelOption.SO_RCVBUF, 64 * 1024)
             .childOption(ChannelOption.SO_SNDBUF, 64 * 1024);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS " + name + " 启动成功，端口: " + port);

            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println(name + " 启动被中断");
        } catch (Exception e) {
            System.err.println(name + " 启动失败: " + e.getMessage());
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            if (businessGroup != null) {
                businessGroup.shutdownGracefully();
            }
        }
    }
}
