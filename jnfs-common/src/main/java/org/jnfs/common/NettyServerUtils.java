package org.jnfs.common;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.function.Supplier;

/**
 * Netty 服务端启动工具类
 * 用于消除重复的 ServerBootstrap 样板代码
 */
public class NettyServerUtils {

    /**
     * 启动一个标准的 JNFS Netty 服务 (单例 Handler 模式)
     * 适用于 @Sharable 的 Handler (如 NameNodeHandler, RegistryHandler)
     */
    public static void start(String name, int port, ChannelHandler businessHandler, EventExecutorGroup businessGroup) {
        start0(name, port, () -> businessHandler, businessGroup);
    }

    /**
     * 启动一个标准的 JNFS Netty 服务 (Supplier 模式)
     * 适用于非 Sharable 的 Handler (如 DataNodeHandler)，每次连接创建新实例
     */
    public static void start(String name, int port, Supplier<ChannelHandler> handlerSupplier, EventExecutorGroup businessGroup) {
        start0(name, port, handlerSupplier, businessGroup);
    }

    private static void start0(String name, int port, Supplier<ChannelHandler> handlerSupplier, EventExecutorGroup businessGroup) {
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
                     
                     // 每次调用 get() 获取 Handler 实例
                     // 如果是单例模式，Supplier 返回同一个对象；如果是工厂模式，返回新对象
                     ChannelHandler handler = handlerSupplier.get();
                     
                     if (businessGroup != null) {
                         ch.pipeline().addLast(businessGroup, handler);
                     } else {
                         ch.pipeline().addLast(handler);
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
