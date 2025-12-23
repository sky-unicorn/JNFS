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
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.util.Map;

/**
 * DataNode 服务启动类
 * 负责实际的文件存储
 * 已针对高并发进行优化
 */
public class DataNodeServer {

    private final int port;
    private final String storagePath;

    public DataNodeServer(int port, String storagePath) {
        this.port = port;
        this.storagePath = storagePath;
    }

    public void run() throws Exception {
        // 1. 线程组优化
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(); 
        
        // 2. 业务线程池
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
                     // 将 storagePath 传递给 DataNodeHandler
                     ch.pipeline().addLast(businessGroup, new DataNodeHandler(storagePath));
                 }
             })
             // 3. TCP 参数调优
             .option(ChannelOption.SO_BACKLOG, 4096) 
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true)
             .childOption(ChannelOption.SO_RCVBUF, 1024 * 1024) 
             .childOption(ChannelOption.SO_SNDBUF, 1024 * 1024);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("JNFS DataNode 启动成功 (High Concurrency Mode)，端口: " + port);
            System.out.println("存储目录: " + storagePath);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            businessGroup.shutdownGracefully();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        // 加载配置
        Map<String, Object> config = ConfigUtil.loadConfig("datanode.yml");
        
        // 读取端口
        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 8080);
        
        // 读取存储路径
        Map<String, Object> storageConfig = (Map<String, Object>) config.get("storage");
        String storagePath = (String) storageConfig.getOrDefault("path", "datanode_files");
        
        new DataNodeServer(port, storagePath).run();
    }
}
