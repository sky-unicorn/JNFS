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
import org.jnfs.common.ConfigUtil;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * NameNode 服务启动类
 * 负责管理元数据和调度 DataNode
 * 已针对高并发进行优化
 */
public class NameNodeServer {

    private final int port;
    private final List<String> dataNodeList;

    public NameNodeServer(int port, List<String> dataNodeList) {
        this.port = port;
        this.dataNodeList = dataNodeList;
    }

    public void run() throws Exception {
        // 1. 线程组优化: Boss负责连接，Worker负责IO读写
        EventLoopGroup bossGroup = new NioEventLoopGroup(1); 
        EventLoopGroup workerGroup = new NioEventLoopGroup(); 
        
        // 2. 业务线程池
        EventExecutorGroup businessGroup = new DefaultEventExecutorGroup(16);
        
        // 初始化 Handler 的 DataNodes
        NameNodeHandler.initDataNodes(dataNodeList);

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
             .option(ChannelOption.SO_BACKLOG, 1024) 
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childOption(ChannelOption.TCP_NODELAY, true) 
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

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        // 加载配置
        Map<String, Object> config = ConfigUtil.loadConfig("namenode.yml");
        
        // 读取端口
        Map<String, Object> serverConfig = (Map<String, Object>) config.get("server");
        int port = (int) serverConfig.getOrDefault("port", 9090);
        
        // 读取 DataNodes 配置
        List<Map<String, Object>> dataNodesConfig = (List<Map<String, Object>>) config.get("datanodes");
        List<String> dataNodeList = new ArrayList<>();
        if (dataNodesConfig != null) {
            for (Map<String, Object> node : dataNodesConfig) {
                String host = (String) node.get("host");
                int nodePort = (int) node.get("port");
                dataNodeList.add(host + ":" + nodePort);
            }
        }
        
        System.out.println("加载 DataNodes: " + dataNodeList);
        
        new NameNodeServer(port, dataNodeList).run();
    }
}
