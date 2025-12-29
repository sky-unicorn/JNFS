package org.jnfs.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 分段锁并发测试
 * 验证 NameNodeHandler 的分段锁机制是否正确处理并发请求
 */
public class LockTest {

    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025";
    private static final String HOST = "localhost";
    private static final int PORT = 9090;

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        
        try {
            System.out.println("=== 测试场景 1: 10个线程并发上传 [同一文件] (预期: 互斥) ===");
            testConcurrency(group, "SAME_FILE_HASH", 10);
            
            Thread.sleep(1000);
            
            System.out.println("\n=== 测试场景 2: 10个线程并发上传 [不同文件] (预期: 并行) ===");
            testConcurrency(group, null, 10);
            
        } finally {
            group.shutdownGracefully();
        }
    }

    private static void testConcurrency(EventLoopGroup group, String fixedHash, int threadCount) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger allowCount = new AtomicInteger(0);
        AtomicInteger waitCount = new AtomicInteger(0);
        AtomicInteger existCount = new AtomicInteger(0);
        
        long start = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String hash = (fixedHash != null) ? fixedHash : "FILE_HASH_" + index;
                    
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
                                     if (msg.getCommandType() == CommandType.NAMENODE_RESPONSE_ALLOW) {
                                         allowCount.incrementAndGet();
                                         // System.out.println("线程 " + index + ": ALLOW");
                                     } else if (msg.getCommandType() == CommandType.NAMENODE_RESPONSE_WAIT) {
                                         waitCount.incrementAndGet();
                                         // System.out.println("线程 " + index + ": WAIT");
                                     } else if (msg.getCommandType() == CommandType.NAMENODE_RESPONSE_EXIST) {
                                         existCount.incrementAndGet();
                                         // System.out.println("线程 " + index + ": EXIST");
                                     }
                                     ctx.close();
                                     latch.countDown();
                                 }
                             });
                         }
                     });

                    ChannelFuture f = b.connect(HOST, PORT).sync();
                    
                    Packet packet = new Packet();
                    packet.setCommandType(CommandType.NAMENODE_PRE_UPLOAD);
                    packet.setToken(CLIENT_TOKEN);
                    packet.setData(hash.getBytes(StandardCharsets.UTF_8));
                    
                    f.channel().writeAndFlush(packet);
                    
                } catch (Exception e) {
                    e.printStackTrace();
                    latch.countDown();
                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();
        executor.shutdown();

        System.out.println("耗时: " + (end - start) + " ms");
        System.out.println("结果统计 -> ALLOW: " + allowCount.get() + ", WAIT: " + waitCount.get() + ", EXIST: " + existCount.get());
        
        if (fixedHash != null) {
            // 同一文件场景：应该只有1个ALLOW，其他的可能是WAIT
            if (allowCount.get() > 1) {
                System.err.println("测试失败: 多个线程同时获得了上传权限!");
            } else {
                System.out.println("测试通过: 锁机制生效，未发生并发写入冲突。");
            }
        } else {
            // 不同文件场景：应该全部ALLOW
            if (allowCount.get() == threadCount) {
                System.out.println("测试通过: 所有请求均并行处理成功。");
            } else {
                System.err.println("测试异常: 部分请求未获得许可 (可能是之前的测试遗留数据影响)。");
            }
        }
    }
}
