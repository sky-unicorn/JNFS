package org.jnfs.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;
import org.jnfs.driver.JNFSDriver;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JNFS 综合测试客户端
 * 包含：标准上传下载、连接池并发测试、安全漏洞测试、资源泄漏测试、并发原子性模拟等
 */
public class ExampleApp {

    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025";

    public static void main(String[] args) {
        System.out.println("=== JNFS 综合测试工具 ===");
        System.out.println("1. 标准文件上传与下载测试 (Standard Test)");
        System.out.println("2. 连接池并发测试 (Connection Pool Test)");
        System.out.println("3. 路径遍历漏洞测试 (Path Traversal Security Test)");
        System.out.println("4. 资源泄漏测试 (Connection Leak Test)");
        System.out.println("5. NameNode分段锁并发测试 (NameNode Lock Test)");
        System.out.println("6. DataNode重命名原子性模拟 (Rename Atomicity Simulation)");
        System.out.print("请输入测试编号 [1-6]: ");

        Scanner scanner = new Scanner(System.in);
        String choice = scanner.nextLine().trim();

        try {
            switch (choice) {
                case "1":
                    runStandardTest();
                    break;
                case "2":
                    runPoolTest();
                    break;
                case "3":
                    runSecurityTest();
                    break;
                case "4":
                    runLeakTest();
                    break;
                case "5":
                    runLockTest();
                    break;
                case "6":
                    runRenameAtomicityTest();
                    break;
                default:
                    System.out.println("无效的输入，默认运行标准测试");
                    runStandardTest();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --- 1. 标准上传下载测试 ---
    private static void runStandardTest() {
        JNFSDriver driver = new JNFSDriver("localhost", 9090);
        try {
            String filePath = "E:\\back-up\\backup.7z";
            File file = new File(filePath);

            if (!file.exists()) {
                System.out.println("警告: 未找到目标文件 " + filePath);
                file = new File("large_test.dat");
                if (!file.exists()) {
                    System.out.println("创建测试文件: " + file.getAbsolutePath());
                    try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
                        raf.setLength(10 * 1024 * 1024); // 10 MB
                    }
                }
            }
            System.out.println("=== 开始上传文件: " + file.getName() + " ===");
            long startUpload = System.currentTimeMillis();

            String storageId = driver.uploadFile(file);

            long endUpload = System.currentTimeMillis();
            System.out.printf("上传总耗时: %.2f 秒%n", (endUpload - startUpload) / 1000.0);
            System.out.println("存储编号: " + storageId);

            System.out.println("\n=== 开始下载文件 ===");
            long startDownload = System.currentTimeMillis();

            File downloadedFile = driver.downloadFile(storageId);

            long endDownload = System.currentTimeMillis();
            System.out.printf("下载总耗时: %.2f 秒%n", (endDownload - startDownload) / 1000.0);
            System.out.println("文件保存路径: " + downloadedFile.getAbsolutePath());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }
    }

    // --- 2. 连接池并发测试 ---
    private static void runPoolTest() throws Exception {
        File testFile = new File("pool_test.txt");
        if (!testFile.exists()) {
            try (FileOutputStream fos = new FileOutputStream(testFile)) {
                fos.write("Hello Pool".getBytes());
            }
        }

        JNFSDriver driver = new JNFSDriver("localhost", 9090);
        int threads = 5;
        int requestsPerThread = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        System.out.println("开始并发测试: " + (threads * requestsPerThread) + " 次请求");
        long start = System.currentTimeMillis();

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    try {
                        driver.uploadFile(testFile);
                        System.out.println(Thread.currentThread().getName() + " - 完成一次上传");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
        long end = System.currentTimeMillis();
        System.out.println("测试结束，耗时: " + (end - start) + "ms");

        driver.close();
        testFile.delete();
    }

    // --- 3. 路径遍历漏洞测试 ---
    private static void runSecurityTest() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
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
                             if (msg.getCommandType() == CommandType.ERROR) {
                                 System.out.println("收到预期错误响应: " + new String(msg.getData()));
                             } else {
                                 System.out.println("收到非预期响应: " + msg.getCommandType());
                             }
                             ctx.close();
                         }
                     });
                 }
             });

            ChannelFuture f = b.connect("localhost", 8080).sync();
            Channel channel = f.channel();

            String maliciousName = "../../../malicious_file.txt";
            byte[] nameBytes = maliciousName.getBytes(StandardCharsets.UTF_8);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(CLIENT_TOKEN);
            packet.setData(nameBytes);
            packet.setStreamLength(10); // file size

            System.out.println("发送恶意上传请求: " + maliciousName);
            channel.writeAndFlush(packet);
            channel.writeAndFlush(Unpooled.wrappedBuffer("HACKED".getBytes()));

            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    // --- 4. 资源泄漏测试 ---
    private static void runLeakTest() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new PacketDecoder());
                     ch.pipeline().addLast(new PacketEncoder());
                 }
             });

            ChannelFuture f = b.connect("localhost", 8080).sync();
            Channel channel = f.channel();

            String hash = "ABCD1234LEAK";
            byte[] nameBytes = hash.getBytes(StandardCharsets.UTF_8);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(CLIENT_TOKEN);
            packet.setData(nameBytes);
            packet.setStreamLength(1024 * 1024);

            channel.writeAndFlush(packet);

            // 发送一部分数据后立即强制断开
            channel.writeAndFlush(Unpooled.buffer().writeBytes(new byte[64 * 1024]));
            channel.close();
            System.out.println("已模拟强制断开连接，请检查 DataNode 日志是否触发清理");

            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    // --- 5. NameNode分段锁并发测试 ---
    private static void runLockTest() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            System.out.println("=== 测试场景 1: 10个线程并发上传 [同一文件] (预期: 互斥) ===");
            testLockConcurrency(group, "SAME_FILE_HASH", 10);

            Thread.sleep(1000);

            System.out.println("\n=== 测试场景 2: 10个线程并发上传 [不同文件] (预期: 并行) ===");
            testLockConcurrency(group, null, 10);
        } finally {
            group.shutdownGracefully();
        }
    }

    private static void testLockConcurrency(EventLoopGroup group, String fixedHash, int threadCount) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger allowCount = new AtomicInteger(0);
        AtomicInteger waitCount = new AtomicInteger(0);

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
                                     } else if (msg.getCommandType() == CommandType.NAMENODE_RESPONSE_WAIT) {
                                         waitCount.incrementAndGet();
                                     }
                                     ctx.close();
                                     latch.countDown();
                                 }
                             });
                         }
                     });
                    ChannelFuture f = b.connect("localhost", 9090).sync();
                    Packet packet = new Packet();
                    packet.setCommandType(CommandType.NAMENODE_PRE_UPLOAD);
                    packet.setToken(CLIENT_TOKEN);
                    packet.setData(hash.getBytes(StandardCharsets.UTF_8));
                    f.channel().writeAndFlush(packet);
                } catch (Exception e) {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();
        System.out.println("耗时: " + (System.currentTimeMillis() - start) + " ms");
        System.out.println("结果统计 -> ALLOW: " + allowCount.get() + ", WAIT: " + waitCount.get());
    }

    // --- 6. DataNode重命名原子性模拟 ---
    private static void runRenameAtomicityTest() throws Exception {
        int threadCount = 10;
        File dir = new File("test_atomicity");
        if (!dir.exists()) dir.mkdirs();
        File targetFile = new File(dir, "shared_target_file.txt");
        if (targetFile.exists()) targetFile.delete();
        Object fileLock = new Object(); // 模拟 DataNodeHandler 中的锁

        System.out.println("开始并发重命名模拟...");
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger skipCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    File tmpFile = new File(dir, "tmp_" + UUID.randomUUID() + ".tmp");
                    Files.write(tmpFile.toPath(), "test data".getBytes());

                    synchronized (fileLock) {
                        if (targetFile.exists()) {
                            tmpFile.delete();
                            skipCount.incrementAndGet();
                        } else {
                            if (tmpFile.renameTo(targetFile)) {
                                successCount.incrementAndGet();
                            } else if (targetFile.exists()) {
                                tmpFile.delete();
                                skipCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        System.out.println("成功(Winner): " + successCount.get());
        System.out.println("跳过(Followers): " + skipCount.get());

        if (targetFile.exists()) targetFile.delete();
        dir.delete();
    }
}
