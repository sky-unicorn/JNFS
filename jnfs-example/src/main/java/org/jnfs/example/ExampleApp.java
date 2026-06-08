package org.jnfs.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.NettyClientBootstrap;
import org.jnfs.common.SecurityConfig;
import org.jnfs.common.Packet;
import org.jnfs.driver.ConnectionStatus;
import org.jnfs.driver.JNFSDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(ExampleApp.class);

    private static final String TOKEN = SecurityConfig.getToken();

    public static void main(String[] args) {
        LOG.info("=== JNFS 综合测试工具 ===");
        LOG.info("1. 标准文件上传与下载测试 (Standard Test)");
        LOG.info("2. 连接池并发 (测试Connection Pool Test)");
        LOG.info("3. 路径遍历漏洞测试 (Path Traversal Security Test)");
        LOG.info("4. 资源泄漏测试 (Connection Leak Test)");
        LOG.info("5. NameNode分段锁并发测试 (NameNode Lock Test)");
        LOG.info("6. DataNode重命名原子性模拟 (Rename Atomicity Simulation)");
        LOG.info("请输入测试编号 [1-6]: ");

        Scanner scanner = new Scanner(System.in);
        String choice = scanner.nextLine().trim();

        try {
            switch (choice) {
                case "1":
                    runStandardTest(scanner);
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
                    LOG.info("无效的输入，默认运行标准测试");
                    runStandardTest(scanner);
            }
        } catch (Exception e) {
            LOG.error("ExampleApp异常", e);
        } finally {
            scanner.close();
        }
    }

    // --- 1. 标准上传下载测试 ---
    private static void runStandardTest(Scanner scanner) {
        JNFSDriver driver = new JNFSDriver("localhost", 5368);
        try {
            // Verify connection before entering interactive loop
            System.out.println("=== Verifying connection to NameNode ===");
            ConnectionStatus status = driver.initialize();
            printConnectionStatus(status);

            if (!status.isOk()) {
                System.err.println("Connection failed. Please check if NameNode is running.");
                return;
            }

            while (true) {
                System.out.println("\n请输入要上传的文件绝对路径 (输入 'exit' 返回主菜单): ");
                String filePath = scanner.nextLine().trim();

                if ("exit".equalsIgnoreCase(filePath)) {
                    break;
                }

                if (filePath.isEmpty()) {
                    continue;
                }

                File file = new File(filePath);
                if (!file.exists() || !file.isFile()) {
                    System.err.println("错误: 文件不存在或不可读 -> " + filePath);
                    continue;
                }

                try {
                    System.out.println("=== 开始上传文件: " + file.getName() + " ===");
                    long startUpload = System.currentTimeMillis();

                    String storageId = driver.uploadFile(file);

                    long endUpload = System.currentTimeMillis();
                    System.out.printf("上传成功! 耗时: %.2f 秒%n", (endUpload - startUpload) / 1000.0);
                    System.out.println("存储ID (Storage ID): " + storageId);

                    System.out.print("是否立即下载回本地验证? (y/n) [默认n]: ");
                    String dlChoice = scanner.nextLine().trim();

                    if ("y".equalsIgnoreCase(dlChoice) || "yes".equalsIgnoreCase(dlChoice)) {
                        System.out.println("\n=== 开始下载文件 ===");
                        long startDownload = System.currentTimeMillis();

                        // 确保下载目录存在
                        String downloadPath = "D:\\data\\jnfs\\download\\";
                        File dlDir = new File(downloadPath);
                        if (!dlDir.exists()) {
                            dlDir.mkdirs();
                        }

                        File downloadedFile = driver.downloadFile(storageId, downloadPath);

                        long endDownload = System.currentTimeMillis();
                        System.out.printf("下载成功! 耗时: %.2f 秒%n", (endDownload - startDownload) / 1000.0);
                        System.out.println("文件已保存至: " + downloadedFile.getAbsolutePath());
                    }

                } catch (Exception e) {
                    LOG.error("文件操作过程中发生错误", e);
                }
            }
        } finally {
            driver.close();
            System.out.println("已断开与 JNFS 服务器的连接。");
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

        JNFSDriver driver = new JNFSDriver("localhost", 5368);
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
            Bootstrap b = NettyClientBootstrap.createWithHandler(group,
                    new SimpleChannelInboundHandler<Packet>() {
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

            ChannelFuture f = b.connect("localhost", 5369).sync();
            Channel channel = f.channel();

            String maliciousName = "../../../malicious_file.txt";
            byte[] nameBytes = maliciousName.getBytes(StandardCharsets.UTF_8);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(TOKEN);
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
            // 使用通用工具类创建 Bootstrap (无业务 Handler)
            Bootstrap b = NettyClientBootstrap.createWithHandler(group);

            ChannelFuture f = b.connect("localhost", 5369).sync();
            Channel channel = f.channel();

            String hash = "ABCD1234LEAK";
            byte[] nameBytes = hash.getBytes(StandardCharsets.UTF_8);

            Packet packet = new Packet();
            packet.setCommandType(CommandType.UPLOAD_REQUEST);
            packet.setToken(TOKEN);
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
                    // 使用通用工具类创建 Bootstrap
                    Bootstrap b = NettyClientBootstrap.createWithHandler(group,
                            new SimpleChannelInboundHandler<Packet>() {
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
                    ChannelFuture f = b.connect("localhost", 5368).sync();
                    Packet packet = new Packet();
                    packet.setCommandType(CommandType.NAMENODE_PRE_UPLOAD);
                    packet.setToken(TOKEN);
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

    // --- Connection Helpers ---

    /**
     * Print connection status in a formatted way.
     */
    private static void printConnectionStatus(ConnectionStatus status) {
        String symbol;
        switch (status.getState()) {
            case SUCCESS:
                symbol = "[OK]";
                break;
            case PARTIAL_SUCCESS:
                symbol = "[PARTIAL]";
                break;
            default:
                symbol = "[FAIL]";
                break;
        }
        System.out.printf("%s Connection State: %s - %s%n",
                symbol, status.getState().name(), status.getMessage());

        if (!status.getReachableRegistries().isEmpty()) {
            System.out.println("    Reachable Registries: " + status.getReachableRegistries());
        }
        if (!status.getUnreachableRegistries().isEmpty()) {
            System.out.println("    Unreachable Registries: " + status.getUnreachableRegistries());
        }
        if (!status.getDiscoveredNameNodes().isEmpty()) {
            System.out.println("    Discovered NameNodes: " + status.getDiscoveredNameNodes());
        }
    }
}
