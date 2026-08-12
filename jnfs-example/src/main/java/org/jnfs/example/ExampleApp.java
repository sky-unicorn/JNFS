package org.jnfs.example;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.NettyClientBootstrap;
import org.jnfs.common.SecurityConfig;
import org.jnfs.common.SecurityUtil;
import org.jnfs.common.Packet;
import org.jnfs.driver.ConnectionStatus;
import org.jnfs.driver.JNFSDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JNFS 综合测试客户端
 * 包含：标准上传下载、连接池并发测试、安全漏洞测试、资源泄漏测试、并发原子性模拟等
 */
public class ExampleApp {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleApp.class);

    private static final String TOKEN = SecurityConfig.getToken();

    // 服务地址（启动时输入；注册中心模式默认 5367，直连 NameNode 模式默认 5368）
    private static String SERVER_HOST = "localhost";
    private static int SERVER_PORT = 5367;
    // 连接方式：true=直连 NameNode，false=注册中心 Registry（默认）
    private static boolean useDirectMode = false;

    public static void main(String[] args) {
        LOG.info("=== JNFS 综合测试工具 ===");

        Scanner scanner = new Scanner(System.in);

        // 选择连接方式：默认注册中心（非直连），通吃单机/集群部署
        System.out.print("请选择连接方式 (1=注册中心 Registry, 2=直连 NameNode) [默认 1]: ");
        useDirectMode = "2".equals(scanner.nextLine().trim());

        System.out.print("请输入主机 (默认 localhost): ");
        String hostInput = scanner.nextLine().trim();
        if (!hostInput.isEmpty()) {
            SERVER_HOST = hostInput;
        }

        // 端口默认随模式：注册中心 5367 / 直连 NameNode 5368
        int defaultPort = useDirectMode ? 5368 : 5367;
        SERVER_PORT = readInt(scanner, "请输入端口", defaultPort);

        LOG.info("连接方式: {} | 地址: {}:{}",
                useDirectMode ? "直连 NameNode" : "注册中心 Registry",
                SERVER_HOST, SERVER_PORT);

        LOG.info("1. 标准文件上传与下载测试 (Standard Test)");
        LOG.info("2. 连接池并发 (测试Connection Pool Test)");
        LOG.info("3. 路径遍历漏洞测试 (Path Traversal Security Test)");
        LOG.info("4. 资源泄漏测试 (Connection Leak Test)");
        LOG.info("5. NameNode分段锁并发测试 (NameNode Lock Test)");
        LOG.info("6. DataNode重命名原子性模拟 (Rename Atomicity Simulation)");
        LOG.info("7. 批量暴力测试 (Brutal Batch Test)");
        LOG.info("请输入测试编号 [1-7]: ");

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
                case "7":
                    runBrutalBatchTest(scanner);
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

    /**
     * 按启动时选择的连接方式创建 Driver：
     *   直连模式  -> new JNFSDriver(host, port)         (直接打 NameNode)
     *   注册中心  -> JNFSDriver.useRegistry(host, port) (经 Registry 发现 NameNode)
     */
    private static JNFSDriver createDriver() {
        if (useDirectMode) {
            return new JNFSDriver(SERVER_HOST, SERVER_PORT);
        }
        return JNFSDriver.useRegistry(SERVER_HOST, SERVER_PORT);
    }

    // --- 1. 标准上传下载测试 ---
    private static void runStandardTest(Scanner scanner) {
        JNFSDriver driver = createDriver();
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

        JNFSDriver driver = createDriver();
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

    // ==================== 7. 批量暴力测试 ====================

    /** 默认 AES key（仅本地 HMAC 篡改测试用，不经集群） */
    private static final byte[] DEFAULT_AES_KEY = "jnfs-aes-key-256bit-secure-key!!".getBytes(StandardCharsets.UTF_8);

    private static void runBrutalBatchTest(Scanner scanner) {
        System.out.println("\n========================================");
        System.out.println("     批量暴力测试 (Brutal Batch Test)");
        System.out.println("========================================");
        System.out.println("确保 NameNode(5368), DataNode(5369), Registry(5367) 均已启动！\n");

        // --- 交互配置 ---
        int fileCount = readInt(scanner, "文件数量", 20);
        int sizeMode  = readInt(scanner, "大小档位 (1=混合, 2=小文件, 3=大文件)", 1);
        int threads   = readInt(scanner, "并发线程数", 10);
        int largeMB   = readInt(scanner, "大文件专项 [MB, 0=跳过]", 300);
        System.out.print("跑协议边界注入 (streamLength 超限 + HMAC 篡改)? [y/n] (默认 y): ");
        String protoChoice = scanner.nextLine().trim();
        boolean runProto = (!"n".equalsIgnoreCase(protoChoice) && !"no".equalsIgnoreCase(protoChoice));

        System.out.println("\n配置: files=" + fileCount + " sizeMode=" + sizeMode + " threads=" + threads
                + " largeMB=" + largeMB + " proto=" + runProto);

        long heapMax = Runtime.getRuntime().maxMemory() / 1024 / 1024;
        System.out.printf("JVM 堆上限: %d MB  (建议 -Xmx≥512m)%n", heapMax);

        // --- 连通性检查 ---
        JNFSDriver driver = createDriver();
        try {
            ConnectionStatus status = driver.initialize();
            printConnectionStatus(status);
            if (!status.isOk()) {
                System.err.println("[FAIL] NameNode 连接失败，请确认服务已启动。");
                return;
            }
        } catch (Exception e) {
            System.err.println("[FAIL] 初始化连接异常: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // --- 按序执行子测试 ---
        // 子方法 A
        try {
            System.out.println("\n=== [A] 批量上传下载 + SHA-256 完整性校验 ===");
            runBatchUploadDownload(driver, fileCount, sizeMode, threads);
        } catch (Exception e) {
            System.err.println("[A] 批量上传下载异常: " + e.getMessage());
            e.printStackTrace();
        }

        // 子方法 B
        try {
            System.out.println("\n=== [B] 高并发同文件上传 ===");
            runConcurrentSameFile(driver, threads);
        } catch (Exception e) {
            System.err.println("[B] 高并发同文件异常: " + e.getMessage());
            e.printStackTrace();
        }

        // 子方法 C
        if (largeMB > 0) {
            try {
                System.out.println("\n=== [C] 大文件专项 OOM 验证 (" + largeMB + " MB) ===");
                runLargeFileOOM(driver, largeMB);
            } catch (Exception e) {
                System.err.println("[C] 大文件专项异常: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("\n[C] 大文件专项已跳过 (largeMB=0)");
        }

        // 子方法 D
        if (runProto) {
            try {
                System.out.println("\n=== [D] 协议边界注入 (streamLength 超限 + HMAC 篡改) ===");
                runProtocolInjection(5);
            } catch (Exception e) {
                System.err.println("[D] 协议注入异常: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("\n[D] 协议边界注入已跳过 (用户选择 no)");
        }

        driver.close();
        System.out.println("\n========================================");
        System.out.println("     批量暴力测试 - 全部完成");
        System.out.println("========================================");
    }

    // ----- 子方法 A: 批量上传下载 + 完整性校验 -----
    private static void runBatchUploadDownload(JNFSDriver driver, int count, int sizeMode, int threads)
            throws Exception {
        // 生成临时目录
        File batchDir = new File(System.getProperty("java.io.tmpdir"), "jnfs_batch_" + System.currentTimeMillis());
        batchDir.mkdirs();
        File dlDir = new File(batchDir, "downloads");
        dlDir.mkdirs();

        long totalBytes = 0;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicInteger corruptCount = new AtomicInteger(0);
        AtomicLong elapsedSumMs = new AtomicLong(0);
        AtomicLong elapsedMinMs = new AtomicLong(Long.MAX_VALUE);
        AtomicLong elapsedMaxMs = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(count);

        System.out.printf("生成 %d 个测试文件...%n", count);
        File[] files = new File[count];
        byte[][] shaPool = new byte[count][]; // SHA-256 of each plain
        for (int i = 0; i < count; i++) {
            int size;
            switch (sizeMode) {
                case 2: size = 4 * 1024; break;                                    // 小文件 4KB
                case 3: size = 50 * 1024 * 1024; break;                            // 大文件 50MB
                case 1:
                default:
                    // 混合: 30% 4KB, 50% 200KB, 20% 50MB
                    double r = Math.random();
                    if (r < 0.3) size = 4 * 1024;
                    else if (r < 0.8) size = 200 * 1024;
                    else size = 50 * 1024 * 1024;
                    break;
            }
            files[i] = new File(batchDir, "batch_" + i + "_" + UUID.randomUUID().toString().substring(0, 8) + ".bin");
            byte[] buf = new byte[8192];
            try (FileOutputStream fos = new FileOutputStream(files[i])) {
                long remaining = size;
                while (remaining > 0) {
                    int n = (int) Math.min(buf.length, remaining);
                    for (int j = 0; j < n; j++) buf[j] = (byte) ((i * 7 + j + remaining) & 0xFF);
                    fos.write(buf, 0, n);
                    remaining -= n;
                }
            }
            shaPool[i] = sha256(files[i]);
            totalBytes += size;
        }
        System.out.printf("共生成 %d 个文件, %.1f MB (临时目录: %s)%n",
                count, totalBytes / 1024.0 / 1024.0, batchDir.getAbsolutePath());

        System.out.printf("开始并发上传下载 (%d 线程)...%n", threads);
        long overallStart = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            final int idx = i;
            executor.submit(() -> {
                long taskStart = System.currentTimeMillis();
                try {
                    File localFile = files[idx];
                    byte[] expectedSha = shaPool[idx];
                    String storageId = driver.uploadFile(localFile);
                    File dl = driver.downloadFile(storageId, dlDir.getAbsolutePath());
                    byte[] actualSha = sha256(dl);
                    if (!MessageDigest.isEqual(expectedSha, actualSha)) {
                        corruptCount.incrementAndGet();
                        System.err.printf("[FAIL] SHA-256 不一致: file #%d storageId=%s%n", idx, storageId);
                    }
                    // 清理下载文件
                    dl.delete();
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                    System.err.printf("[FAIL] file #%d: %s%n", idx, e.getMessage());
                } finally {
                    long elapsed = System.currentTimeMillis() - taskStart;
                    elapsedSumMs.addAndGet(elapsed);
                    elapsedMinMs.accumulateAndGet(elapsed, Math::min);
                    elapsedMaxMs.accumulateAndGet(elapsed, Math::max);
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        long overallMs = System.currentTimeMillis() - overallStart;

        // 统计输出
        System.out.printf("总量: %d 文件, %.1f MB | 成功: %d | 失败: %d | SHA-256 损坏: %d%n",
                count, totalBytes / 1024.0 / 1024.0, successCount.get(), failCount.get(), corruptCount.get());
        System.out.printf("并发: %d 线程 | 总耗时: %.1f s | 吞吐: %.1f MB/s%n",
                threads, overallMs / 1000.0, (totalBytes / 1024.0 / 1024.0) / (overallMs / 1000.0));
        long avgMs = successCount.get() > 0 ? elapsedSumMs.get() / successCount.get() : 0;
        System.out.printf("单次耗时: 平均 %.1f s | 最快 %.1f s | 最慢 %.1f s%n",
                avgMs / 1000.0, elapsedMinMs.get() / 1000.0, elapsedMaxMs.get() / 1000.0);

        // 清理
        for (File f : files) {
            if (f != null && f.exists()) f.delete();
        }
        File[] dlFiles = dlDir.listFiles();
        if (dlFiles != null) {
            for (File f : dlFiles) f.delete();
        }
        dlDir.delete();
        batchDir.delete();
    }

    // ----- 子方法 B: 高并发同文件上传 -----
    private static void runConcurrentSameFile(JNFSDriver driver, int threads) throws Exception {
        // 生成一个共享文件
        File sameFile = File.createTempFile("jnfs_same_", ".bin");
        sameFile.deleteOnExit();
        int fileMB = 5;
        byte[] buf = new byte[8192];
        try (FileOutputStream fos = new FileOutputStream(sameFile)) {
            long remaining = (long) fileMB * 1024 * 1024;
            while (remaining > 0) {
                int n = (int) Math.min(buf.length, remaining);
                for (int j = 0; j < n; j++) buf[j] = (byte) ((remaining + j) & 0xFF);
                fos.write(buf, 0, n);
                remaining -= n;
            }
        }
        byte[] expectedSha = sha256(sameFile);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicInteger dupHitCount = new AtomicInteger(0);
        Set<String> storageIds = java.util.Collections.synchronizedSet(new HashSet<>());

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        long start = System.currentTimeMillis();

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    String sid = driver.uploadFile(sameFile);
                    if (sid != null && !sid.isEmpty()) {
                        storageIds.add(sid);
                        successCount.incrementAndGet();
                        // 用 downloadFile 反去重：同一 hash 的多次 upload 应返回同一 storageId
                        // 若 NameNode 返回 EXIST，Driver 会将 address 作为 "storageId" 返回
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // 预期：部分请求可能因 WAIT 超时或去重返回已存在的 storageId
                    String msg = e.getMessage();
                    if (msg != null && (msg.contains("已存在") || msg.contains("exist") || msg.contains("wait"))) {
                        dupHitCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                        System.err.println("[FAIL] 并发同文件: " + msg);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        long elapsed = System.currentTimeMillis() - start;

        System.out.printf("结果: 提交 %d 并发 | 成功获 storageId: %d | 去重命中(已存在): %d | 失败: %d | 耗时 %.1f s%n",
                threads, successCount.get(), dupHitCount.get(), failCount.get(), elapsed / 1000.0);
        System.out.printf("唯一 storageId 数: %d (预期 ≈1, 去重生效)%n", storageIds.size());
        for (String sid : storageIds) {
            System.out.printf("  storageId: %s%n", sid);
        }
    }

    // ----- 子方法 C: 大文件专项 OOM 验证 -----
    private static void runLargeFileOOM(JNFSDriver driver, int sizeMB) throws Exception {
        File largeFile = File.createTempFile("jnfs_large_", ".bin");
        largeFile.deleteOnExit();
        System.out.printf("生成 %d MB 测试文件...%n", sizeMB);
        byte[] buf = new byte[8192];
        try (FileOutputStream fos = new FileOutputStream(largeFile)) {
            long remaining = (long) sizeMB * 1024 * 1024;
            while (remaining > 0) {
                int n = (int) Math.min(buf.length, remaining);
                for (int j = 0; j < n; j++) buf[j] = (byte) ((remaining + j * 31) & 0xFF);
                fos.write(buf, 0, n);
                remaining -= n;
            }
        }
        byte[] expectedSha = sha256(largeFile);
        long heapBefore = usedHeap();

        System.out.println("上传中...");
        long t0 = System.currentTimeMillis();
        String storageId = driver.uploadFile(largeFile);
        long upMs = System.currentTimeMillis() - t0;
        long heapAfterUpload = usedHeap();
        System.out.printf("上传完成: %.1f s | 堆: %d -> %d MB%n",
                upMs / 1000.0, heapBefore, heapAfterUpload);

        // 生成一个临时目录用于下载
        File dlDir = new File(System.getProperty("java.io.tmpdir"), "jnfs_large_dl_" + System.currentTimeMillis());
        dlDir.mkdirs();
        try {
            System.out.println("下载中...");
            t0 = System.currentTimeMillis();
            File downloaded = driver.downloadFile(storageId, dlDir.getAbsolutePath());
            long dlMs = System.currentTimeMillis() - t0;
            long heapAfterDownload = usedHeap();
            System.out.printf("下载完成: %.1f s | 堆: %d -> %d MB%n",
                    dlMs / 1000.0, heapAfterUpload, heapAfterDownload);

            byte[] actualSha = sha256(downloaded);
            if (MessageDigest.isEqual(expectedSha, actualSha)) {
                System.out.printf("SHA-256 校验: PASS%n");
            } else {
                System.err.printf("[FAIL] SHA-256 不一致！%n");
            }

            System.out.printf("吞吐 [上传]: %.1f MB/s | [下载]: %.1f MB/s%n",
                    sizeMB / (upMs / 1000.0), sizeMB / (dlMs / 1000.0));

            if (heapAfterDownload > Runtime.getRuntime().maxMemory() * 0.9) {
                System.err.println("[WARN] 堆内存接近上限！若再大可能 OOM");
            } else {
                System.out.printf("堆使用安全: %d / %d MB max (%.0f%% )%n",
                        heapAfterDownload, Runtime.getRuntime().maxMemory() / 1024 / 1024,
                        100.0 * heapAfterDownload / (Runtime.getRuntime().maxMemory() / 1024 / 1024));
            }
            System.out.println("[C] 大文件 OOM 验证: PASS (无 OOM)");

            downloaded.delete();
        } finally {
            dlDir.delete();
        }
    }

    // ----- 子方法 D: 协议边界注入 -----
    private static void runProtocolInjection(int rounds) throws Exception {
        AtomicInteger streamLimitPass = new AtomicInteger(0);
        AtomicInteger hmacPass = new AtomicInteger(0);

        EventLoopGroup group = new NioEventLoopGroup(2);
        try {
            for (int r = 1; r <= rounds; r++) {
                System.out.printf("--- Round %d / %d ---%n", r, rounds);

                // D-1: 超限 streamLength 注入
                try {
                    Bootstrap b = NettyClientBootstrap.createWithHandler(group,
                            new SimpleChannelInboundHandler<Packet>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
                                    // 预期：超限包被 PacketDecoder ctx.close() 静默拒绝，
                                    // 不会到达此 handler（channelInactive 触发）
                                }

                                @Override
                                public void channelInactive(ChannelHandlerContext ctx) {
                                    // 连接被对端关闭，符合预期（PacketDecoder ctx.close()）
                                    streamLimitPass.incrementAndGet();
                                    System.out.printf("  [D-1] streamLimit 注入: PASS (连接被拒绝)%n");
                                }
                            });
                    // 连 DataNode 5369
                    ChannelFuture f = b.connect("localhost", 5369).awaitUninterruptibly();
                    if (f.isSuccess()) {
                        Channel ch = f.channel();
                        Packet pkt = new Packet();
                        pkt.setCommandType(CommandType.UPLOAD_REQUEST);
                        pkt.setToken(TOKEN);
                        pkt.setData("inject_hash".getBytes(StandardCharsets.UTF_8));
                        pkt.setStreamLength(Long.MAX_VALUE);
                        ch.writeAndFlush(pkt);
                        // 等对端关闭
                        ch.closeFuture().await(5000);
                    } else {
                        System.err.printf("  [D-1] streamLimit 注入: FAIL (连接 DataNode 失败: %s)%n",
                                f.cause() != null ? f.cause().getMessage() : "unknown");
                    }
                } catch (Exception e) {
                    System.err.printf("  [D-1] streamLimit 注入异常: %s%n", e.getMessage());
                }
                Thread.sleep(200);

                // D-2: HMAC 篡改验证（本地，不经集群）
                try {
                    SecurityUtil su = new SecurityUtil(DEFAULT_AES_KEY);
                    // 生成小文件 → 加密 → 篡改密文 → 解密断言
                    File plain = File.createTempFile("jnfs_proto_plain_", ".bin");
                    File enc = File.createTempFile("jnfs_proto_enc_", ".bin");
                    File dec = File.createTempFile("jnfs_proto_dec_", ".bin");
                    plain.deleteOnExit();
                    enc.deleteOnExit();
                    dec.deleteOnExit();

                    // 写明文（1KB）
                    byte[] plainBytes = new byte[1024];
                    for (int i = 0; i < plainBytes.length; i++) plainBytes[i] = (byte) (i & 0xFF);
                    Files.write(plain.toPath(), plainBytes);

                    // 加密
                    su.encryptFile(plain, enc);

                    // 篡改密文中部 1 字节
                    byte[] encBytes = Files.readAllBytes(enc.toPath());
                    encBytes[encBytes.length / 2] ^= 0x42; // flip a nibble
                    Files.write(enc.toPath(), encBytes);

                    // 流式解密
                    boolean hmacCaught = false;
                    try (FileOutputStream fos = new FileOutputStream(dec);
                         OutputStream ds = su.createDecryptOutputStream(fos, dec)) {
                        try (FileInputStream fis = new FileInputStream(enc)) {
                            byte[] buf = new byte[8192];
                            int n;
                            while ((n = fis.read(buf)) != -1) {
                                ds.write(buf, 0, n);
                            }
                        }
                    } catch (IOException e) {
                        if (e.getMessage() != null && e.getMessage().contains("HMAC 验证失败")) {
                            hmacCaught = true;
                        } else {
                            throw e;
                        }
                    }

                    if (hmacCaught && !dec.exists()) {
                        hmacPass.incrementAndGet();
                        System.out.printf("  [D-2] HMAC 篡改验证: PASS (检出篡改, 脏文件已清理)%n");
                    } else if (hmacCaught) {
                        System.out.printf("  [D-2] HMAC 篡改验证: 检出篡改但脏文件未清理%n");
                    } else {
                        System.err.printf("  [D-2] HMAC 篡改验证: FAIL (未检出篡改)%n");
                    }

                    plain.delete(); enc.delete(); dec.delete();
                } catch (Exception e) {
                    System.err.printf("  [D-2] HMAC 篡改异常: %s%n", e.getMessage());
                }
            }
        } finally {
            group.shutdownGracefully();
        }

        System.out.println("========================================");
        System.out.printf("[D] 协议注入汇总:%n");
        System.out.printf("  streamLimit 过率: %d/%d%n", streamLimitPass.get(), rounds);
        System.out.printf("  HMAC 篡改过率:   %d/%d%n", hmacPass.get(), rounds);
    }

    // ====== 工具方法 ======

    private static byte[] sha256(File file) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = fis.read(buf)) != -1) {
                    md.update(buf, 0, n);
                }
            }
            return md.digest();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 failed", e);
        }
    }

    /** 当前堆的已使用量，单位 MB */
    private static long usedHeap() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    }

    private static int readInt(Scanner scanner, String prompt, int defaultValue) {
        System.out.print(prompt + " (默认 " + defaultValue + "): ");
        String input = scanner.nextLine().trim();
        if (input.isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            System.out.println("非法输入，使用默认值 " + defaultValue);
            return defaultValue;
        }
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
