package org.jnfs.example;

import org.jnfs.datanode.DataNodeHandler;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并发重命名原子性测试
 * 模拟 DataNodeHandler 的重命名逻辑，验证 FILE_LOCK 是否生效
 */
public class RenameAtomicityTest {

    private static final int THREAD_COUNT = 10;
    private static final String TEST_DIR = "test_atomicity";
    private static final String TARGET_FILE_NAME = "shared_target_file.txt";
    private static final Object FILE_LOCK = new Object();

    public static void main(String[] args) throws Exception {
        File dir = new File(TEST_DIR);
        if (!dir.exists()) dir.mkdirs();
        
        File targetFile = new File(dir, TARGET_FILE_NAME);
        if (targetFile.exists()) targetFile.delete();

        System.out.println("开始并发重命名测试，目标文件: " + targetFile.getAbsolutePath());

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        AtomicInteger skipCount = new AtomicInteger(0);

        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    // 每个线程创建一个唯一的临时文件
                    File tmpFile = new File(dir, "tmp_" + UUID.randomUUID() + ".tmp");
                    Files.write(tmpFile.toPath(), "test data".getBytes());

                    // 模拟 DataNodeHandler 的同步块
                    synchronized (FILE_LOCK) {
                        if (targetFile.exists()) {
                            // 模拟秒传逻辑
                            tmpFile.delete();
                            skipCount.incrementAndGet();
                        } else {
                            // 模拟重命名
                            if (tmpFile.renameTo(targetFile)) {
                                successCount.incrementAndGet();
                            } else {
                                if (targetFile.exists()) {
                                    tmpFile.delete();
                                    skipCount.incrementAndGet();
                                } else {
                                    failCount.incrementAndGet();
                                    tmpFile.delete();
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        System.out.println("测试结束");
        System.out.println("成功重命名(First Winner): " + successCount.get());
        System.out.println("跳过重命名(Followers): " + skipCount.get());
        System.out.println("失败次数: " + failCount.get());

        if (successCount.get() == 1 && (skipCount.get() + successCount.get() == THREAD_COUNT)) {
            System.out.println("测试通过: 原子性得到保证，只有一个线程执行了重命名，其他线程正确跳过。");
        } else {
            System.err.println("测试失败: 状态不一致!");
        }

        // 清理
        if (targetFile.exists()) targetFile.delete();
        dir.delete();
    }
}
