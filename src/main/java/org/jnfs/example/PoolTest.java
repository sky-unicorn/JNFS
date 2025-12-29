package org.jnfs.example;

import org.jnfs.driver.JNFSDriver;

import java.io.File;
import java.io.FileOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 测试连接池效果
 */
public class PoolTest {

    public static void main(String[] args) throws Exception {
        // 创建一个临时小文件用于测试
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
                        // 每次上传都会触发多次 NameNode 交互 (PreUpload, GetLoc, Commit)
                        // 理论上这些应该复用同一个 Channel (如果释放得当)
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
}
