package org.jnfs.example;

import org.jnfs.driver.JNFSDriver;

import java.io.File;

/**
 * 示例应用：模拟客户端引入 Driver SDK 进行文件操作
 */
public class ExampleApp {

    public static void main(String[] args) {
        // 1. 初始化 Driver (连接 NameNode)
        JNFSDriver driver = new JNFSDriver("localhost", 9090);
        
        try {
            // --- 上传测试 ---
            String filePath = "E:\\back-up\\backup.7z";
            File file = new File(filePath);
            
            if (!file.exists()) {
                System.out.println("警告: 未找到目标文件 " + filePath);
                file = new File("large_test.dat");
                if (!file.exists()) {
                    System.out.println("创建测试文件...");
                    try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file, "rw")) {
                        raf.setLength(10 * 1024 * 1024); // 10 MB
                    }
                }
            }
            System.out.println("=== 开始上传文件: " + file.getName() + " ===");

            long startUpload = System.currentTimeMillis();
            
            // 调用 Driver 上传
            String storageId = driver.uploadFile(file);
            
            long endUpload = System.currentTimeMillis();
            System.out.println("--------------------------------------------------");
            System.out.printf("上传总耗时: %.2f 秒%n", (endUpload - startUpload) / 1000.0);
            System.out.println("存储编号 (Storage ID): " + storageId);
            System.out.println("--------------------------------------------------");

            // --- 下载测试 ---
            System.out.println("\n=== 开始下载文件 (通过 Storage ID) ===");
            long startDownload = System.currentTimeMillis();
            
            File downloadedFile = driver.downloadFile(storageId);
            
            long endDownload = System.currentTimeMillis();
            System.out.println("--------------------------------------------------");
            System.out.printf("下载总耗时: %.2f 秒%n", (endDownload - startDownload) / 1000.0);
            System.out.println("文件保存路径: " + downloadedFile.getAbsolutePath());
            System.out.println("--------------------------------------------------");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 4. 关闭资源
            driver.close();
        }
    }
}
