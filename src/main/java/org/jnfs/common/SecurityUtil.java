package org.jnfs.common;

import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 加密工具类
 * 使用 AES 算法对文件进行加密和解密
 */
public class SecurityUtil {

    // 默认密钥 (16字节 = 128位)，实际生产中应从安全配置中心获取或由用户提供
    private static final byte[] DEFAULT_KEY = "jnfs-secret-key!".getBytes();
    
    private static final AES aes = SecureUtil.aes(DEFAULT_KEY);

    /**
     * 加密文件
     * @param srcFile 源文件 (明文)
     * @param destFile 目标文件 (密文)
     */
    public static void encryptFile(File srcFile, File destFile) throws Exception {
        try (InputStream in = new FileInputStream(srcFile);
             OutputStream out = new FileOutputStream(destFile)) {
            aes.encrypt(in, out, true); // isClose = true
        }
    }

    /**
     * 解密文件
     * @param srcFile 源文件 (密文)
     * @param destFile 目标文件 (明文)
     */
    public static void decryptFile(File srcFile, File destFile) throws Exception {
        try (InputStream in = new FileInputStream(srcFile);
             OutputStream out = new FileOutputStream(destFile)) {
            aes.decrypt(in, out, true);
        }
    }
}
