package org.jnfs.common;

import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.AES;

import java.io.*;

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

    /**
     * 创建解密输出流
     * @param out 底层输出流
     * @return 包装后的解密流
     */
    public static OutputStream createDecryptOutputStream(OutputStream out) {
        try {
            // 保持与 SecureUtil.aes(DEFAULT_KEY) 一致的算法配置
            javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES");
            cipher.init(javax.crypto.Cipher.DECRYPT_MODE, new javax.crypto.spec.SecretKeySpec(DEFAULT_KEY, "AES"));
            return new javax.crypto.CipherOutputStream(out, cipher);
        } catch (Exception e) {
            throw new RuntimeException("创建解密流失败", e);
        }
    }
}
