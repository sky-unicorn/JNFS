package org.jnfs.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * 加密工具类
 * 使用 AES-256-CTR + HMAC-SHA256 (Encrypt-then-MAC) 模式
 *
 * 文件格式 (v1):
 *   [version(1)] [HMAC-SHA256(32)] [IV(16)] [ciphertext]
 *
 * 向后兼容: 自动检测旧版 ECB 格式并降级解密
 */
public class SecurityUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

    // v1 版本标识
    private static final byte VERSION_CTR_HMAC = 0x01;
    // CTR IV 长度 (128 位)
    private static final int IV_LENGTH = 16;
    // HMAC-SHA256 输出长度 (256 位 = 32 字节)
    private static final int HMAC_LENGTH = 32;
    // 文件头总长度: version(1) + hmac(32) + iv(16) = 49 字节
    private static final int HEADER_LENGTH = 1 + HMAC_LENGTH + IV_LENGTH;

    // 旧版 ECB 密钥 (仅用于解密旧文件)
    private static final byte[] LEGACY_KEY = "jnfs-secret-key!".getBytes(StandardCharsets.UTF_8);

    private final SecretKey aesKey;
    private final byte[] hmacKey;
    private final SecureRandom secureRandom;

    public SecurityUtil(byte[] encryptionKey) {
        this.aesKey = new SecretKeySpec(Arrays.copyOf(encryptionKey, encryptionKey.length), "AES");
        this.secureRandom = new SecureRandom();

        // HMAC 密钥: 对 AES 密钥做 SHA-256 派生
        this.hmacKey = deriveHmacKey(encryptionKey);
    }

    private static byte[] deriveHmacKey(byte[] aesKeyBytes) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return sha256.digest(aesKeyBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    // ======================== 公共 API ========================

    /**
     * 加密文件 (AES-256-CTR + HMAC-SHA256)
     * 格式: [version(0x01)][HMAC(32)][IV(16)][ciphertext]
     */
    public void encryptFile(File srcFile, File destFile) throws Exception {
        File tmpFile = new File(destFile.getParentFile(), destFile.getName() + ".jnfs_tmp");
        try {
            // 生成随机 IV
            byte[] iv = new byte[IV_LENGTH];
            secureRandom.nextBytes(iv);

            // 先写入密文到临时文件 (IV + ciphertext 部分)
            File bodyTmp = new File(destFile.getParentFile(), destFile.getName() + ".jnfs_body_tmp");
            try (FileInputStream fis = new FileInputStream(srcFile);
                 FileOutputStream bodyOut = new FileOutputStream(bodyTmp)) {

                Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
                cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(iv));

                // 写入 IV
                bodyOut.write(iv);

                // 流式加密
                byte[] buf = new byte[8192];
                int n;
                while ((n = fis.read(buf)) != -1) {
                    byte[] encrypted = cipher.update(buf, 0, n);
                    if (encrypted != null) {
                        bodyOut.write(encrypted);
                    }
                }
                byte[] finalBlock = cipher.doFinal();
                if (finalBlock != null && finalBlock.length > 0) {
                    bodyOut.write(finalBlock);
                }
            }

            // 计算 HMAC-SHA256 (IV + ciphertext)
            byte[] hmac = computeFileHmac(bodyTmp);

            // 组装最终文件: [version][hmac][body(IV + ciphertext)]
            try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
                fos.write(VERSION_CTR_HMAC);
                fos.write(hmac);
                // 追加 body
                try (FileInputStream bodyIn = new FileInputStream(bodyTmp)) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = bodyIn.read(buf)) != -1) {
                        fos.write(buf, 0, n);
                    }
                }
            }

            // 清理 body 临时文件
            bodyTmp.delete();

            // 原子重命名
            if (destFile.exists()) {
                destFile.delete();
            }
            if (!tmpFile.renameTo(destFile)) {
                throw new IOException("重命名加密文件失败: " + destFile.getAbsolutePath());
            }
        } catch (Exception e) {
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
            throw e;
        }
    }

    /**
     * 解密文件，自动检测格式 (CTR+HMAC v1 或旧版 ECB)
     */
    public void decryptFile(File srcFile, File destFile) throws Exception {
        try (FileInputStream fis = new FileInputStream(srcFile)) {
            int firstByte = fis.read();
            if (firstByte == -1) {
                throw new IOException("文件为空");
            }

            if (firstByte != VERSION_CTR_HMAC) {
                decryptLegacyFile(srcFile, destFile);
                return;
            }

            // 读取 HMAC (32 字节)
            byte[] storedHmac = readFully(fis, HMAC_LENGTH);

            // 读取 IV (16 字节)
            byte[] iv = readFully(fis, IV_LENGTH);

            // 读取剩余密文
            byte[] ciphertext = readAll(fis);

            // 验证 HMAC-SHA256 (IV + ciphertext)
            byte[] computedHmac = computeHmac(iv, ciphertext);
            if (!MessageDigest.isEqual(storedHmac, computedHmac)) {
                throw new IOException("HMAC 验证失败，文件可能被篡改");
            }

            // AES-CTR 解密
            Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv));
            byte[] plaintext = cipher.doFinal(ciphertext);

            // 写入目标文件
            try (FileOutputStream fos = new FileOutputStream(destFile)) {
                fos.write(plaintext);
            }
        }
    }

    /**
     * 创建 CTR+HMAC 流式解密输出流
     *
     * 工作原理:
     * 1. 缓存收到的所有加密数据
     * 2. close() 时读取 header, 验证 HMAC, CTR 解密写出
     */
    public OutputStream createDecryptOutputStream(OutputStream out) {
        return new CtrHmacDecryptStream(out);
    }

    // ======================== 内部实现 ========================

    /**
     * 计算 HMAC-SHA256
     */
    private byte[] computeHmac(byte[]... data) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
        for (byte[] d : data) {
            mac.update(d);
        }
        return mac.doFinal();
    }

    /**
     * 计算文件的 HMAC-SHA256 (流式处理，避免大文件内存溢出)
     */
    private byte[] computeFileHmac(File file) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = fis.read(buf)) != -1) {
                mac.update(buf, 0, n);
            }
        }
        return mac.doFinal();
    }

    /**
     * 解密旧版 ECB 格式文件
     */
    private void decryptLegacyFile(File srcFile, File destFile) throws Exception {
        LOG.info("检测到旧版 ECB 格式，使用传统方式解密");
        SecretKeySpec legacyKeySpec = new SecretKeySpec(LEGACY_KEY, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, legacyKeySpec);

        try (FileInputStream fis = new FileInputStream(srcFile);
             CipherInputStream cis = new CipherInputStream(fis, cipher);
             FileOutputStream fos = new FileOutputStream(destFile)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = cis.read(buf)) != -1) {
                fos.write(buf, 0, n);
            }
        }
    }

    /**
     * 从流中精确读取指定字节数
     */
    private static byte[] readFully(InputStream in, int length) throws IOException {
        byte[] buf = new byte[length];
        int offset = 0;
        while (offset < length) {
            int n = in.read(buf, offset, length - offset);
            if (n == -1) {
                throw new IOException("Unexpected end of stream (expected " + length + " bytes, got " + offset + ")");
            }
            offset += n;
        }
        return buf;
    }

    /**
     * 读取流中剩余全部字节
     */
    private static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
            bos.write(buf, 0, n);
        }
        return bos.toByteArray();
    }

    // ======================== 内部类: CTR+HMAC 流式解密 ========================

    private class CtrHmacDecryptStream extends FilterOutputStream {

        private byte[] buffer = new byte[0];
        private boolean versionRead = false;
        private boolean isLegacy = false;
        private boolean closed = false;

        CtrHmacDecryptStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            append(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            append(b, off, len);
        }

        private void append(byte[] b, int off, int len) throws IOException {
            byte[] newBuf = new byte[buffer.length + len];
            System.arraycopy(buffer, 0, newBuf, 0, buffer.length);
            System.arraycopy(b, off, newBuf, buffer.length, len);
            buffer = newBuf;
        }

        @Override
        public void close() throws IOException {
            if (closed) return;
            closed = true;

            try {
                if (buffer.length == 0) {
                    return;
                }

                byte version = buffer[0];
                if (version != VERSION_CTR_HMAC) {
                    // 旧版 ECB 格式
                    decryptLegacyBuffer();
                    return;
                }

                // CTR+HMAC 格式: 需要完整的 header
                if (buffer.length < HEADER_LENGTH) {
                    throw new IOException("数据不完整: 期望至少 " + HEADER_LENGTH
                            + " 字节 header，实际 " + buffer.length + " 字节");
                }

                // 提取 header 字段
                byte[] storedHmac = new byte[HMAC_LENGTH];
                System.arraycopy(buffer, 1, storedHmac, 0, HMAC_LENGTH);

                byte[] iv = new byte[IV_LENGTH];
                System.arraycopy(buffer, 1 + HMAC_LENGTH, iv, 0, IV_LENGTH);

                // 提取密文
                int ciphertextLen = buffer.length - HEADER_LENGTH;
                byte[] ciphertext = new byte[ciphertextLen];
                System.arraycopy(buffer, HEADER_LENGTH, ciphertext, 0, ciphertextLen);

                // 验证 HMAC-SHA256 (IV + ciphertext)
                byte[] computedHmac = computeHmac(iv, ciphertext);
                if (!MessageDigest.isEqual(storedHmac, computedHmac)) {
                    throw new IOException("HMAC 验证失败，数据可能被篡改");
                }

                // AES-CTR 解密
                Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv));
                byte[] plaintext = cipher.doFinal(ciphertext);

                out.write(plaintext);
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("CTR+HMAC 解密失败", e);
            } finally {
                buffer = new byte[0];
                if (out != null) {
                    out.close();
                }
            }
        }

        private void decryptLegacyBuffer() throws IOException {
            try {
                LOG.info("流式解密: 检测到旧版 ECB 格式");
                SecretKeySpec legacyKeySpec = new SecretKeySpec(LEGACY_KEY, "AES");
                Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                cipher.init(Cipher.DECRYPT_MODE, legacyKeySpec);
                byte[] plaintext = cipher.doFinal(buffer);
                out.write(plaintext);
                buffer = new byte[0];
            } catch (Exception e) {
                throw new IOException("旧版格式解密失败", e);
            }
        }
    }
}
