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
    // 公开供 DataNode 头部读取等场景计算密文文件逻辑长度（文件长度 - HEADER_LENGTH）
    public static final int HEADER_LENGTH = 1 + HMAC_LENGTH + IV_LENGTH;

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
     * 加密内存中的短数据（AES-256-CTR + HMAC-SHA256，Encrypt-then-MAC）。
     * <p>格式与文件版一致：{@code [version(0x01)][HMAC(32)][IV(16)][ciphertext]}。
     * 适用于 RPC payload 等短数据，不落盘。
     *
     * @param plaintext 明文（UTF-8 字节）
     * @return 密文（含 version + HMAC + IV 头）
     */
    public byte[] encryptBytes(byte[] plaintext) throws Exception {
        byte[] iv = new byte[IV_LENGTH];
        secureRandom.nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, aesKey, new IvParameterSpec(iv));
        byte[] ciphertext = cipher.doFinal(plaintext);

        // HMAC 覆盖 (IV + ciphertext)
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
        mac.update(iv);
        mac.update(ciphertext);
        byte[] hmac = mac.doFinal();

        // 组装: version + hmac + iv + ciphertext
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1 + HMAC_LENGTH + IV_LENGTH + ciphertext.length);
        bos.write(VERSION_CTR_HMAC);
        bos.write(hmac);
        bos.write(iv);
        bos.write(ciphertext);
        return bos.toByteArray();
    }

    /**
     * 解密 {@link #encryptBytes} 产出的数据，校验 HMAC（常量时间比较）。
     * 失败抛 {@link IOException}，调用方应拒绝启动（防止被篡改/密钥不匹配）。
     *
     * @param data 密文
     * @return 明文
     */
    public byte[] decryptBytes(byte[] data) throws Exception {
        if (data == null || data.length < HEADER_LENGTH) {
            throw new IOException("加密 payload 过短");
        }
        if (data[0] != VERSION_CTR_HMAC) {
            throw new IOException("加密 payload 版本不匹配");
        }
        byte[] storedHmac = Arrays.copyOfRange(data, 1, 1 + HMAC_LENGTH);
        byte[] iv = Arrays.copyOfRange(data, 1 + HMAC_LENGTH, 1 + HMAC_LENGTH + IV_LENGTH);
        byte[] ciphertext = Arrays.copyOfRange(data, HEADER_LENGTH, data.length);

        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
        mac.update(iv);
        mac.update(ciphertext);
        byte[] computed = mac.doFinal();
        if (!MessageDigest.isEqual(storedHmac, computed)) {
            throw new IOException("HMAC 验证失败，数据可能被篡改或密钥不匹配");
        }

        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv));
        return cipher.doFinal(ciphertext);
    }

    /**
     * 解密 v1 格式密文的前缀（不校验 HMAC，仅用于文件头嗅探等尽力而为场景）。
     * <p>
     * 入参为 v1 格式 {@code [version(0x01)][HMAC(32)][IV(16)][ciphertext]} 的任意前缀：
     * 跳过 {@link #HEADER_LENGTH} 字节头后，用头内 IV 初始化 AES/CTR 解密剩余密文前缀。
     * CTR 是流密码，前缀解密不需要完整密文；HMAC 覆盖全量数据，前缀无法校验，
     * 因此解密结果仅可用于类型嗅探等非安全场景，不得作为可信数据。
     *
     * @param enc v1 格式密文前缀（至少含完整 header，否则返回空数组）
     * @return 明文前缀；非 v1 格式（legacy ECB）或密文部分为空时返回空数组
     */
    public byte[] decryptHead(byte[] enc) throws Exception {
        if (enc == null || enc.length <= HEADER_LENGTH) {
            return new byte[0];
        }
        if (enc[0] != VERSION_CTR_HMAC) {
            // 旧版 ECB 格式：块加密，前缀无法独立解密，尽力而为返回空
            return new byte[0];
        }
        byte[] iv = Arrays.copyOfRange(enc, 1 + HMAC_LENGTH, HEADER_LENGTH);
        byte[] ciphertext = Arrays.copyOfRange(enc, HEADER_LENGTH, enc.length);
        if (ciphertext.length == 0) {
            return new byte[0];
        }
        Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv));
        return cipher.doFinal(ciphertext);
    }

    /**
     * 加密文件 (AES-256-CTR + HMAC-SHA256)
     * 格式: [version(0x01)][HMAC(32)][IV(16)][ciphertext]
     */
    public void encryptFile(File srcFile, File destFile) throws Exception {
        File tmpFile = new File(destFile.getParentFile(), destFile.getName() + ".jnfs_tmp");
        // bodyTmp 声明提到 try 之外，以便 catch/finally 统一清理，避免临时文件泄漏
        File bodyTmp = new File(destFile.getParentFile(), destFile.getName() + ".jnfs_body_tmp");
        try {
            // 生成随机 IV
            byte[] iv = new byte[IV_LENGTH];
            secureRandom.nextBytes(iv);

            // 先写入密文到临时文件 (IV + ciphertext 部分)
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
            if (bodyTmp.exists()) {
                bodyTmp.delete();
            }
            throw e;
        }
    }

    /**
     * 解密文件，自动检测格式 (CTR+HMAC v1 或旧版 ECB)
     *
     * <p>流式实现：复用 {@link #createDecryptOutputStream} 的 CTR+HMAC 解密逻辑，
     * 逐块读取密文并写出明文，内存恒定 O(块大小)。大文件不再 OOM。
     */
    public void decryptFile(File srcFile, File destFile) throws Exception {
        // 先探测首字节判断格式；v1 文件需要从首字节起完整喂给流式解密器，
        // 因此无论格式如何，都重新打开 FileInputStream 从头读取。
        try (FileInputStream probe = new FileInputStream(srcFile)) {
            int firstByte = probe.read();
            if (firstByte == -1) {
                throw new IOException("文件为空");
            }
            if (firstByte != VERSION_CTR_HMAC) {
                // 旧版 ECB 格式，decryptLegacyFile 已是流式（CipherInputStream）
                decryptLegacyFile(srcFile, destFile);
                return;
            }
        }

        // v1: [version][HMAC(32)][IV(16)][ciphertext] 全部流式喂给解密器
        // 解密器内部会缓冲 header、流式解密、close 时校验 HMAC；
        // HMAC 失败时会关闭并删除已写出的目标文件，再抛 IOException。
        try (FileInputStream fis = new FileInputStream(srcFile);
             FileOutputStream fos = new FileOutputStream(destFile);
             OutputStream decryptStream = createDecryptOutputStream(fos, destFile)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = fis.read(buf)) != -1) {
                decryptStream.write(buf, 0, n);
            }
        }
    }

    /**
     * 创建 CTR+HMAC 流式解密输出流。
     *
     * <p>真流式实现：缓冲固定 49 字节 header 后，每收到一块密文立即
     * {@code cipher.update} 得明文写出、{@code mac.update} 累积 HMAC，
     * 内存恒定 O(块大小)。close() 时 {@code mac.doFinal()} 常量时间比较，
     * 失败则删除目标文件并抛 IOException（落地+失败回滚）。
     *
     * <p>不掌握目标文件路径，HMAC 失败时无法自动删除已写出的明文。
     * 若需要自动回滚，请使用 {@link #createDecryptOutputStream(OutputStream, File)}。
     */
    public OutputStream createDecryptOutputStream(OutputStream out) {
        return new CtrHmacDecryptStream(out, null);
    }

    /**
     * 创建 CTR+HMAC 流式解密输出流，并记录目标文件用于 HMAC 失败回滚。
     *
     * @param out        底层输出流（明文落地目标）
     * @param targetFile 目标文件；HMAC 校验失败时删除此文件以避免脏数据
     */
    public OutputStream createDecryptOutputStream(OutputStream out, File targetFile) {
        return new CtrHmacDecryptStream(out, targetFile);
    }

    // ======================== 内部实现 ========================

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

    // ======================== 内部类: CTR+HMAC 流式解密 ========================

    /**
     * CTR+HMAC 流式解密输出流。
     *
     * <p>v1 格式 [version(0x01)][HMAC(32)][IV(16)][ciphertext] 采用真流式处理：
     * <ul>
     *   <li>缓冲固定 49 字节 header 后，初始化 AES/CTR/NoPadding 解密器与 HmacSHA256；</li>
     *   <li>每收到一块密文立即 {@code cipher.update} 得明文 {@code out.write}，
     *       {@code mac.update} 累积 HMAC；</li>
     *   <li>{@code close()} 时执行 {@code cipher.doFinal} 写出尾部，并以
     *       {@link MessageDigest#isEqual} 常量时间比较 {@code mac.doFinal()} 与存储 HMAC。</li>
     * </ul>
     * 内存占用恒为 O(块大小)，不再累积全部密文。
     *
     * <p><b>落地 + 失败回滚语义</b>：明文在 HMAC 校验前已流式写入目标文件；
     * 一旦 {@code close()} 校验 HMAC 失败，会关闭底层流并删除已知的目标文件，
     * 随后抛出 {@link IOException}，避免留下被篡改的脏数据。
     *
     * <p><b>legacy ECB 路径</b>：若首字节非 0x01，按 AES/ECB/PKCS5Padding 解密。
     * ECB 无 IV，因此同样以 {@code cipher.update} 流式产出明文（内存 O(1)）。
     * PKCS5Padding 的填充合法性仅在 {@code doFinal()} 时才能判定，因此 legacy
     * 路径的数据完整性同样需到 {@code close()} 才确认（历史小文件，风险可控）。
     */
    private class CtrHmacDecryptStream extends FilterOutputStream {

        /** v1 header 缓冲：version(1) + HMAC(32) + IV(16) = 49 字节 */
        private final byte[] headerBuf = new byte[HEADER_LENGTH];
        private int headerBytesRead = 0;
        private boolean versionRead = false;
        private boolean headerDone = false;
        private boolean isLegacy = false;
        private boolean closed = false;

        private Cipher cipher;
        private Mac mac;
        private byte[] storedHmac;

        /** HMAC 失败时用于删除已写出的目标文件；裸 OutputStream 构造时为 null */
        private final File targetFile;

        CtrHmacDecryptStream(OutputStream out, File targetFile) {
            super(out);
            this.targetFile = targetFile;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            int pos = off;
            int remaining = len;

            while (remaining > 0) {
                if (!headerDone) {
                    // 阶段一：读取/填充 header，直到能决定 legacy/v1 并初始化 cipher
                    if (!versionRead) {
                        // 读取首字节以判断文件格式
                        headerBuf[0] = b[pos];
                        headerBytesRead = 1;
                        versionRead = true;
                        pos++;
                        remaining--;

                        if (headerBuf[0] != VERSION_CTR_HMAC) {
                            // 旧版 ECB：首字节即为密文
                            isLegacy = true;
                            initLegacyCipher();
                            byte[] plain = cipher.update(headerBuf, 0, 1);
                            if (plain != null && plain.length > 0) {
                                out.write(plain);
                            }
                            headerDone = true;
                        }
                    }

                    if (!headerDone && !isLegacy) {
                        // v1：继续填充剩余 header
                        int headerNeeded = HEADER_LENGTH - headerBytesRead;
                        int toCopy = Math.min(headerNeeded, remaining);
                        if (toCopy > 0) {
                            System.arraycopy(b, pos, headerBuf, headerBytesRead, toCopy);
                            headerBytesRead += toCopy;
                            pos += toCopy;
                            remaining -= toCopy;
                        }

                        if (headerBytesRead == HEADER_LENGTH) {
                            initCtrCipher();
                            headerDone = true;
                        }
                    }
                } else {
                    // 阶段二：header 到齐，真流式解密/写出
                    if (isLegacy) {
                        byte[] plain = cipher.update(b, pos, remaining);
                        if (plain != null && plain.length > 0) {
                            out.write(plain);
                        }
                    } else {
                        mac.update(b, pos, remaining);
                        byte[] plain = cipher.update(b, pos, remaining);
                        if (plain != null && plain.length > 0) {
                            out.write(plain);
                        }
                    }
                    pos += remaining;
                    remaining = 0;
                }
            }
        }

        private void initLegacyCipher() throws IOException {
            try {
                LOG.info("流式解密: 检测到旧版 ECB 格式");
                SecretKeySpec legacyKeySpec = new SecretKeySpec(LEGACY_KEY, "AES");
                cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
                cipher.init(Cipher.DECRYPT_MODE, legacyKeySpec);
            } catch (Exception e) {
                throw new IOException("初始化旧版 ECB 解密失败", e);
            }
        }

        private void initCtrCipher() throws IOException {
            try {
                storedHmac = new byte[HMAC_LENGTH];
                System.arraycopy(headerBuf, 1, storedHmac, 0, HMAC_LENGTH);

                byte[] iv = new byte[IV_LENGTH];
                System.arraycopy(headerBuf, 1 + HMAC_LENGTH, iv, 0, IV_LENGTH);

                cipher = Cipher.getInstance("AES/CTR/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, aesKey, new IvParameterSpec(iv));

                mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(hmacKey, "HmacSHA256"));
                // HMAC 覆盖 (IV + ciphertext)
                mac.update(iv);
            } catch (Exception e) {
                throw new IOException("初始化 CTR+HMAC 解密失败", e);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;

            IOException failure = null;
            try {
                if (!headerDone) {
                    if (headerBytesRead == 0) {
                        // 空输入：保持与旧行为一致，留空文件
                        return;
                    }
                    if (headerBuf[0] == VERSION_CTR_HMAC && headerBytesRead < HEADER_LENGTH) {
                        throw new IOException("数据不完整: 期望至少 " + HEADER_LENGTH
                                + " 字节 header，实际仅 " + headerBytesRead + " 字节");
                    }
                    // legacy 但数据不足一个块：让 doFinal() 抛出 BadPaddingException
                }

                // 写出可能的 doFinal 尾部（CTR 无 padding，通常为空或少量字节）
                if (cipher != null) {
                    byte[] tail = cipher.doFinal();
                    if (tail != null && tail.length > 0) {
                        out.write(tail);
                    }
                }

                if (!isLegacy && mac != null) {
                    // 常量时间比较 HMAC
                    byte[] computed = mac.doFinal();
                    if (!MessageDigest.isEqual(storedHmac, computed)) {
                        failure = new IOException("HMAC 验证失败，数据可能被篡改");
                    }
                }
            } catch (IOException e) {
                failure = (failure != null) ? failure : e;
            } catch (Exception e) {
                // legacy ECB 路径的 doFinal() 抛 BadPaddingException 时也走此分支，
                // 按 isLegacy 区分错误信息，避免 ECB 失败被误报为 CTR+HMAC 解密失败。
                failure = new IOException(
                        isLegacy ? "旧版 ECB 解密失败（数据损坏或密钥不匹配）" : "CTR+HMAC 解密失败",
                        e);
            } finally {
                try {
                    out.close();
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    }
                }
                // 失败回滚：删除已写出的（可能被篡改的）明文，避免脏数据
                if (failure != null && targetFile != null) {
                    deleteTargetFileSafely(targetFile);
                }
            }

            if (failure != null) {
                throw failure;
            }
        }

        private void deleteTargetFileSafely(File file) {
            try {
                if (file != null && file.exists() && !file.delete()) {
                    LOG.warn("HMAC 校验失败后未能删除目标文件: {}", file.getAbsolutePath());
                }
            } catch (Exception e) {
                LOG.warn("删除目标文件异常: {}", file.getAbsolutePath(), e);
            }
        }
    }
}
