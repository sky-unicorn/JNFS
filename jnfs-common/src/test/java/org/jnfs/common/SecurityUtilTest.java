package org.jnfs.common;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SecurityUtil.decryptHead 单元测试：v1 格式密文前缀解密（类型嗅探场景）。
 */
class SecurityUtilTest {

    private final SecurityUtil util =
            new SecurityUtil(SecurityConfig.getAesKey());

    @Test
    void decryptHeadRoundTripsPrefix() throws Exception {
        // 明文长度 1000（超过头部嗅探的 8KB 上限内的任意值均可）
        byte[] plain = new byte[1000];
        for (int i = 0; i < plain.length; i++) {
            plain[i] = (byte) (i % 251);
        }
        byte[] enc = util.encryptBytes(plain);

        // 模拟 DataNode 读回前 49+8192 字节（这里全量），解密头应与明文前缀一致
        byte[] head = util.decryptHead(enc);
        assertArrayEquals(plain, head, "前缀解密结果应与明文一致（CTR 流密码前缀可独立解密）");
    }

    @Test
    void decryptHeadOnTruncatedInputDecryptsAvailablePrefix() throws Exception {
        byte[] plain = "hello-jnfs-content-sniffing".getBytes(StandardCharsets.UTF_8);
        byte[] enc = util.encryptBytes(plain);

        // 截取 header + 前 5 字节密文（对应明文前 5 字节）
        int truncated = SecurityUtil.HEADER_LENGTH + 5;
        byte[] prefix = java.util.Arrays.copyOf(enc, truncated);
        byte[] head = util.decryptHead(prefix);
        assertEquals(5, head.length, "截断前缀应只解出 5 字节明文");
        assertArrayEquals(java.util.Arrays.copyOf(plain, 5), head);
    }

    @Test
    void decryptHeadOnShortOrLegacyInputReturnsEmpty() throws Exception {
        // 不足 header 长度
        assertTrue(util.decryptHead(new byte[]{0x01, 0x02}).length == 0,
                "短于 header 的输入应返回空数组");
        // 非 v1 首字节（legacy ECB 格式无法前缀解密）
        assertTrue(util.decryptHead(new byte[100]).length == 0,
                "非 v1 首字节应返回空数组");
        assertTrue(util.decryptHead(null).length == 0, "null 输入应返回空数组");
    }
}
