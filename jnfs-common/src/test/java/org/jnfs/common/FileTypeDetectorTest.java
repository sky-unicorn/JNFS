package org.jnfs.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FileTypeDetector 单元测试：扩展名/MIME 两套映射与归一标签、目录一致性。
 */
class FileTypeDetectorTest {

    // ==================== 扩展名识别 ====================

    @Test
    void fromFilenameRecognizesCommonExtensions() {
        assertEquals("doc", FileTypeDetector.fromFilename("report.DOC"));
        assertEquals("docx", FileTypeDetector.fromFilename("spec.docx"));
        assertEquals("txt", FileTypeDetector.fromFilename("notes.txt"));
        assertEquals("pdf", FileTypeDetector.fromFilename("manual.pdf"));
        assertEquals("jpg", FileTypeDetector.fromFilename("photo.JPEG"));
        assertEquals("html", FileTypeDetector.fromFilename("index.htm"));
        assertEquals("yaml", FileTypeDetector.fromFilename("config.yml"));
        assertEquals("tar", FileTypeDetector.fromFilename("backup.tar"));
        assertEquals("gz", FileTypeDetector.fromFilename("backup.tar.gz"));
    }

    @Test
    void fromFilenameReturnsNullForUnknownOrMissingExtension() {
        assertNull(FileTypeDetector.fromFilename("no-extension"), "无扩展名应返回 null");
        assertNull(FileTypeDetector.fromFilename("trailingdot."), "尾部点应返回 null");
        assertNull(FileTypeDetector.fromFilename("archive.unknownext"), "目录外扩展名应返回 null");
        assertNull(FileTypeDetector.fromFilename(null), "null 文件名应返回 null");
    }

    // ==================== MIME 识别 ====================

    @Test
    void fromMimeMapsCommonTypesAndStripsParams() {
        assertEquals("doc", FileTypeDetector.fromMime("application/msword"));
        assertEquals("docx", FileTypeDetector.fromMime(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"));
        assertEquals("txt", FileTypeDetector.fromMime("text/plain; charset=UTF-8"));
        assertEquals("jpg", FileTypeDetector.fromMime("image/jpeg"));
        assertEquals("mp4", FileTypeDetector.fromMime("video/mp4"));
        assertEquals("zip", FileTypeDetector.fromMime("application/zip"));
    }

    @Test
    void fromMimeReturnsNullForUnmappable() {
        assertNull(FileTypeDetector.fromMime("application/octet-stream"));
        assertNull(FileTypeDetector.fromMime("application/x-unknown-thing"));
        assertNull(FileTypeDetector.fromMime(null));
    }

    // ==================== 目录一致性 ====================

    @Test
    void extensionsOfTypeConsistentWithFromFilename() {
        // 归一标签组：jpg 应同时含 jpg/jpeg，且 fromFilename 对两者都返回 jpg
        List<String> jpgExts = FileTypeDetector.extensionsOfType("jpg");
        assertTrue(jpgExts.contains("jpg") && jpgExts.contains("jpeg"),
                "jpg 标签应包含 jpg/jpeg 两个扩展名");
        for (String ext : jpgExts) {
            assertEquals("jpg", FileTypeDetector.fromFilename("f." + ext));
        }
        assertTrue(FileTypeDetector.extensionsOfType("unknown-type").isEmpty(),
                "目录外类型应返回空扩展名列表");
    }

    @Test
    void knownTypesCoversAllExtensionValuesAndSorted() {
        List<String> types = FileTypeDetector.knownTypes();
        assertTrue(types.contains("txt") && types.contains("docx")
                && types.contains("jpg") && types.contains("zip"), "目录应含常见类型");
        for (int i = 1; i < types.size(); i++) {
            assertTrue(types.get(i - 1).compareTo(types.get(i)) < 0, "knownTypes 应升序");
        }
    }
}
