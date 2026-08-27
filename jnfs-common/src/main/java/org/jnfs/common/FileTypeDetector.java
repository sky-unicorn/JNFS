package org.jnfs.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 文件类型识别工具（纯静态函数，零 IO）。
 * <p>
 * 两套映射共享同一套类型标签：
 * <ul>
 *   <li>{@link #fromFilename}：文件名扩展名 → 类型标签（上传提交时同步落库，微秒级）；</li>
 *   <li>{@link #fromMime}：Tika 内容嗅探出的 MIME → 类型标签（后台异步兜底）。</li>
 * </ul>
 * 类型标签统一为小写、不带点（如 {@code docx / txt / jpg / zip}）；
 * 无扩展名 / 无法映射时返回 {@code null}，由调用方决定回退（如展示为"未知"）。
 */
public final class FileTypeDetector {

    /**
     * 扩展名 → 类型标签（不含点，小写）。
     * 同类家族的扩展名归一为同一标签（如 jpg/jpeg → {@code jpg}，htm/html → {@code html}）。
     */
    private static final Map<String, String> EXT_TO_TYPE = buildExtMap();

    /** 类型标签 → 该类型包含的扩展名列表（由 {@link #EXT_TO_TYPE} 反转推导） */
    private static final Map<String, List<String>> TYPE_TO_EXTS = buildTypeToExts();

    private FileTypeDetector() {
    }

    private static Map<String, String> buildExtMap() {
        // LinkedHashMap 保持声明顺序稳定；knownTypes() 最终排序输出，顺序仅影响推导
        Map<String, String> map = new LinkedHashMap<>();
        // ---- 文档 ----
        putSingles(map, "txt", "md", "rtf", "pdf", "doc", "docx", "xls", "xlsx",
                "ppt", "pptx", "csv", "tsv", "odt", "ods", "odp");
        // ---- 文本/代码 ----
        putSingles(map, "json", "xml", "css", "js", "ts", "jsx", "tsx", "vue",
                "java", "kt", "py", "c", "cpp", "h", "hpp", "cs", "go", "rs",
                "php", "rb", "swift", "scala", "sql", "properties", "ini", "cfg",
                "conf", "toml");
        putGroup(map, "yaml", "yaml", "yml");
        putGroup(map, "html", "html", "htm");
        // ---- 图片 ----
        putGroup(map, "jpg", "jpg", "jpeg");
        putGroup(map, "tiff", "tiff", "tif");
        putSingles(map, "png", "gif", "bmp", "svg", "webp", "ico", "heic");
        // ---- 音视频 ----
        putGroup(map, "mpg", "mpg", "mpeg");
        putSingles(map, "mp3", "wav", "flac", "aac", "ogg", "m4a", "mp4", "avi",
                "mkv", "mov", "wmv", "flv", "webm");
        // ---- 压缩/归档 ----
        putSingles(map, "zip", "rar", "7z", "tar", "gz", "bz2", "xz", "jar", "war", "iso");
        // ---- 二进制/可执行 ----
        putSingles(map, "exe", "dll", "so", "dylib", "bin", "apk", "deb", "rpm", "msi", "class");
        // ---- 脚本/其它 ----
        putSingles(map, "sh", "bat", "cmd", "ps1", "log", "key", "pem", "crt", "cer",
                "p12", "db", "sqlite", "bak");
        return map;
    }

    /** 注册"扩展名即类型标签"的单例组（如 txt → txt） */
    private static void putSingles(Map<String, String> map, String... types) {
        for (String type : types) {
            map.put(type, type);
        }
    }

    /** 注册归一标签组：type 为规范标签，exts 为全部等价扩展名（如 jpg → jpg/jpeg） */
    private static void putGroup(Map<String, String> map, String type, String... exts) {
        for (String ext : exts) {
            map.put(ext, type);
        }
    }

    private static Map<String, List<String>> buildTypeToExts() {
        Map<String, List<String>> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : EXT_TO_TYPE.entrySet()) {
            result.computeIfAbsent(entry.getValue(), k -> new ArrayList<>()).add(entry.getKey());
        }
        for (List<String> exts : result.values()) {
            Collections.sort(exts);
        }
        return result;
    }

    /**
     * 按文件名扩展名识别类型。
     *
     * @param filename 原始文件名（null / 空安全）
     * @return 类型标签（小写不带点）；无扩展名或扩展名不在目录中返回 {@code null}
     */
    public static String fromFilename(String filename) {
        if (filename == null) {
            return null;
        }
        int dot = filename.lastIndexOf('.');
        if (dot < 0 || dot == filename.length() - 1) {
            return null;
        }
        String ext = filename.substring(dot + 1).toLowerCase(Locale.ROOT);
        return EXT_TO_TYPE.get(ext);
    }

    /**
     * 按 Tika MIME 类型识别标签（内容嗅探结果）。
     *
     * @param mime Tika 检出的 MIME（可带 {@code ;charset=...} 参数，null 安全）
     * @return 类型标签；MIME 无法映射返回 {@code null}（调用方回退扩展名或"未知"）
     */
    public static String fromMime(String mime) {
        if (mime == null) {
            return null;
        }
        int semi = mime.indexOf(';');
        String bare = (semi >= 0 ? mime.substring(0, semi) : mime).trim().toLowerCase(Locale.ROOT);
        switch (bare) {
            // 文档
            case "application/msword": return "doc";
            case "application/vnd.ms-excel": return "xls";
            case "application/vnd.ms-powerpoint": return "ppt";
            case "application/vnd.openxmlformats-officedocument.wordprocessingml.document": return "docx";
            case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": return "xlsx";
            case "application/vnd.openxmlformats-officedocument.presentationml.presentation": return "pptx";
            case "application/vnd.oasis.opendocument.text": return "odt";
            case "application/vnd.oasis.opendocument.spreadsheet": return "ods";
            case "application/vnd.oasis.opendocument.presentation": return "odp";
            case "application/pdf": return "pdf";
            case "application/rtf": return "rtf";
            case "text/plain": return "txt";
            case "text/markdown": return "md";
            case "text/csv": return "csv";
            case "text/html": return "html";
            case "application/xml":
            case "text/xml": return "xml";
            case "application/json": return "json";
            case "application/javascript":
            case "text/javascript": return "js";
            // 图片
            case "image/jpeg": return "jpg";
            case "image/png": return "png";
            case "image/gif": return "gif";
            case "image/bmp": return "bmp";
            case "image/svg+xml": return "svg";
            case "image/webp": return "webp";
            case "image/tiff": return "tiff";
            case "image/x-icon":
            case "image/vnd.microsoft.icon": return "ico";
            // 音频
            case "audio/mpeg": return "mp3";
            case "audio/mp4":
            case "audio/x-m4a": return "m4a";
            case "audio/wav":
            case "audio/x-wav": return "wav";
            case "audio/flac":
            case "audio/x-flac": return "flac";
            case "audio/aac": return "aac";
            case "audio/ogg":
            case "application/ogg": return "ogg";
            // 视频
            case "video/mp4": return "mp4";
            case "video/x-msvideo": return "avi";
            case "video/quicktime": return "mov";
            case "video/x-matroska": return "mkv";
            case "video/webm": return "webm";
            case "video/x-ms-wmv": return "wmv";
            case "video/mpeg": return "mpg";
            // 压缩/归档
            case "application/zip": return "zip";
            case "application/x-rar-compressed":
            case "application/vnd.rar": return "rar";
            case "application/x-7z-compressed": return "7z";
            case "application/x-tar": return "tar";
            case "application/gzip":
            case "application/x-gzip": return "gz";
            case "application/x-bzip2": return "bz2";
            case "application/x-xz": return "xz";
            case "application/java-archive": return "jar";
            case "application/x-iso9660-image": return "iso";
            // 二进制/可执行
            case "application/vnd.android.package-archive": return "apk";
            case "application/x-msdownload":
            case "application/x-dosexec": return "exe";
            case "application/x-sharedlib": return "so";
            case "application/x-executable": return "bin";
            // 证书/数据库/其它
            case "application/x-pem-file": return "pem";
            case "application/pkix-cert":
            case "application/x-x509-ca-cert": return "crt";
            case "application/x-sqlite3": return "sqlite";
            default: return null;
        }
    }

    /**
     * 类型标签对应的扩展名列表（用于"扩展名兜底筛选"：旧数据 file_type 为 NULL 时按
     * {@code filename LIKE '%.<ext>'} 匹配）。未知类型返回空列表。
     */
    public static List<String> extensionsOfType(String type) {
        if (type == null) {
            return Collections.emptyList();
        }
        List<String> exts = TYPE_TO_EXTS.get(type.toLowerCase(Locale.ROOT));
        return exts != null ? exts : Collections.emptyList();
    }

    /**
     * 全目录类型标签（去重、升序）。供文件类型筛选下拉框作为候选。
     */
    public static List<String> knownTypes() {
        List<String> types = new ArrayList<>(TYPE_TO_EXTS.keySet());
        Collections.sort(types);
        return types;
    }
}
