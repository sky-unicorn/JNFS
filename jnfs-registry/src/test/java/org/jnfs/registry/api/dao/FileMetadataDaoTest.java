package org.jnfs.registry.api.dao;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * FileMetadataDao 单元测试：内存 H2（MODE=MariaDB，贴近 h2 运行时）验证
 * 分页/筛选 SQL 的两方言通用性、NULL file_size 语义与扩展名兜底筛选。
 */
class FileMetadataDaoTest {

    private static final String URL =
            "jdbc:h2:mem:filesdao;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_ON_EXIT=FALSE";

    private HikariDataSource dataSource;
    private FileMetadataDao dao;

    @BeforeEach
    void setUp() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(2);
        dataSource = new HikariDataSource(config);
        dao = new FileMetadataDao(dataSource);

        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE file_metadata ("
                    + "storage_id CHAR(36) PRIMARY KEY, "
                    + "filename VARCHAR(255) NOT NULL, "
                    + "file_hash CHAR(64) NOT NULL, "
                    + "file_size BIGINT DEFAULT NULL, "
                    + "file_type VARCHAR(32) DEFAULT NULL, "
                    + "replication_factor TINYINT NOT NULL DEFAULT 1, "
                    + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP)");
            stmt.execute("CREATE TABLE file_location ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
                    + "file_hash CHAR(64) NOT NULL, "
                    + "datanode_id VARCHAR(128) DEFAULT NULL, "
                    + "datanode_addr VARCHAR(100) DEFAULT NULL, "
                    + "replica_role TINYINT NOT NULL DEFAULT 0, "
                    + "status TINYINT NOT NULL DEFAULT 1)");
            // 1. 有类型有大小（新上传）
            stmt.execute("INSERT INTO file_metadata (storage_id, filename, file_hash, file_size, file_type) "
                    + "VALUES ('s1', 'report.pdf', 'h1', 1024, 'pdf')");
            // 2. 类型 NULL + 大小 NULL + 扩展名 pdf（旧数据回填未覆盖场景，靠扩展名兜底筛选）
            stmt.execute("INSERT INTO file_metadata (storage_id, filename, file_hash) "
                    + "VALUES ('s2', 'legacy.PDF', 'h2')");
            // 3. 大小 NULL + 类型 bin（旧数据大小未知）
            stmt.execute("INSERT INTO file_metadata (storage_id, filename, file_hash, file_type) "
                    + "VALUES ('s3', 'data.bin', 'h3', 'bin')");
            stmt.execute("INSERT INTO file_location (file_hash, datanode_id, datanode_addr) "
                    + "VALUES ('h1', 'node-1', '10.0.0.1:9000')");
            stmt.execute("INSERT INTO file_location (file_hash, datanode_addr) VALUES ('h2', '10.0.0.2:9000')");
        }
    }

    @AfterEach
    void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    void queryFilesKeepsNullSizeAsNull() throws Exception {
        FileMetadataDao.Page page = dao.queryFiles(new FileMetadataDao.Filter(null, null, null, null), 1, 20);
        assertEquals(3, page.total);
        Map<String, FileMetadataDao.FileRow> byId = new java.util.HashMap<>();
        for (FileMetadataDao.FileRow row : page.rows) {
            byId.put(row.storageId, row);
        }
        assertEquals(1024L, byId.get("s1").fileSize, "有大小行应原样返回");
        assertNull(byId.get("s2").fileSize, "NULL 大小必须保持 NULL（regression: wasNull 顺序错误会变 0）");
        assertNull(byId.get("s3").fileSize, "NULL 大小必须保持 NULL");
    }

    @Test
    void fileTypeFilterFallsBackToExtensionForNullType() throws Exception {
        FileMetadataDao.Page byPdf = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, "pdf", null), 1, 20);
        assertEquals(2, byPdf.total, "pdf 应匹配存储值行 + NULL 类型扩展名兜底行");
        List<String> pdfNames = byPdf.rows.stream().map(r -> r.filename).sorted().toList();
        assertEquals(Arrays.asList("legacy.PDF", "report.pdf"), pdfNames);

        FileMetadataDao.Page byBin = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, "bin", null), 1, 20);
        assertEquals(1, byBin.total, "bin 应仅匹配存储值行");

        FileMetadataDao.Page byZip = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, "zip", null), 1, 20);
        assertEquals(0, byZip.total, "无 zip 文件应返回 0");
    }

    @Test
    void nodeFilterMatchesDatanodeIdAndLegacyAddr() throws Exception {
        // 匹配 datanode_id
        FileMetadataDao.Page byNode = dao.queryFiles(
                new FileMetadataDao.Filter("node-1", "10.0.0.1:9000", null, null), 1, 20);
        assertEquals(1, byNode.total, "node-1 应匹配 h1");
        assertEquals("h1", byNode.rows.get(0).fileHash);
        // 仅 datanode_addr 有值的旧数据（nodeId 为地址本身）
        FileMetadataDao.Page byLegacy = dao.queryFiles(
                new FileMetadataDao.Filter(null, "10.0.0.2:9000", null, null), 1, 20);
        assertEquals(1, byLegacy.total, "datanode_addr 兜底应匹配 h2");
        assertEquals("h2", byLegacy.rows.get(0).fileHash);
    }

    @Test
    void storageIdFilterEscapesWildcards() throws Exception {
        // storage_id 含 LIKE 通配字符：只有转义后按字面匹配才算命中
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO file_metadata (storage_id, filename, file_hash) "
                    + "VALUES ('100%_notes', 'note.txt', 'h4')");
        }
        FileMetadataDao.Page hit = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, null, "100%_notes"), 1, 20);
        assertEquals(1, hit.total, "转义后的通配符应按字面匹配");
        assertEquals("100%_notes", hit.rows.get(0).storageId);

        // 下划线必须按字面处理：若未转义，LIKE 会把 _ 当单字符通配而误命中
        FileMetadataDao.Page miss = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, null, "notes_txt"), 1, 20);
        assertEquals(0, miss.total, "下划线转义后不应作为通配符命中");
    }

    @Test
    void unknownTypeFilterMatchesStoredAndNull() throws Exception {
        // s2 无 file_type（NULL）；插入一行显式 'unknown' 存值
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO file_metadata (storage_id, filename, file_hash, file_type) "
                    + "VALUES ('s4', 'noext', 'h4', 'unknown')");
        }
        FileMetadataDao.Page page = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, "unknown", null), 1, 20);
        assertEquals(2, page.total, "unknown 应匹配存值 'unknown' 行 + NULL 类型行（两者展示均为未知）");
        // 已识别类型的行不应被 unknown 命中
        FileMetadataDao.Page notPdf = dao.queryFiles(
                new FileMetadataDao.Filter(null, null, "unknown", null), 1, 20);
        assertEquals(2, notPdf.total);
    }

    @Test
    void queryReplicasGroupsByHash() throws Exception {
        Map<String, List<FileMetadataDao.Replica>> reps = dao.queryReplicas(Arrays.asList("h1", "h2"));
        assertEquals(1, reps.get("h1").size());
        assertEquals("node-1", reps.get("h1").get(0).nodeId);
        // datanode_id 为空时 nodeId 回退 datanode_addr
        assertEquals("10.0.0.2:9000", reps.get("h2").get(0).nodeId);
    }
}
