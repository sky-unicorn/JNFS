package org.jnfs.namenode.migration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.MigrationResult;
import org.jnfs.common.migration.MigrationRunner;
import org.jnfs.common.migration.StorageMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H2 迁移链集成测试：验证 H2 全链建表 + 幂等重跑 + file→H2 数据导入。
 * <p>
 * H2 全新部署首启不走 "写 CURRENT_VERSION" 捷径（detectH2Version freshDeployHint=false），
 * 版本 0 起步走完整迁移链（V0→V1→...→V7），全部表由迁移步骤自身创建。
 * <p>
 * 覆盖：
 * <ol>
 *   <li>全链建表：schema_version=7，mysql/jnfs.sql 中全部 10 张表存在</li>
 *   <li>幂等重跑：第二次 run() 无副作用、无异常、版本仍为 7</li>
 *   <li>FileToH2Importer：规整后的 namenode_meta.log 导入 file_metadata/file_location，
 *       重入（标记跳过）不产生重复行；导入行带扩展名推导的 file_type、file_size 为 NULL</li>
 * </ol>
 */
class H2MigrationChainTest {

    private static final String URL =
            "jdbc:h2:mem:migtest;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_ON_EXIT=FALSE";

    /** mysql/jnfs.sql 中 NameNode 迁移链负责的全部表 */
    private static final String[] ALL_TABLES = {
            "schema_version", "node_registry", "file_metadata", "file_location",
            "file_upload_lock", "replication_group", "replica_sync_task",
            "replication_policy", "replication_control", "node_drain"
    };

    private HikariDataSource dataSource;
    private File dataDir;

    @BeforeEach
    void setUp() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(2);
        dataSource = new HikariDataSource(config);
        dataDir = new File(System.getProperty("java.io.tmpdir"),
                "h2migtest-" + System.nanoTime());
        dataDir.mkdirs();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (dataSource != null) {
            dataSource.close();
        }
        if (dataDir != null && dataDir.exists()) {
            try (var paths = Files.walk(dataDir.toPath())) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    @Test
    void fullChainBuildsH2SchemaAndIsIdempotent() throws Exception {
        // === 首次 run()：H2 全新部署走全链建表 ===
        MigrationResult first = MigrationRunner.run(StorageMode.H2, dataDir, dataSource);
        assertTrue(first.isSuccess(), "H2 首次迁移应成功: " + first.getMessage());

        // schema_version = 7
        assertEquals(7, readSchemaVersion(), "迁移后 schema_version 应为 7");

        // mysql/jnfs.sql 全部表存在
        try (Connection conn = dataSource.getConnection()) {
            JdbcDialect dialect = JdbcDialect.dialectFor(StorageMode.H2);
            for (String table : ALL_TABLES) {
                assertTrue(dialect.tableExists(conn, table),
                        "H2 全链建表后表 " + table + " 应存在");
            }
            // V6：node_registry.free_space 列存在
            assertTrue(dialect.columnExists(conn, "node_registry", "free_space"),
                    "V6 后 node_registry.free_space 列应存在");
            // V7：file_metadata.file_type 列存在
            assertTrue(dialect.columnExists(conn, "file_metadata", "file_type"),
                    "V7 后 file_metadata.file_type 列应存在");
        }

        // 种子行（V2→V3 写入）应存在
        assertEquals(1, countRows("replication_policy"),
                "replication_policy 种子行应存在");
        assertEquals(1, countRows("replication_control"),
                "replication_control 种子行应存在");

        // === 重跑：幂等无副作用无异常 ===
        MigrationResult second = MigrationRunner.run(StorageMode.H2, dataDir, dataSource);
        assertTrue(second.isSuccess(), "H2 重跑迁移应成功: " + second.getMessage());
        assertEquals(7, readSchemaVersion(), "重跑后 schema_version 仍应为 7");
        try (Connection conn = dataSource.getConnection()) {
            JdbcDialect dialect = JdbcDialect.dialectFor(StorageMode.H2);
            for (String table : ALL_TABLES) {
                assertTrue(dialect.tableExists(conn, table),
                        "重跑后表 " + table + " 应仍存在");
            }
        }
        assertEquals(1, countRows("replication_policy"), "重跑后种子行不应重复");
        assertEquals(1, countRows("replication_control"), "重跑后种子行不应重复");
    }

    @Test
    void fileToH2ImporterImportsMetadataIdempotently() throws Exception {
        // 先建好 H2 schema（全链）
        MigrationResult result = MigrationRunner.run(StorageMode.H2, dataDir, dataSource);
        assertTrue(result.isSuccess(), "H2 迁移应成功: " + result.getMessage());

        // 准备规整后的 file 日志（V1 格式 ADD|filename|hash|node_id|storageId）
        // 第 1、2 行同 hash 不同地址（模拟多副本历史），第 3 行 host:port 地址
        File logFile = new File(dataDir, "namenode_meta.log");
        Files.writeString(logFile.toPath(),
                "ADD|a.txt|hash-a|192.168.1.10:9000|storage-1\n"
                        + "ADD|a.txt|hash-a|node-1|storage-1\n"
                        + "ADD|b.bin|hash-b|192.168.1.11:9000|storage-2\n",
                StandardCharsets.UTF_8);

        FileToH2Importer.importIfApplicable(dataDir, dataSource);

        // file_metadata：3 行（storage_id 主键 INSERT IGNORE，storage-1/a/hash-a 去重保留 1 条）
        assertEquals(2, countRows("file_metadata"), "file_metadata 应导入 2 条（storage-1/storage-2）");
        // file_location：3 行（hash-a 两条不同地址 + hash-b 一条）
        assertEquals(3, countRows("file_location"), "file_location 应导入 3 条");

        // 完成标记已写
        assertTrue(new File(dataDir, "file_to_h2_imported").exists(),
                "导入完成标记 file_to_h2_imported 应存在");

        // 重跑：标记命中 → 跳过，无副作用
        FileToH2Importer.importIfApplicable(dataDir, dataSource);
        assertEquals(2, countRows("file_metadata"), "重跑后 file_metadata 行数不应变化");
        assertEquals(3, countRows("file_location"), "重跑后 file_location 行数不应变化");

        // legacy file 导入数据为单副本：file_metadata.replication_factor 均为默认 1
        // （用户可在 Dashboard 配置冗余组后，新上传文件按组大小多副本）
        // V7 语义：导入行 file_type 按扩展名推导、file_size 为 NULL（大小未知，后台回填）
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT storage_id, filename, file_hash, replication_factor, file_type, file_size "
                             + "FROM file_metadata ORDER BY storage_id")) {
            // 直接断言最小契约：replication_factor 全部为 1
            try (ResultSet rs = ps.executeQuery()) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                    assertEquals(1, rs.getInt("replication_factor"),
                            "legacy file 导入数据为单副本：replication_factor 应为 1");
                    String filename = rs.getString("filename");
                    String expectedType = "a.txt".equals(filename) ? "txt" : "bin";
                    assertEquals(expectedType, rs.getString("file_type"),
                            filename + " 导入行 file_type 应扩展名推导为 " + expectedType);
                    long size = rs.getLong("file_size");
                    assertTrue(rs.wasNull(), "导入行 file_size 应为 NULL（大小未知）");
                    assertTrue(size == 0, "wasNull 成立时取值为 0（防御断言）");
                }
                assertEquals(2, rows, "file_metadata 应有 2 行");
            }
        }
    }

    // ==================== 工具方法 ====================

    private int readSchemaVersion() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")) {
            assertTrue(rs.next(), "schema_version 表应有记录");
            return rs.getInt(1);
        }
    }

    private int countRows(String table) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
            assertTrue(rs.next(), "COUNT 查询应返回一行");
            return rs.getInt(1);
        }
    }
}