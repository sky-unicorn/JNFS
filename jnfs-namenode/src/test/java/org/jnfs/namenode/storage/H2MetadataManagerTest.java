package org.jnfs.namenode.storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jnfs.common.migration.MigrationResult;
import org.jnfs.common.migration.MigrationRunner;
import org.jnfs.common.migration.StorageMode;
import org.jnfs.namenode.H2MetadataManager;
import org.jnfs.namenode.MetadataCacheManager;
import org.jnfs.namenode.ReplicaLocation;
import org.jnfs.common.replication.ReplicaRole;
import org.jnfs.common.replication.ReplicaStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * H2MetadataManager 集成测试：用 H2 mem 库验证 JdbcMetadataManager 共享 JDBC 逻辑。
 * <p>
 * 覆盖：
 * <ul>
 *   <li>isJdbcBacked()==true、getDataSource() 非空</li>
 *   <li>logAddFile 事务（写 file_metadata + file_location，重复 storage_id 用 INSERT IGNORE 幂等）</li>
 *   <li>queryByHash（单副本返回 PRIMARY location、真实 filename 而非 "loaded_from_file"）</li>
 *   <li>queryHashByStorageId、isFileExist</li>
 *   <li>tryAcquireUploadLock（首次 true、重复键经 dialect.isDuplicateKeyError 判定返回 false）、releaseUploadLock</li>
 *   <li>backfillDataNodeIds（构造样例验证幂等）</li>
 * </ul>
 */
class H2MetadataManagerTest {

    private static final String URL =
            "jdbc:h2:mem:metamgr;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_ON_EXIT=FALSE";

    private HikariDataSource dataSource;
    private H2MetadataManager manager;
    private File dataDir;

    @BeforeEach
    void setUp() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(2);
        dataSource = new HikariDataSource(config);

        // 先跑迁移链建表（H2 全链建表）
        dataDir = new File(System.getProperty("java.io.tmpdir"),
                "h2metamgr-" + System.nanoTime());
        dataDir.mkdirs();

        MigrationResult result = MigrationRunner.run(StorageMode.H2, dataDir, dataSource);
        assertTrue(result.isSuccess(), "H2 迁移应成功: " + result.getMessage());

        // 构造 H2MetadataManager（父类构造时执行锚点表 DDL 兜底，已存在则 no-op）
        manager = new H2MetadataManager(dataSource);
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

    // ==================== 能力探测 ====================

    @Test
    void isJdbcBackedReturnsTrue() {
        assertTrue(manager.isJdbcBacked(), "H2MetadataManager.isJdbcBacked() 应返回 true");
    }

    @Test
    void getDataSourceReturnsNonNull() {
        assertNotNull(manager.getDataSource(), "H2MetadataManager.getDataSource() 应非空");
        assertSame(dataSource, manager.getDataSource(), "getDataSource() 应返回构造时注入的池");
    }

    // ==================== logAddFile 事务 ====================

    @Test
    void logAddFileWritesMetadataAndLocation() throws Exception {
        List<ReplicaLocation> locs = Collections.singletonList(
                new ReplicaLocation("node-1", ReplicaRole.PRIMARY.getCode(), ReplicaStatus.ACTIVE.getCode()));
        manager.logAddFile("test.txt", "hash-abc", "storage-001", 1, locs);

        // 验证 file_metadata
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT filename, file_hash, storage_id, replication_factor FROM file_metadata WHERE storage_id='storage-001'")) {
            assertTrue(rs.next(), "file_metadata 应有 1 行");
            assertEquals("test.txt", rs.getString("filename"));
            assertEquals("hash-abc", rs.getString("file_hash"));
            assertEquals("storage-001", rs.getString("storage_id"));
            assertEquals(1, rs.getInt("replication_factor"));
        }

        // 验证 file_location
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT file_hash, datanode_id, replica_role, status FROM file_location WHERE file_hash='hash-abc'")) {
            assertTrue(rs.next(), "file_location 应有 1 行");
            assertEquals("hash-abc", rs.getString("file_hash"));
            assertEquals("node-1", rs.getString("datanode_id"));
            assertEquals(0, rs.getInt("replica_role")); // PRIMARY
            assertEquals(1, rs.getInt("status"));        // ACTIVE
        }
    }

    @Test
    void logAddFileWithDuplicateStorageIdIsRejected() throws Exception {
        List<ReplicaLocation> locs = Collections.singletonList(
                new ReplicaLocation("node-1", ReplicaRole.PRIMARY.getCode(), ReplicaStatus.ACTIVE.getCode()));
        // 首次写入
        manager.logAddFile("dup.txt", "hash-dup", "storage-dup", 1, locs);
        // 重复 storage_id：file_metadata 主键冲突（storage_id 为 PRIMARY KEY）
        // logAddFile 仅对 file_location 用 INSERT IGNORE；file_metadata 重复 storage_id 会拒绝写入
        assertThrows(IOException.class, () ->
                manager.logAddFile("dup2.txt", "hash-dup2", "storage-dup", 1, locs),
                "重复 storage_id 的 INSERT 应抛 IOException（包装自 SQLException 主键冲突）");

        // 验证 file_metadata 只有一行（事务回滚，无半写状态）
        assertEquals(1, countRows("file_metadata", "storage_id='storage-dup'"),
                "重复 storage_id 不应产生重复行");
        // 事务回滚：file_location 也不应有 hash-dup2 的残留行
        assertEquals(0, countRows("file_location", "file_hash='hash-dup2'"),
                "事务回滚后 file_location 不应有残留行");
    }

    @Test
    void insertIgnoreFileLocationIsIdempotent() throws Exception {
        // 先插入 file_metadata，再验证 file_location 的 INSERT IGNORE 幂等（事务内第二段逻辑）
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_metadata (storage_id, filename, file_hash, replication_factor) " +
                    "VALUES ('storage-loc', 'loc.txt', 'hash-loc', 1)");
        }

        String insertIgnore = "INSERT IGNORE INTO file_location " +
                "(file_hash, datanode_id, datanode_addr, status, replica_role) " +
                "VALUES ('hash-loc', 'node-loc', '10.0.0.9:9000', 1, 0)";
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(insertIgnore);
            // 重复执行同一 INSERT IGNORE（同一 (file_hash, datanode_id) 被 uk_hash_node 唯一约束去重）
            conn.createStatement().executeUpdate(insertIgnore);
        }
        assertEquals(1, countRows("file_location", "file_hash='hash-loc'"),
                "INSERT IGNORE file_location 幂等：不应产生重复行");
    }

    // ==================== queryByHash ====================

    @Test
    void queryByHashReturnsPrimaryLocationWithRealFilename() throws Exception {
        // 插入测试数据
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_metadata (storage_id, filename, file_hash, replication_factor) " +
                    "VALUES ('storage-q', 'real-file.txt', 'hash-q', 1)");
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_location (file_hash, datanode_id, datanode_addr, status, replica_role) " +
                    "VALUES ('hash-q', 'node-q', '10.0.0.1:9000', 1, 0)");
        }

        MetadataCacheManager.MetadataEntry entry = manager.queryByHash("hash-q");
        assertNotNull(entry, "queryByHash 应返回非 null");
        assertEquals("real-file.txt", entry.filename, "filename 应为真实文件名，而非 'loaded_from_file'");
        assertEquals("hash-q", entry.hash);
        assertEquals("storage-q", entry.storageId);
        assertEquals(1, entry.locations.size());
        assertEquals("node-q", entry.getPrimaryNodeId());
        assertEquals(ReplicaRole.PRIMARY.getCode(), entry.getPrimaryLocation().getRole());
    }

    @Test
    void queryByHashReturnsNullForNonExistent() {
        MetadataCacheManager.MetadataEntry entry = manager.queryByHash("nonexistent-hash");
        assertNull(entry, "不存在的 hash 应返回 null");
    }

    // ==================== queryHashByStorageId ====================

    @Test
    void queryHashByStorageIdReturnsHash() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_metadata (storage_id, filename, file_hash, replication_factor) " +
                    "VALUES ('sid-1', 'f.txt', 'h-1', 1)");
        }
        assertEquals("h-1", manager.queryHashByStorageId("sid-1"));
        assertNull(manager.queryHashByStorageId("nonexistent-sid"));
    }

    // ==================== isFileExist ====================

    @Test
    void isFileExistReturnsTrueForExistingFile() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_metadata (storage_id, filename, file_hash, replication_factor) " +
                    "VALUES ('sid-exist', 'exist.txt', 'hash-exist', 1)");
        }
        assertTrue(manager.isFileExist("hash-exist"));
        assertFalse(manager.isFileExist("hash-not-exist"));
    }

    // ==================== tryAcquireUploadLock / releaseUploadLock ====================

    @Test
    void tryAcquireUploadLockFirstSucceedsDuplicateFails() {
        // 首次获取锁
        boolean acquired1 = manager.tryAcquireUploadLock("hash-lock", "namenode-1");
        assertTrue(acquired1, "首次获取锁应成功");

        // 重复获取（同一 hash，不同 namenode_id）应失败（唯一键冲突）
        boolean acquired2 = manager.tryAcquireUploadLock("hash-lock", "namenode-2");
        assertFalse(acquired2, "重复获取锁应失败（唯一键冲突）");
    }

    @Test
    void releaseUploadLockAllowsReacquire() {
        manager.tryAcquireUploadLock("hash-rel", "nn-1");
        manager.releaseUploadLock("hash-rel");
        // 释放后应能重新获取
        boolean reacquired = manager.tryAcquireUploadLock("hash-rel", "nn-2");
        assertTrue(reacquired, "释放锁后应能重新获取");
    }

    @Test
    void tryAcquireUploadLockCleansExpiredLock() throws SQLException {
        // 直接插入一条已过期的锁行（expire_time 在过去 60 分钟）
        // 覆盖 JdbcMetadataManager.tryAcquireUploadLock 的过期锁清理路径（DELETE ... expire_time < now）
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO file_upload_lock (file_hash, namenode_id, expire_time) " +
                    "VALUES ('hash-expired', 'old-nn', DATEADD('MINUTE', -60, CURRENT_TIMESTAMP))");
        }
        // tryAcquireUploadLock 应先清理过期锁，再插入新锁，返回 true（而非因唯一键冲突返回 false）
        boolean acquired = manager.tryAcquireUploadLock("hash-expired", "new-nn");
        assertTrue(acquired, "已过期的锁应被清理，新 namenode 应能获取锁");
        // 验证锁持有者已是 new-nn
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT namenode_id FROM file_upload_lock WHERE file_hash = 'hash-expired'")) {
            assertTrue(rs.next(), "锁行应存在");
            assertEquals("new-nn", rs.getString(1), "锁应被新 namenode 持有");
        }
    }

    // ==================== backfillDataNodeIds ====================

    @Test
    void backfillDataNodeIdsWithNoMappingReturnsZero() {
        // 无映射时返回 0
        int result = manager.backfillDataNodeIds();
        assertEquals(0, result, "无映射时 backfillDataNodeIds 应返回 0");
    }

    @Test
    void backfillDataNodeIdsUpdatesNullDatanodeId() throws Exception {
        // 插入一条 datanode_id=NULL 的 file_location
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_metadata (storage_id, filename, file_hash, replication_factor) " +
                    "VALUES ('sid-bf', 'bf.txt', 'hash-bf', 1)");
            conn.createStatement().executeUpdate(
                    "INSERT INTO file_location (file_hash, datanode_id, datanode_addr, status, replica_role) " +
                    "VALUES ('hash-bf', NULL, '10.0.0.5:9000', 1, 0)");
        }

        // 注册映射：10.0.0.5:9000 → node-bf
        org.jnfs.common.NodeAddressResolver.updateMappingFromDataNodes(
                java.util.Collections.singletonList("node-bf|10.0.0.5:9000|1000"));

        int updated = manager.backfillDataNodeIds();
        assertTrue(updated > 0, "backfillDataNodeIds 应补全至少 1 条记录");

        // 验证 datanode_id 已补全
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT datanode_id FROM file_location WHERE file_hash='hash-bf'")) {
            assertTrue(rs.next());
            assertEquals("node-bf", rs.getString("datanode_id"));
        }

        // 幂等：再次 backfill 应返回 0（已无 NULL 行）
        int updated2 = manager.backfillDataNodeIds();
        assertEquals(0, updated2, "幂等 backfill 应返回 0（已无 NULL 行）");
    }

    // ==================== recover (no-op) ====================

    @Test
    void recoverIsNoOpForJdbc() throws Exception {
        // JDBC 模式 recover 应为 no-op，不抛异常
        java.util.Map<String, String> f2h = new java.util.HashMap<>();
        java.util.Map<String, String> h2s = new java.util.HashMap<>();
        java.util.Map<String, String> h2id = new java.util.HashMap<>();
        java.util.Set<String> ph = java.util.concurrent.ConcurrentHashMap.newKeySet();
        manager.recover(f2h, h2s, h2id, ph);
        assertTrue(f2h.isEmpty(), "JDBC 模式 recover 不应灌入数据");
        assertTrue(h2s.isEmpty());
        assertTrue(h2id.isEmpty());
    }

    // ==================== 工具方法 ====================

    private int countRows(String table, String where) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE " + where)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }
}
