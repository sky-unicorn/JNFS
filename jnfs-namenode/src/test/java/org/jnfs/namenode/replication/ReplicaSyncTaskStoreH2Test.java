package org.jnfs.namenode.replication;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jnfs.common.migration.MigrationResult;
import org.jnfs.common.migration.MigrationRunner;
import org.jnfs.common.migration.StorageMode;
import org.jnfs.common.replication.ReplicaSyncTask;
import org.jnfs.common.replication.SyncTaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ReplicaSyncTaskStore 在 H2 下的方言回归测试。
 * <p>
 * 背景：findStaleInFlight 原用 mysql 专有的 {@code NOW() - INTERVAL ? MINUTE}，H2 MariaDB 模式
 * 不支持（H2DialectProbeTest 探针 i）。H2 模式启用副本后该 Store 会被构造，此处守护
 * 改用应用侧参数化 Timestamp 后的 SQL 在 H2 上可正确执行且语义正确。
 */
class ReplicaSyncTaskStoreH2Test {

    private static final String URL =
            "jdbc:h2:mem:synctask;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_ON_EXIT=FALSE";

    private HikariDataSource dataSource;
    private ReplicaSyncTaskStore store;
    private File dataDir;

    @BeforeEach
    void setUp() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(URL);
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(2);
        dataSource = new HikariDataSource(config);

        // 跑 H2 全链迁移建表（replica_sync_task 由 MysqlV1ToV2 在 H2 上建立）
        dataDir = new File(System.getProperty("java.io.tmpdir"),
                "synctask-" + System.nanoTime());
        dataDir.mkdirs();
        MigrationResult result = MigrationRunner.run(StorageMode.H2, dataDir, dataSource);
        assertTrue(result.isSuccess(), "H2 迁移应成功: " + result.getMessage());

        store = new ReplicaSyncTaskStore(dataSource);
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
    void findStaleInFlightOnlyReturnsOldTasks() throws SQLException {
        // 插入 2 行 IN_FLIGHT：一行 40 分钟前（应被检出）、一行 5 分钟前（不应被检出）
        insertTask("task-old", "hash-old", "node-src", "node-dst", 10L,
                SyncTaskStatus.IN_FLIGHT.getCode(), 0,
                new Timestamp(System.currentTimeMillis() - 40L * 60_000L));
        insertTask("task-fresh", "hash-fresh", "node-src", "node-dst", 10L,
                SyncTaskStatus.IN_FLIGHT.getCode(), 0,
                new Timestamp(System.currentTimeMillis() - 5L * 60_000L));

        List<ReplicaSyncTask> stale = store.findStaleInFlight(30);
        assertEquals(1, stale.size(), "30 分钟阈值应只回收 40 分钟前的那行");
        assertEquals("task-old", stale.get(0).getTaskId());
    }

    @Test
    void findStaleInFlightExcludesPending() throws SQLException {
        // PENDING 行即便很旧也不应被 IN_FLIGHT 回收器检出
        insertTask("task-pending", "hash-pending", "node-src", "node-dst", 10L,
                SyncTaskStatus.PENDING.getCode(), 0,
                new Timestamp(System.currentTimeMillis() - 40L * 60_000L));

        List<ReplicaSyncTask> stale = store.findStaleInFlight(30);
        assertTrue(stale.isEmpty(), "PENDING 行不应被 IN_FLIGHT 回收器检出");
    }

    @Test
    void upsertPendingIdempotent() throws SQLException {
        ReplicaSyncTask t = new ReplicaSyncTask();
        t.setTaskId("task-upsert");
        t.setFileHash("hash-up");
        t.setSourceNode("node-src");
        t.setTargetNode("node-dst");
        t.setFileSize(100L);
        // 重复入队同一 (file_hash, target_node) 不新增行（uk_hash_target 去重）
        store.upsertPending(t);
        store.upsertPending(t);
        assertEquals(1, countRows(), "重复 upsert 同一 (hash,target) 应幂等不新增行");
    }

    private void insertTask(String taskId, String hash, String src, String dst,
                            long size, int status, int retry, Timestamp updateTime) throws SQLException {
        String sql = "INSERT INTO replica_sync_task"
                + " (task_id, file_hash, source_node, target_node, status, retry_count, file_size, create_time, update_time)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setString(2, hash);
            ps.setString(3, src);
            ps.setString(4, dst);
            ps.setInt(5, status);
            ps.setInt(6, retry);
            ps.setLong(7, size);
            ps.setTimestamp(8, updateTime);
            ps.setTimestamp(9, updateTime);
            ps.executeUpdate();
        }
    }

    private int countRows() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT COUNT(*) FROM replica_sync_task")) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }
}
