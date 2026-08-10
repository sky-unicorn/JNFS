package org.jnfs.common;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 H2 AUTO_SERVER=TRUE 混合模式协调：
 * Registry 与 NameNode 是两个独立 JVM 进程，单机模式共享同一条 H2 文件库，
 * 两者都经 {@link H2DataSourceFactory} 构建逐字节一致的 URL。本测试用同 JVM
 * 内的两个 DataSource 模拟多进程共享（第一个建立文件锁，第二个经 AUTO_SERVER
 * 以 TCP client 身份接入），验证：两池可同时打开、可各自建表、可交叉读写。
 */
class H2DataSourceFactoryTest {

    @TempDir
    Path tempDir;

    private File dataDir;
    private HikariDataSource dsA;
    private HikariDataSource dsB;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = tempDir.toFile();
        // 模拟 Registry 进程与 NameNode 进程各建一个池，指向同一 H2 文件
        dsA = H2DataSourceFactory.createDataSource(dataDir, 2);
        dsB = H2DataSourceFactory.createDataSource(dataDir, 2);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (dsA != null) {
            dsA.close();
        }
        if (dsB != null) {
            dsB.close();
        }
    }

    @Test
    void twoPoolsShareSameH2FileViaAutoServer() throws Exception {
        // 1. 两池都能打开同一文件（第二个池经 AUTO_SERVER 接入，不应因文件独占锁失败）
        try (Connection connA = dsA.getConnection(); Connection connB = dsB.getConnection()) {
            assertTrue(connA.isValid(2), "连接池 A 应可用");
            assertTrue(connB.isValid(2), "连接池 B 应可用（AUTO_SERVER 混合模式）");
        }

        // 2. 池 A 建表，池 B 可见（同一物理库）
        try (Connection connA = dsA.getConnection(); Statement st = connA.createStatement()) {
            st.executeUpdate(NodeRegistryDdl.createTableDdl());
        }
        try (Connection connA = dsA.getConnection();
             Statement st = connA.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT COUNT(*) FROM information_schema.tables "
                             + "WHERE table_schema = CURRENT_SCHEMA AND table_name = 'node_registry'")) {
            assertTrue(rs.next() && rs.getInt(1) > 0, "node_registry 表应存在");
        }

        // 3. 池 B 写入，池 A 读到（交叉读写，模拟 NameNode/Registry 各自持有连接）
        try (Connection connB = dsB.getConnection();
             PreparedStatement ps = connB.prepareStatement(
                     "INSERT INTO node_registry (node_id, node_type, host, port, free_space, last_heartbeat) "
                             + "VALUES (?, ?, ?, ?, ?, ?)")) {
            ps.setString(1, "dn-1");
            ps.setString(2, "DATANODE");
            ps.setString(3, "192.168.1.10");
            ps.setInt(4, 9000);
            ps.setLong(5, 1024L * 1024 * 1024);
            ps.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));
            ps.executeUpdate();
        }
        try (Connection connA = dsA.getConnection();
             PreparedStatement ps = connA.prepareStatement(
                     "SELECT free_space FROM node_registry WHERE node_id = ?")) {
            ps.setString(1, "dn-1");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "池 B 写入的行应在池 A 读到");
                assertEquals(1024L * 1024 * 1024, rs.getLong(1), "free_space 应一致");
            }
        }
    }

    @Test
    void urlIsStableAndContainsAutoServer() {
        String url = H2DataSourceFactory.buildJdbcUrl(dataDir);
        assertTrue(url.startsWith("jdbc:h2:file:"), "应为 file 数据库 URL: " + url);
        assertTrue(url.contains(";AUTO_SERVER=TRUE"), "必须启用 AUTO_SERVER 混合模式: " + url);
        assertEquals(url, H2DataSourceFactory.buildJdbcUrl(dataDir), "URL 构建必须确定性一致");
    }

    @Test
    void dataSurvivesRestartLikeRegistryNodeList() throws Exception {
        // 模拟 Registry 首次运行：建表 + 写入节点注册
        try (Connection conn = dsA.getConnection(); Statement st = conn.createStatement()) {
            st.executeUpdate(NodeRegistryDdl.createTableDdl());
        }
        upsertNode(dsA, "dn-1", "DATANODE", "192.168.1.10", 9000, 2048L);
        upsertNode(dsA, "nn-1", "NAMENODE", "192.168.1.20", 5368, 0L);

        // 模拟 Registry 重启：关闭全部连接（含 AUTO_SERVER server 进程），再重新打开同一文件
        dsA.close();
        dsB.close();
        HikariDataSource dsRestart = H2DataSourceFactory.createDataSource(dataDir, 2);
        try (Connection conn = dsRestart.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT node_id, node_type, host, port, free_space FROM node_registry ORDER BY node_id")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "重启后 dn-1 应仍在 node_registry");
                assertEquals("dn-1", rs.getString("node_id"));
                assertEquals("DATANODE", rs.getString("node_type"));
                assertEquals(2048L, rs.getLong("free_space"));

                assertTrue(rs.next(), "重启后 nn-1 应仍在 node_registry");
                assertEquals("nn-1", rs.getString("node_id"));
                assertEquals("NAMENODE", rs.getString("node_type"));
                assertEquals(0L, rs.getLong("free_space"));
            }
        } finally {
            dsRestart.close();
        }
    }

    private static void upsertNode(HikariDataSource ds, String nodeId, String nodeType,
                                   String host, int port, long freeSpace) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO node_registry (node_id, node_type, host, port, free_space, last_heartbeat) "
                             + "VALUES (?, ?, ?, ?, ?, ?) "
                             + "ON DUPLICATE KEY UPDATE host = VALUES(host), port = VALUES(port), "
                             + "free_space = VALUES(free_space), last_heartbeat = VALUES(last_heartbeat)")) {
            ps.setString(1, nodeId);
            ps.setString(2, nodeType);
            ps.setString(3, host);
            ps.setInt(4, port);
            ps.setLong(5, freeSpace);
            ps.setTimestamp(6, new java.sql.Timestamp(System.currentTimeMillis()));
            ps.executeUpdate();
        }
    }
}
