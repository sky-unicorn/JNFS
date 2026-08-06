package org.jnfs.namenode.storage;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H2 MariaDB 方言兼容性探针：验证 file→H2 改造计划中拟复用的 mysql 方言点是否在 H2 下可用。
 * <p>每项独立测试方法；失败项打印 H2 实际报错。结果作为 {@code JdbcDialect} 设计的事实依据。
 * <p>探针目的不是「全绿」，而是摸清哪些方言点 H2 直接支持、哪些必须 dialect 分支。
 */
class H2DialectProbeTest {

    private static final String URL =
            "jdbc:h2:mem:probe;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_ON_EXIT=FALSE";

    private Connection open() throws SQLException {
        return DriverManager.getConnection(URL, "sa", "");
    }

    @Test
    void a_engineAndCharset() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_a (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
            System.out.println("[a] ENGINE=InnoDB DEFAULT CHARSET=utf8mb4  -> PASS");
        }
    }

    @Test
    void b_backtickIdentifiers() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE `t_b` (`id` INT, `name` VARCHAR(64))");
            System.out.println("[b] 反引号标识符  -> PASS");
        }
    }

    @Test
    void c_types() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_c (id BIGINT AUTO_INCREMENT PRIMARY KEY, flag TINYINT, ts DATETIME)");
            System.out.println("[c] BIGINT AUTO_INCREMENT / TINYINT / DATETIME  -> PASS");
        }
    }

    @Test
    void d_onUpdateCurrentTimestamp() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_d (id INT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
            System.out.println("[d] ON UPDATE CURRENT_TIMESTAMP  -> PASS");
        }
    }

    @Test
    void e_keyIndexSyntax() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_e (id INT, hash VARCHAR(64), KEY idx_hash (hash))");
            System.out.println("[e] 建表内 KEY idx_hash (hash) 索引语法  -> PASS");
        }
    }

    @Test
    void f_insertIgnore() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_f (id INT PRIMARY KEY, v VARCHAR(64))");
            s.execute("INSERT INTO t_f VALUES (1,'a')");
            s.execute("INSERT IGNORE INTO t_f VALUES (1,'b')");
            try (ResultSet rs = s.executeQuery("SELECT v FROM t_f WHERE id=1")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1), "INSERT IGNORE 应忽略重复行，保留原值 'a'");
            }
            System.out.println("[f] INSERT IGNORE (重复主键不报错、不影响原行)  -> PASS");
        }
    }

    @Test
    void g_onDuplicateKeyUpdate() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_g (id INT PRIMARY KEY, v VARCHAR(64))");
            s.execute("INSERT INTO t_g VALUES (1,'a')");
            s.execute("INSERT INTO t_g VALUES (1,'b') ON DUPLICATE KEY UPDATE v = VALUES(v)");
            try (ResultSet rs = s.executeQuery("SELECT v FROM t_g WHERE id=1")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1), "ON DUPLICATE KEY UPDATE 应把 v 更新为 'b'");
            }
            System.out.println("[g] INSERT ... ON DUPLICATE KEY UPDATE v=VALUES(v)  -> PASS");
        }
    }

    @Test
    void h_nullSafeSpaceship() throws SQLException {
        // 探针结论：H2 MariaDB 模式不支持 <=>（mysql 空安全等值），抛语法错误。
        // 回归守护：若未来 H2 支持 <=>，此处会红，提示可简化 JdbcDialect。
        // JdbcDialect.nullSafeEquals 已用双方都支持的 IS NOT DISTINCT FROM 封装。
        try (Connection c = open(); Statement s = c.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeQuery("SELECT NULL <=> NULL"));
            // 反向验证替代写法 IS NOT DISTINCT FROM 可用
            try (ResultSet rs = s.executeQuery("SELECT NULL IS NOT DISTINCT FROM NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1), "NULL IS NOT DISTINCT FROM NULL 应为 true");
            }
            System.out.println("[h] <=> 不支持（如预期）-> 走 IS NOT DISTINCT FROM");
        }
    }

    @Test
    void i_nowMinusInterval() throws SQLException {
        // 探针结论：H2 不支持 "NOW() - INTERVAL n MINUTE" 语法。
        // 回归守护：若未来 H2 支持，此处会红。
        // mysql 专有的 ReplicaSyncTaskStore 用此方言（H2 单副本不构造该组件，故不触发）。
        try (Connection c = open(); Statement s = c.createStatement()) {
            assertThrows(SQLException.class, () -> s.executeQuery("SELECT NOW() - INTERVAL 5 MINUTE"));
            // 反向验证替代写法 DATEADD 可用
            try (ResultSet rs = s.executeQuery("SELECT DATEADD('MINUTE', -5, CURRENT_TIMESTAMP)")) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1), "DATEADD 应返回非空时间戳");
            }
            System.out.println("[i] NOW()-INTERVAL 不支持（如预期）-> 走 DATEADD");
        }
    }

    @Test
    void j_addColumnAfter() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_j (col1 VARCHAR(64))");
            s.execute("ALTER TABLE t_j ADD COLUMN col2 VARCHAR(64) AFTER col1");
            System.out.println("[j] ALTER TABLE ADD COLUMN ... AFTER col1  -> PASS");
        }
    }

    @Test
    void k_duplicateAddIndex() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_k (id INT, x INT)");
            // k1: 首次 ADD INDEX 应成功
            s.execute("ALTER TABLE t_k ADD INDEX idx_x (x)");
            System.out.println("[k1] ALTER TABLE ADD INDEX idx_x  -> PASS");
            // k2: 重复 ADD INDEX 应抛异常，且 SQLState 以 42 开头 + 消息含 "already exists"
            //     （JdbcDialect.H2Dialect.isDuplicateIndexError 的判定依据，回归守护）
            SQLException k2 = assertThrows(SQLException.class,
                    () -> s.execute("ALTER TABLE t_k ADD INDEX idx_x (x)"));
            String k2State = k2.getSQLState();
            assertNotNull(k2State, "重复 ADD INDEX 应返回非空 SQLState");
            assertTrue(k2State.startsWith("42"), "重复 ADD INDEX SQLState 应以 42 开头，实际: " + k2State);
            assertNotNull(k2.getMessage());
            assertTrue(k2.getMessage().contains("already exists"),
                    "重复 ADD INDEX 消息应含 'already exists'，实际: " + k2.getMessage());
            System.out.println("[k2] 重复 ALTER ADD INDEX idx_x -> SQLState=" + k2State + "（如预期）");
            // k3: 重复 CREATE INDEX（非 IF NOT EXISTS）——迁移链用 ALTER ADD INDEX 做重复检测，
            //     CREATE INDEX 行为与 ALTER 不同，这里记录实际行为（幂等或抛错都给出明确断言）
            s.execute("CREATE INDEX idx_y ON t_k(id)");   // 首次创建成功
            try {
                s.execute("CREATE INDEX idx_y ON t_k(id)"); // 重复创建
                System.out.println("[k3] 重复 CREATE INDEX idx_y -> 不抛异常（H2 MariaDB 静默幂等）");
            } catch (SQLException e) {
                assertTrue(e.getSQLState() != null && e.getSQLState().startsWith("42"),
                        "重复 CREATE INDEX 若抛异常，SQLState 应以 42 开头，实际: "
                                + (e.getSQLState() == null ? "null" : e.getSQLState()));
                System.out.println("[k3] 重复 CREATE INDEX idx_y -> SQLState=" + e.getSQLState() + "（抛异常）");
            }
            // k4: CREATE INDEX IF NOT EXISTS 重复不抛异常
            s.execute("CREATE INDEX IF NOT EXISTS idx_z ON t_k(id)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_z ON t_k(id)");
            System.out.println("[k4] CREATE INDEX IF NOT EXISTS 重复  -> PASS");
        }
    }

    @Test
    void l_createIndexIfNotExists() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_l (id INT, y INT)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_y ON t_l(y)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_y ON t_l(y)");
            System.out.println("[l] CREATE INDEX IF NOT EXISTS (重复不报错)  -> PASS");
        }
    }

    @Test
    void m_informationSchemaDatabase() throws SQLException {
        // 探针结论：H2 的 DATABASE() 返回空串，用 information_schema + DATABASE() 查表存在会得到 0。
        // 回归守护：若未来 H2 修复 DATABASE()，此处断言会红，提示可简化 JdbcDialect.tableExists。
        // JdbcDialect.tableExists 已用 CURRENT_SCHEMA 封装。
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_m (id INT)");
            // 错误写法：DATABASE() 返回空 -> 查不到（断言此行为）
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 't_m'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "H2 的 DATABASE() 返回空，此写法查不到表（如预期）");
            }
            // 正确写法：CURRENT_SCHEMA
            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA AND table_name = 't_m'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "CURRENT_SCHEMA 能查到 t_m");
            }
            System.out.println("[m] DATABASE() 返回空（如预期）-> 走 CURRENT_SCHEMA");
        }
    }

    @Test
    void n_uniqueConstraintSqlState() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_n (id INT PRIMARY KEY)");
            s.execute("INSERT INTO t_n VALUES (1)");
            SQLException caught = assertThrows(SQLException.class, () -> {
                try (Statement s2 = c.createStatement()) {
                    s2.execute("INSERT INTO t_n VALUES (1)");
                }
            });
            System.out.println("[n] 唯一约束冲突 SQLState=" + caught.getSQLState()
                    + " code=" + caught.getErrorCode() + "（期望 SQLState=23505）");
            assertEquals("23505", caught.getSQLState(), "唯一约束冲突 SQLState 应为 23505");
        }
    }

    @Test
    void o_isNotDistinctFrom() throws SQLException {
        try (Connection c = open(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t_o (a VARCHAR(64))");
            s.execute("INSERT INTO t_o VALUES (NULL),(NULL),('x')");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM t_o WHERE a IS NOT DISTINCT FROM NULL")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "IS NOT DISTINCT FROM NULL 应匹配 2 个 NULL 行");
            }
            System.out.println("[o] IS NOT DISTINCT FROM 空安全查重  -> PASS");
        }
    }
}
