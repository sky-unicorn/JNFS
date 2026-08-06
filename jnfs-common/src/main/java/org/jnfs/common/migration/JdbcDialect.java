package org.jnfs.common.migration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC 方言抽象——收口 MySQL 与 H2 的 SQL 差异
 * <p>
 * 迁移框架与 MetadataManager 均通过此接口访问数据库，避免业务代码中出现
 * if(mysql)/if(h2) 的散落分支。
 * <p>
 * 使用方式：
 * <pre>
 * JdbcDialect dialect = JdbcDialect.dialectFor(storageMode);
 * if (dialect.isDuplicateKeyError(e)) { ... }
 * </pre>
 */
public interface JdbcDialect {

    // ==================== 工厂 ====================

    /**
     * 根据 StorageMode 返回对应方言实例
     *
     * @param mode 存储模式（MYSQL / H2；FILE 不走 JDBC，传入会抛异常）
     * @return 方言实例
     */
    static JdbcDialect dialectFor(StorageMode mode) {
        switch (mode) {
            case MYSQL:
                return MysqlDialect.INSTANCE;
            case H2:
                return H2Dialect.INSTANCE;
            default:
                throw new IllegalArgumentException("No JdbcDialect for mode: " + mode);
        }
    }

    // ==================== 错误判定 ====================

    /**
     * 判断是否为唯一约束冲突（重复键）错误
     * <ul>
     *   <li>MySQL：errorCode == 1062</li>
     *   <li>H2：SQLState == "23505"</li>
     * </ul>
     */
    boolean isDuplicateKeyError(SQLException e);

    /**
     * 判断是否为重复索引错误（CREATE INDEX 时索引名已存在）
     * <ul>
     *   <li>MySQL：errorCode == 1061</li>
     *   <li>H2：SQLState 以 "42" 开头且消息含 "already exists"（H2 对重复索引抛
     *           SQLState 42121 或类似 42xxx，具体值因版本略有差异，故用前缀+消息双重判定）</li>
     * </ul>
     */
    boolean isDuplicateIndexError(SQLException e);

    // ==================== 元数据查询 ====================

    /**
     * 判断表是否存在
     * <ul>
     *   <li>MySQL：{@code information_schema.tables WHERE table_schema=DATABASE()}</li>
     *   <li>H2：{@code information_schema.tables WHERE table_schema=CURRENT_SCHEMA}
     *       （H2 的 DATABASE() 返回空串，必须用 CURRENT_SCHEMA）</li>
     * </ul>
     */
    boolean tableExists(Connection conn, String table) throws SQLException;

    /**
     * 判断列是否存在
     * <ul>
     *   <li>MySQL：{@code information_schema.columns WHERE table_schema=DATABASE()}</li>
     *   <li>H2：{@code information_schema.columns WHERE table_schema=CURRENT_SCHEMA}</li>
     * </ul>
     */
    boolean columnExists(Connection conn, String table, String column) throws SQLException;

    // ==================== SQL 片段 ====================

    /**
     * 返回 NULL 安全等值比较的 SQL 片段
     * <p>
     * MySQL 8.0.33+ 与 H2 均支持 {@code IS NOT DISTINCT FROM}，故统一返回
     * {@code <column> IS NOT DISTINCT FROM ?}，零分支。
     * <p>
     * 注意：MySQL 旧版（&lt; 8.0.33）不支持此语法，若需兼容旧版 MySQL，
     * 需改回 {@code <=>} 并在 MysqlDialect / H2Dialect 中分别实现。
     *
     * @param column 列名（不含反引号，由调用方按需包裹）
     * @return SQL 片段，如 {@code my_col IS NOT DISTINCT FROM ?}
     */
    default String nullSafeEquals(String column) {
        // MySQL 8.0.33+ 与 H2 均支持 IS NOT DISTINCT FROM，统一零分支
        return column + " IS NOT DISTINCT FROM ?";
    }

    /**
     * 返回当前时间戳的 SQL 函数
     * <ul>
     *   <li>MySQL：{@code NOW()}</li>
     *   <li>H2：{@code CURRENT_TIMESTAMP}</li>
     * </ul>
     */
    String nowLiteral();

    // ==================== MySQL 方言 ====================

    /**
     * MySQL 方言实现
     */
    final class MysqlDialect implements JdbcDialect {
        static final MysqlDialect INSTANCE = new MysqlDialect();

        private MysqlDialect() {
        }

        @Override
        public boolean isDuplicateKeyError(SQLException e) {
            return e.getErrorCode() == 1062;
        }

        @Override
        public boolean isDuplicateIndexError(SQLException e) {
            return e.getErrorCode() == 1061;
        }

        @Override
        public boolean tableExists(Connection conn, String table) throws SQLException {
            String sql = "SELECT COUNT(*) FROM information_schema.tables "
                    + "WHERE table_schema = DATABASE() AND table_name = ?";
            return existsQuery(conn, sql, table);
        }

        @Override
        public boolean columnExists(Connection conn, String table, String column) throws SQLException {
            String sql = "SELECT COUNT(*) FROM information_schema.columns "
                    + "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?";
            return existsQuery2(conn, sql, table, column);
        }

        @Override
        public String nowLiteral() {
            return "NOW()";
        }
    }

    // ==================== H2 方言 ====================

    /**
     * H2 方言实现（MariaDB 兼容模式）
     */
    final class H2Dialect implements JdbcDialect {
        static final H2Dialect INSTANCE = new H2Dialect();

        private H2Dialect() {
        }

        @Override
        public boolean isDuplicateKeyError(SQLException e) {
            return "23505".equals(e.getSQLState());
        }

        @Override
        public boolean isDuplicateIndexError(SQLException e) {
            // H2 对重复 ADD INDEX 抛 SQLState 42121 或类似 42xxx，
            // 具体值因版本略有差异，故用前缀 "42" + 消息含 "already exists" 双重判定
            String state = e.getSQLState();
            String msg = e.getMessage();
            return state != null && state.startsWith("42")
                    && msg != null && msg.contains("already exists");
        }

        @Override
        public boolean tableExists(Connection conn, String table) throws SQLException {
            // H2 的 DATABASE() 返回空串，必须用 CURRENT_SCHEMA
            String sql = "SELECT COUNT(*) FROM information_schema.tables "
                    + "WHERE table_schema = CURRENT_SCHEMA AND table_name = ?";
            return existsQuery(conn, sql, table);
        }

        @Override
        public boolean columnExists(Connection conn, String table, String column) throws SQLException {
            String sql = "SELECT COUNT(*) FROM information_schema.columns "
                    + "WHERE table_schema = CURRENT_SCHEMA AND table_name = ? AND column_name = ?";
            return existsQuery2(conn, sql, table, column);
        }

        @Override
        public String nowLiteral() {
            return "CURRENT_TIMESTAMP";
        }
    }

    // ==================== 内部工具 ====================

    /**
     * 单参数 existence 查询
     */
    private static boolean existsQuery(Connection conn, String sql, String param) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, param);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    /**
     * 双参数 existence 查询
     */
    private static boolean existsQuery2(Connection conn, String sql, String param1, String param2)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, param1);
            stmt.setString(2, param2);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
}
