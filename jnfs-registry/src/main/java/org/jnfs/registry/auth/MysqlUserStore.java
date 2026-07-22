package org.jnfs.registry.auth;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * MySQL 模式用户存储
 * <p>
 * 使用 HikariCP 连接池操作 dashboard_user 表。
 * 构造时 CREATE TABLE IF NOT EXISTS 幂等建表（不接入 MigrationRunner）。
 * <p>
 * 可与 NameNode 共用同一 MySQL 实例，但建议使用独立数据库（如 jnfs_registry），
 * 避免与元数据表混淆。
 */
public class MysqlUserStore implements UserStore {

    private static final Logger LOG = LoggerFactory.getLogger(MysqlUserStore.class);

    private final HikariDataSource dataSource;

    public MysqlUserStore(String host, int port, String dbName, String user, String password) {
        this.dataSource = createDataSource(host, port, dbName, user, password);
        ensureTableExists();
        LOG.info("MysqlUserStore: 已连接 {}:{}/{}", host, port, dbName);
    }

    @Override
    public String findPasswordHash(String username) {
        String sql = "SELECT password_hash FROM dashboard_user WHERE username = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, username);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("password_hash");
                }
            }
        } catch (SQLException e) {
            LOG.error("MysqlUserStore: 查询用户失败: username={}", username, e);
        }
        return null;
    }

    @Override
    public void saveUser(String username, String bcryptHash) {
        // 先检查是否已存在（幂等）
        if (findPasswordHash(username) != null) {
            LOG.warn("MysqlUserStore: 用户 '{}' 已存在，跳过重复创建", username);
            return;
        }
        String sql = "INSERT INTO dashboard_user (username, password_hash) VALUES (?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, username);
            stmt.setString(2, bcryptHash);
            stmt.executeUpdate();
            LOG.info("MysqlUserStore: 用户 '{}' 已保存", username);
        } catch (SQLException e) {
            LOG.error("MysqlUserStore: 保存用户失败: username={}", username, e);
        }
    }

    @Override
    public boolean updatePassword(String username, String newBcryptHash) {
        String sql = "UPDATE dashboard_user SET password_hash = ? WHERE username = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, newBcryptHash);
            stmt.setString(2, username);
            int rows = stmt.executeUpdate();
            if (rows > 0) {
                LOG.info("MysqlUserStore: 用户 '{}' 密码已更新", username);
                return true;
            } else {
                LOG.warn("MysqlUserStore: 用户 '{}' 不存在，无法修改密码", username);
                return false;
            }
        } catch (SQLException e) {
            LOG.error("MysqlUserStore: 更新密码失败: username={}", username, e);
            return false;
        }
    }

    @Override
    public int userCount() {
        String sql = "SELECT COUNT(*) FROM dashboard_user";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error("MysqlUserStore: 查询用户数失败", e);
        }
        return 0;
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            LOG.info("MysqlUserStore: DataSource 已关闭");
        }
    }

    // ==================== 内部方法 ====================

    private static HikariDataSource createDataSource(String host, int port, String dbName,
                                                      String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + dbName
                + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(3); // Dashboard 用户量极小
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        return new HikariDataSource(config);
    }

    /**
     * 幂等建表：CREATE TABLE IF NOT EXISTS
     */
    private void ensureTableExists() {
        String sql = "CREATE TABLE IF NOT EXISTS `dashboard_user` ("
                + "`username` VARCHAR(64) NOT NULL, "
                + "`password_hash` VARCHAR(72) NOT NULL COMMENT 'BCrypt 哈希', "
                + "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP, "
                + "`update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                + "PRIMARY KEY (`username`)"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Dashboard 登录用户'";
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(sql);
            LOG.info("MysqlUserStore: dashboard_user 表已确保存在");
        } catch (SQLException e) {
            LOG.error("MysqlUserStore: 创建 dashboard_user 表失败", e);
        }
    }
}
