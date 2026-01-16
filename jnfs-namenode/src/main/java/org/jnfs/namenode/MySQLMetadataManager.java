package org.jnfs.namenode;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * MySQL 元数据管理器
 * 使用 JDBC 替换本地文件日志
 */
public class MySQLMetadataManager extends MetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLMetadataManager.class);

    private final HikariDataSource dataSource;

    public MySQLMetadataManager(String host, int port, String dbName, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true");
        config.setUsername(user);
        config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        
        this.dataSource = new HikariDataSource(config);
        
        // 确保表存在 (简单起见，生产环境建议手动创建)
        try (Connection conn = dataSource.getConnection()) {
            // file_metadata
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `file_metadata` (" +
                "`storage_id` CHAR(36) NOT NULL," +
                "`filename` VARCHAR(255) NOT NULL," +
                "`file_hash` CHAR(64) NOT NULL," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`storage_id`)," +
                "KEY `idx_hash` (`file_hash`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            // file_location
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `file_location` (" +
                "`id` BIGINT AUTO_INCREMENT PRIMARY KEY," +
                "`file_hash` CHAR(64) NOT NULL," +
                "`datanode_addr` VARCHAR(100) NOT NULL," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "UNIQUE KEY `uk_hash_node` (`file_hash`, `datanode_addr`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            // file_upload_lock
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS `file_upload_lock` (" +
                "`file_hash` CHAR(64) NOT NULL," +
                "`namenode_id` VARCHAR(64) NOT NULL," +
                "`expire_time` DATETIME NOT NULL," +
                "`create_time` DATETIME DEFAULT CURRENT_TIMESTAMP," +
                "PRIMARY KEY (`file_hash`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
        } catch (SQLException e) {
            LOG.error("创建数据库表失败", e);
        }
    }

    @Override
    public MetadataCacheManager.MetadataEntry queryByHash(String hash) {
        String sql = "SELECT m.filename, m.file_hash, m.storage_id, l.datanode_addr " +
                     "FROM file_metadata m " +
                     "JOIN file_location l ON m.file_hash = l.file_hash " +
                     "WHERE m.file_hash = ? LIMIT 1";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new MetadataCacheManager.MetadataEntry(
                        rs.getString("filename"),
                        rs.getString("file_hash"),
                        rs.getString("datanode_addr"),
                        rs.getString("storage_id")
                    );
                }
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 按Hash查询失败", e);
        }
        return null;
    }

    @Override
    public String queryHashByStorageId(String storageId) {
        String sql = "SELECT file_hash FROM file_metadata WHERE storage_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, storageId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("file_hash");
                }
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 按StorageId查询Hash失败", e);
        }
        return null;
    }

    @Override
    public boolean isFileExist(String hash) {
        String sql = "SELECT 1 FROM file_metadata WHERE file_hash = ? LIMIT 1";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 检查文件存在失败", e);
            return false;
        }
    }

    @Override
    public boolean tryAcquireUploadLock(String hash, String nodeId) {
        String deleteSql = "DELETE FROM file_upload_lock WHERE file_hash = ? AND expire_time < NOW()";
        String insertSql = "INSERT INTO file_upload_lock (file_hash, namenode_id, expire_time) VALUES (?, ?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            // 1. 清理过期锁
            try (PreparedStatement stmt = conn.prepareStatement(deleteSql)) {
                stmt.setString(1, hash);
                stmt.executeUpdate();
            }

            // 2. 尝试获取锁 (30分钟过期)
            try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                stmt.setString(1, hash);
                stmt.setString(2, nodeId);
                stmt.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis() + 30 * 60 * 1000));
                stmt.executeUpdate();
                return true;
            }
        } catch (SQLException e) {
            // Duplicate entry error code for MySQL is 1062
            if (e.getErrorCode() == 1062) { 
                return false;
            }
            LOG.error("[MySQLMetadataManager] 获取锁失败", e);
            return false;
        }
    }

    @Override
    public void releaseUploadLock(String hash) {
        String sql = "DELETE FROM file_upload_lock WHERE file_hash = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, hash);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 释放锁失败", e);
        }
    }

    @Override
    public void recover(Map<String, String> filenameToHash, 
                        Map<String, String> hashToStorage,
                        Map<String, String> hashToId,
                        Set<String> persistedHashes) {
        LOG.info("[MySQLMetadataManager] 正在从数据库恢复元数据...");
        int count = 0;
        
        String sql = "SELECT m.filename, m.file_hash, m.storage_id, l.datanode_addr " +
                     "FROM file_metadata m " +
                     "JOIN file_location l ON m.file_hash = l.file_hash";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
             
            while (rs.next()) {
                String filename = rs.getString("filename");
                String hash = rs.getString("file_hash");
                String storageId = rs.getString("storage_id");
                String address = rs.getString("datanode_addr");
                
                filenameToHash.put(filename, hash);
                // 注意：如果有多个副本，这里只会覆盖为最后一个 (当前架构只支持单副本)
                hashToStorage.put(hash, address);
                hashToId.put(hash, storageId);
                persistedHashes.add(hash);
                
                count++;
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 恢复失败", e);
        }
        
        LOG.info("[MySQLMetadataManager] 恢复完成，共加载 {} 条记录", count);
    }

    @Override
    public void logAddFile(String filename, String hash, String address, String storageId) {
        // 使用事务确保一致性
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 1. 插入 metadata (如果 hash 已存在可能导致重复，这里假设 hash 是文件级唯一，但 filename 不同)
                // 简化起见，我们允许 hash 重复 (不同文件内容相同)，但 storage_id 必须唯一
                String sqlMeta = "INSERT INTO file_metadata (storage_id, filename, file_hash) VALUES (?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sqlMeta)) {
                    stmt.setString(1, storageId);
                    stmt.setString(2, filename);
                    stmt.setString(3, hash);
                    stmt.executeUpdate();
                }
                
                // 2. 插入 location (如果已存在则忽略，用于秒传场景)
                String sqlLoc = "INSERT IGNORE INTO file_location (file_hash, datanode_addr) VALUES (?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sqlLoc)) {
                    stmt.setString(1, hash);
                    stmt.setString(2, address);
                    stmt.executeUpdate();
                }

                // 3. 删除锁 (确保原子性)
                String sqlUnlock = "DELETE FROM file_upload_lock WHERE file_hash = ?";
                try (PreparedStatement stmt = conn.prepareStatement(sqlUnlock)) {
                    stmt.setString(1, hash);
                    stmt.executeUpdate();
                }
                
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            LOG.error("[MySQLMetadataManager] 写入数据库失败", e);
        }
    }
}
