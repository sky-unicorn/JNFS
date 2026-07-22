package org.jnfs.namenode.migration;

import org.jnfs.common.NodeAddressResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * file 模式 → mysql 模式 跨模式数据导入器
 * <p>
 * 触发场景：部署从单机 file 模式切换到分布式 mysql 模式时，NameNode 启动检测到
 * {@code namenode_meta.log} 存在且尚未完成导入，则把 file 历史元数据自动导入 mysql。
 * <p>
 * 数据映射（file 日志行 {@code ADD|filename|hash|address|storageId}）：
 * <ul>
 *   <li>{@code file_metadata}：以 {@code storage_id} 为主键 INSERT IGNORE 去重（INV-1 storage_id 不变、INV-2 唯一）</li>
 *   <li>{@code file_location}：按 {@code address} 是否为 host:port 决定写入
 *       {@code datanode_addr}(host:port) 或 {@code datanode_id}(node_id)。
 *       <b>不</b>在冷导入时反查补全 node_id，留给运行时在线补全（见设计文档 §4.9 / §4.10）</li>
 * </ul>
 * <p>
 * 幂等可重入（INV-3）：
 * <ul>
 *   <li>每条记录 per-row 幂等（file_metadata INSERT IGNORE + file_location NULL 安全等值守卫）</li>
 *   <li>全部完成后写完成标记 {@code file_to_mysql_imported}（原子写）；二次启动命中标记即跳过</li>
 *   <li>中途崩溃重启：标记不存在 → 重跑，per-row 幂等跳过已导入行，补齐剩余后写标记</li>
 * </ul>
 * <p>
 * 失败处理（INV-4）：导入抛异常由调用方 {@code NameNodeServer} 捕获并 {@code System.exit(2)} 拒绝启动。
 */
public final class FileToMysqlImporter {

    private static final Logger LOG = LoggerFactory.getLogger(FileToMysqlImporter.class);

    private static final String METADATA_LOG_FILE = "namenode_meta.log";
    private static final String IMPORT_MARKER = "file_to_mysql_imported";
    private static final String IMPORT_MARKER_TMP = "file_to_mysql_imported.tmp";

    private FileToMysqlImporter() {
    }

    /**
     * 若存在 file 模式历史数据且尚未导入，则执行 file → mysql 自动导入。
     * <p>
     * 调用前须保证 mysql 表已建好（由 {@code MySQLMetadataManager} 构造时 CREATE TABLE IF NOT EXISTS 确保），
     * 且 file 日志已规整为 V1 格式（由 {@code MigrationRunner.run(FILE, dataDir, null)} 确保，稳定 storageId）。
     *
     * @param dataDir 数据目录（APP_HOME，namenode_meta.log 所在目录）
     * @param ds      mysql 数据源
     */
    public static void importIfApplicable(File dataDir, DataSource ds) throws Exception {
        File logFile = new File(dataDir, METADATA_LOG_FILE);
        if (!logFile.exists()) {
            return; // 纯 mysql 部署，无 file 历史
        }

        File marker = new File(dataDir, IMPORT_MARKER);
        if (marker.exists()) {
            LOG.info("[FileToMysqlImporter] 已存在导入完成标记 {}，跳过 file→mysql 自动导入", IMPORT_MARKER);
            return;
        }

        LOG.warn("[FileToMysqlImporter] 检测到 file 模式历史数据 ({})，启动 file→mysql 自动导入...", METADATA_LOG_FILE);

        int metadataCount = 0;
        int locationCount = 0;
        int skippedLines = 0;

        // 单连接 + 每条记录一事务：既避免反复获取连接，又保证 per-row 原子与可重入
        try (BufferedReader reader = new BufferedReader(
                     new InputStreamReader(new FileInputStream(logFile), StandardCharsets.UTF_8));
             Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\\|", -1);
                if (parts.length != 5 || !"ADD".equals(parts[0])) {
                    // 非法行：尽力转换，记录后跳过（不应中断整批导入）
                    LOG.warn("[FileToMysqlImporter] 跳过无法识别的行: {}", line);
                    skippedLines++;
                    continue;
                }
                String filename = parts[1];
                String hash = parts[2];
                String address = parts[3];
                String storageId = parts[4];

                try {
                    metadataCount += insertMetadataIgnore(conn, storageId, filename, hash);
                    locationCount += insertLocationIfAbsent(conn, hash, address);
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    throw new SQLException("file→mysql 导入失败，行内容: " + line + "，原因: " + e.getMessage(), e);
                }
            }
        }

        writeMarkerAtomic(marker, metadataCount, locationCount);
        LOG.warn("[FileToMysqlImporter] file→mysql 自动导入完成: file_metadata 新增 {} 条, file_location 新增 {} 条, 跳过非法行 {} 条",
                metadataCount, locationCount, skippedLines);
    }

    /**
     * file_metadata 以 storage_id 为主键插入，INSERT IGNORE 天然去重。
     *
     * @return 实际新增行数（0 表示已存在被跳过）
     */
    private static int insertMetadataIgnore(Connection conn, String storageId, String filename, String hash)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT IGNORE INTO file_metadata (storage_id, filename, file_hash) VALUES (?, ?, ?)")) {
            stmt.setString(1, storageId);
            stmt.setString(2, filename);
            stmt.setString(3, hash);
            return stmt.executeUpdate();
        }
    }

    /**
     * file_location 幂等插入：先 NULL 安全等值查重，不存在才插入。
     * <p>
     * address 为 host:port → 写 datanode_addr、datanode_id 置 NULL（留运行时在线补全）；
     * address 为 node_id → 写 datanode_id、datanode_addr 置 NULL。
     */
    private static int insertLocationIfAbsent(Connection conn, String hash, String address) throws SQLException {
        boolean isHostPort = NodeAddressResolver.isHostPort(address);
        String datanodeId = isHostPort ? null : address;
        String datanodeAddr = isHostPort ? address : null;

        if (locationExists(conn, hash, datanodeId, datanodeAddr)) {
            return 0;
        }
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO file_location (file_hash, datanode_id, datanode_addr) VALUES (?, ?, ?)")) {
            stmt.setString(1, hash);
            setNullableString(stmt, 2, datanodeId);
            setNullableString(stmt, 3, datanodeAddr);
            return stmt.executeUpdate();
        }
    }

    /**
     * NULL 安全等值查重。{@code <=>} 在 MySQL 中对 NULL 也判定相等，
     * 解决 UNIQUE(file_hash, datanode_id) 在 datanode_id 为 NULL 时无法去重的问题。
     */
    private static boolean locationExists(Connection conn, String hash, String datanodeId, String datanodeAddr)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT 1 FROM file_location WHERE file_hash = ? AND datanode_id <=> ? AND datanode_addr <=> ? LIMIT 1")) {
            stmt.setString(1, hash);
            setNullableString(stmt, 2, datanodeId);
            setNullableString(stmt, 3, datanodeAddr);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static void setNullableString(PreparedStatement stmt, int index, String value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.VARCHAR);
        } else {
            stmt.setString(index, value);
        }
    }

    /**
     * 原子写完成标记（tmp + fsync + rename），与 {@code MigrationRunner.writeFileVersionAtomic} 同款模式。
     * 标记文件记录导入计数，供运维审计。
     */
    private static void writeMarkerAtomic(File marker, int metadataCount, int locationCount) throws IOException {
        File tmp = new File(marker.getParentFile(), IMPORT_MARKER_TMP);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tmp), StandardCharsets.UTF_8))) {
            writer.write("done");
            writer.newLine();
            writer.write("file_metadata=" + metadataCount);
            writer.newLine();
            writer.write("file_location=" + locationCount);
            writer.newLine();
            writer.flush();
        }
        try (FileOutputStream fos = new FileOutputStream(tmp, true)) {
            fos.getFD().sync();
        }
        if (!tmp.renameTo(marker)) {
            Files.move(tmp.toPath(), marker.toPath(),
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }
    }
}
