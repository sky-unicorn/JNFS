package org.jnfs.namenode.migration;

import org.jnfs.common.FileTypeDetector;
import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.MigrationContext;
import org.jnfs.common.migration.MigrationStep;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC 模式（mysql / h2）V6 -> V7 迁移步骤。
 * <p>
 * 1. 为 file_metadata 增加 {@code file_type} 列（VARCHAR(32) NULL，文件类型标签：
 *    上传时按扩展名即时写入，后台 Tika 嗅探兜底无扩展名/不可靠扩展名的文件）。
 * <p>
 * 2. 回填存量数据 file_type：逐行按文件名扩展名计算类型并回写（Java 侧计算，
 *    h2 / mysql 零 SQL 方言差异）。
 * <p>
 * 3. file_size 语义迁移：{@code 0 → NULL}。旧版本从未写入 file_size（恒为默认 0），
 *    无法区分"大小未知"与"真实的 0 字节空文件"；本步骤统一把 0 归为 NULL（大小未知），
 *    由后台 FileTypeDetectScheduler 读 DataNode 实际长度回填（空文件最终写回 0 即出队）。
 *    新上传路径（Driver 提交带 fileSize）直接写入真实大小，不再产生 NULL。
 * <p>
 * 幂等性（INV-3）：列级经 {@link JdbcDialect#columnExists} 守卫；行级回填与 0→NULL
 * 仅处理条件匹配的行，重入无副作用。
 * <p>
 * 失败拒绝启动（INV-4）：SQLException 向上抛出，由 MigrationRunner 记录并返回 fail，
 * NameNode {@code System.exit(2)} 拒绝启动。
 * <p>
 * 版本号写入：{@code handlesOwnVersionWrite()} 返回 false，由
 * {@link org.jnfs.common.migration.MigrationRunner#writeJdbcVersion} 单点完成。
 */
public class JdbcV6ToV7 implements MigrationStep {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcV6ToV7.class);

    static final String TABLE = "file_metadata";
    static final String COLUMN = "file_type";

    @Override
    public int fromVersion() {
        return 6;
    }

    @Override
    public int toVersion() {
        return 7;
    }

    @Override
    public boolean supports(StorageMode mode) {
        return mode == StorageMode.MYSQL || mode == StorageMode.H2;
    }

    @Override
    public boolean handlesOwnVersionWrite() {
        return false;
    }

    @Override
    public String migrate(MigrationContext ctx) throws Exception {
        if (ctx.dataSource() == null) {
            return ctx.mode() + " mode requires a DataSource";
        }
        JdbcDialect dialect = JdbcDialect.dialectFor(ctx.mode());

        try (Connection conn = ctx.dataSource().getConnection()) {
            // 1. 加列（幂等）
            if (!dialect.columnExists(conn, TABLE, COLUMN)) {
                conn.createStatement().executeUpdate(
                        "ALTER TABLE `" + TABLE + "` "
                                + "ADD COLUMN `" + COLUMN + "` VARCHAR(32) DEFAULT NULL "
                                + "COMMENT '文件类型标签(扩展名识别/Tika内容嗅探), NULL=未知' "
                                + "AFTER `replication_factor`"
                );
                LOG.info("JdbcV6ToV7: {}.{} 列已添加", TABLE, COLUMN);
            } else {
                LOG.info("JdbcV6ToV7: {}.{} 列已存在，跳过 ALTER", TABLE, COLUMN);
            }

            // 2. 回填存量 file_type（按扩展名，仅 NULL 行，重入无副作用）
            int typed = backfillFileType(conn);
            LOG.info("JdbcV6ToV7: file_type 扩展名回填 {} 行", typed);

            // 3. file_size 语义迁移：0 → NULL（0 不再代表"已知大小"，NULL=未知）
            int nulled = conn.createStatement().executeUpdate(
                    "UPDATE `" + TABLE + "` SET `file_size` = NULL WHERE `file_size` = 0");
            LOG.info("JdbcV6ToV7: file_size 0→NULL 语义迁移 {} 行", nulled);
        }
        return null;
    }

    /**
     * 逐行回填：SELECT storage_id, filename WHERE file_type IS NULL →
     * 按扩展名计算标签 → UPDATE ... WHERE storage_id=? AND file_type IS NULL。
     * 计算在 Java 侧完成（{@link FileTypeDetector}），h2 / mysql 零 SQL 差异。
     */
    private int backfillFileType(Connection conn) throws SQLException {
        List<Object[]> rows = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT storage_id, filename FROM " + TABLE + " WHERE file_type IS NULL");
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String type = FileTypeDetector.fromFilename(rs.getString("filename"));
                if (type != null) {
                    rows.add(new Object[]{type, rs.getString("storage_id")});
                }
            }
        }

        int updated = 0;
        try (PreparedStatement stmt = conn.prepareStatement(
                "UPDATE " + TABLE + " SET file_type = ? WHERE storage_id = ? AND file_type IS NULL")) {
            for (Object[] row : rows) {
                stmt.setString(1, (String) row[0]);
                stmt.setString(2, (String) row[1]);
                updated += stmt.executeUpdate();
            }
        }
        return updated;
    }
}
