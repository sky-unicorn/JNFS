package org.jnfs.namenode;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jnfs.common.migration.JdbcDialect;
import org.jnfs.common.migration.StorageMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * H2 嵌入式文件库元数据管理器（file 模式的替代实现）
 * <p>
 * 继承 {@link JdbcMetadataManager}，复用全部 JDBC 业务逻辑。子类只负责：
 * <ul>
 *   <li>{@link #createLocalDataSource(File)}：构建 jdbc:h2:file URL + HikariCP（maximumPoolSize=2）</li>
 *   <li>方言 = {@link JdbcDialect.H2Dialect}</li>
 * </ul>
 * 锚点表 DDL 走父类 {@link JdbcMetadataManager#buildDdl}（与 mysql 同一份 DDL，探针验证零分支兼容）。
 * <p>
 * 单副本语义：H2 作为 file 模式替代，与 file 一致为单副本；冗余组件（ReplicationGroupStore /
 * ReplicaSyncScheduler）在 NameNode 启动时保持 null（由 {@link NameNodeServer} 决定）。
 */
public class H2MetadataManager extends JdbcMetadataManager {

    private static final Logger LOG = LoggerFactory.getLogger(H2MetadataManager.class);

    /**
     * 使用已有 H2 DataSource 构造（迁移流程中先创建 DataSource，再传入）。
     * <p>
     * 与 {@link MySQLMetadataManager} 同构：父类构造时执行锚点表 DDL（兜底，迁移链已建表则 no-op）。
     *
     * @param dataSource H2 嵌入式文件库数据源
     */
    public H2MetadataManager(HikariDataSource dataSource) {
        super(dataSource, JdbcDialect.dialectFor(StorageMode.H2));
        LOG.info("H2MetadataManager 已初始化（嵌入式文件库，单副本语义）");
    }

    /**
     * 构建本地 H2 嵌入式文件库数据源。
     * <p>
     * URL {@code jdbc:h2:file:<dataDir>/jnfs;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;
     * CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_ON_EXIT=FALSE}，
     * HikariCP maximumPoolSize=2（嵌入式单进程，无需高并发池）。
     * <p>
     * 路径用正斜杠归一化，避免 Windows 反斜杠在 JDBC URL 中被当作转义符。
     *
     * @param dataDir 数据目录（由 {@link org.jnfs.common.DataDirResolver#dataDir()} 解析）
     * @return 配置好的 HikariDataSource（调用方持有其生命周期）
     */
    public static HikariDataSource createLocalDataSource(File dataDir) {
        String dir = dataDir.getAbsolutePath().replace('\\', '/');
        String url = "jdbc:h2:file:" + dir + "/jnfs"
                + ";MODE=MariaDB"
                + ";DATABASE_TO_LOWER=TRUE"
                + ";CASE_INSENSITIVE_IDENTIFIERS=TRUE"
                + ";DB_CLOSE_ON_EXIT=FALSE";
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(2);
        LOG.info("H2 嵌入式文件库数据源已创建: {}", url);
        return new HikariDataSource(config);
    }
}
