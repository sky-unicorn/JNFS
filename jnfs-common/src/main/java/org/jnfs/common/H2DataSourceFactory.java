package org.jnfs.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;

/**
 * H2 嵌入式文件库 DataSource 工厂（单一来源）。
 * <p>
 * Registry 与 NameNode 共用此工厂构建 H2 JDBC URL，保证 URL 参数逐字节一致，
 * 使 {@code AUTO_SERVER=TRUE} 混合模式正常协调（单机打包下 Registry 与 NameNode
 * 是两个独立 JVM 进程，共享同一条 H2 文件库必须开启 AUTO_SERVER，否则第二个进程
 * 打开文件会因独占锁失败）。
 * <p>
 * URL 参数：
 * <ul>
 *   <li>{@code MODE=MariaDB}：兼容 mysql 方言（与原 H2MetadataManager 一致）</li>
 *   <li>{@code DATABASE_TO_LOWER=TRUE} / {@code CASE_INSENSITIVE_IDENTIFIERS=TRUE}：表名/列名大小写归一</li>
 *   <li>{@code AUTO_SERVER=TRUE}：混合模式，允许多进程共享同一文件库（Registry + NameNode）</li>
 * </ul>
 * <b>注意</b>：H2 2.x 不支持 {@code AUTO_SERVER=TRUE && DB_CLOSE_ON_EXIT=FALSE} 组合（抛
 * {@code Feature not supported}）。故此处不再显式设置 DB_CLOSE_ON_EXIT（保持默认 TRUE，
 * 即最后一个连接关闭 / JVM 退出时自动关闭数据库）。单机部署下 NameNode 与 Registry
 * 一起启停，由最后一个退出进程兜底关闭，语义正确。
 * <p>
 * 路径用正斜杠归一化，避免 Windows 反斜杠在 JDBC URL 中被当作转义符。
 */
public final class H2DataSourceFactory {

    private H2DataSourceFactory() {
        // 工具类，禁止实例化
    }

    /**
     * 构建 H2 嵌入式文件库 JDBC URL（AUTO_SERVER 混合模式）。
     *
     * @param dataDir H2 数据目录（含文件库 {@code jnfs.mv.db}）
     * @return JDBC URL
     */
    public static String buildJdbcUrl(File dataDir) {
        String dir = dataDir.getAbsolutePath().replace('\\', '/');
        return "jdbc:h2:file:" + dir + "/jnfs"
                + ";MODE=MariaDB"
                + ";DATABASE_TO_LOWER=TRUE"
                + ";CASE_INSENSITIVE_IDENTIFIERS=TRUE"
                + ";AUTO_SERVER=TRUE";
    }

    /**
     * 创建 H2 嵌入式文件库 HikariDataSource。
     *
     * @param dataDir       H2 数据目录
     * @param maximumPoolSize 连接池大小（由调用方按角色决定：Registry=2, NameNode=2）
     * @return 配置好的 HikariDataSource（调用方持有其生命周期）
     */
    public static HikariDataSource createDataSource(File dataDir, int maximumPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(buildJdbcUrl(dataDir));
        config.setUsername("sa");
        config.setPassword("");
        config.setMaximumPoolSize(maximumPoolSize);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        return new HikariDataSource(config);
    }
}