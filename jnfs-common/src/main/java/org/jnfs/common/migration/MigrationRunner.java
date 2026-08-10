package org.jnfs.common.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 迁移执行器
 * <p>
 * 负责：版本检测、步骤加载排序、链式执行、原子性保证。
 * <p>
 * 关键原则：
 * - 原子性：每步迁移与版本号写入一起成功或一起失败
 *   - file 模式：先写 meta_version.tmp 再 renameTo 覆盖
 *   - mysql 模式：迁移 DML 与 UPDATE schema_version 在同一事务内
 * - 幂等性：迁移步骤可重入，中途崩溃重启后能安全再次执行
 * - 顺序性：严格按 fromVersion 升序执行，不允许跳版本
 */
public final class MigrationRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationRunner.class);

    /** 当前目标版本（代码所期望的最新 schema 版本） */
    public static final int CURRENT_VERSION = 6;

    /** file 模式版本文件名 */
    public static final String META_VERSION_FILE = "meta_version";
    /** file 模式临时版本文件名 */
    public static final String META_VERSION_TMP = "meta_version.tmp";
    /** file 模式元数据日志文件名 */
    public static final String METADATA_LOG_FILE = "namenode_meta.log";

    /** JDBC 模式（mysql / h2）版本表名 */
    public static final String SCHEMA_VERSION_TABLE = "schema_version";
    /** JDBC 模式锚点业务表（所有版本都存在的表） */
    public static final String ANCHOR_TABLE = "file_metadata";

    /**
     * H2 嵌入式文件库的数据库文件名（仅命名常量）。
     * <p>实际路径解析由 NameNode 侧用 {@link org.jnfs.common.DataDirResolver}
     * 完成（{@code jdbc:h2:file:<dataDir>/jnfs}），本类不强依赖此常量。
     */
    public static final String H2_DB_FILE = "jnfs.mv.db";

    /** 迁移步骤配置文件目录 */
    private static final String MIGRATIONS_DIR = "META-INF/migrations/";

    private MigrationRunner() {
    }

    /**
     * 执行迁移
     *
     * @param mode       存储模式
     * @param dataDir    file 模式下的数据目录（mysql / h2 模式可为 null）
     * @param dataSource mysql / h2 模式下的数据源（file 模式可为 null；H2 嵌入式库
     *                   复用同一 dataSource 槽位，与 mysql 同构）
     * @return 迁移结果
     */
    public static MigrationResult run(StorageMode mode, File dataDir, DataSource dataSource) {
        MigrationContext ctx = new MigrationContext(mode, dataDir, dataSource);

        try {
            int currentVersion = detectVersion(ctx);
            LOG.info("检测到当前 schema 版本: {}", currentVersion);

            if (currentVersion == CURRENT_VERSION) {
                LOG.info("已是最新版本，无需迁移");
                return MigrationResult.ok("Already at latest version " + CURRENT_VERSION);
            }

            // 全新部署：currentVersion > CURRENT_VERSION 不应出现，
            // 但 CURRENT_VERSION 由 detectVersion 返回表示全新部署
            if (currentVersion > CURRENT_VERSION) {
                // 这种情况理论上不应出现，但做防御性处理
                LOG.warn("当前版本 {} 高于代码版本 {}，可能降级部署，跳过迁移", currentVersion, CURRENT_VERSION);
                return MigrationResult.ok("Current version is newer than code version, skipping migration");
            }

            // 加载并排序迁移步骤
            List<MigrationStep> steps = loadSteps();
            steps = steps.stream()
                    .filter(s -> s.supports(mode))
                    .sorted(Comparator.comparingInt(MigrationStep::fromVersion))
                    .collect(Collectors.toList());

            LOG.info("加载到 {} 个适用于 {} 模式的迁移步骤", steps.size(), mode);

            // 链式执行
            int version = currentVersion;
            for (MigrationStep step : steps) {
                if (step.fromVersion() < version) {
                    // 已执行过的步骤，跳过
                    continue;
                }
                if (step.fromVersion() > version) {
                    // 版本跳跃，不允许
                    return MigrationResult.fail("Cannot migrate from version " + version
                            + " to " + step.toVersion() + ": step expects fromVersion=" + step.fromVersion()
                            + ". Linear migration is required.");
                }
                if (step.fromVersion() == version) {
                    LOG.info("执行迁移步骤: {} → {} ({})", step.fromVersion(), step.toVersion(),
                            step.getClass().getSimpleName());
                    String error = step.migrate(ctx);
                    if (error != null) {
                        LOG.error("迁移步骤 {} → {} 失败: {}", step.fromVersion(), step.toVersion(), error);
                        return MigrationResult.fail("Migration step " + step.fromVersion() + "→"
                                + step.toVersion() + " failed: " + error);
                    }
                    // 写入新版本号（原子性保证）
                    // 步骤声明了 handlesOwnVersionWrite() → 已在自己事务内写入版本号 (MySQL 原子性,§4.6)
                    // 否则 → Runner 负责写入 (file 模式：原子 rename 覆盖 meta_version)
                    version = step.toVersion();
                    if (!step.handlesOwnVersionWrite()) {
                        writeVersion(ctx, version);
                        LOG.info("版本号已更新为: {}", version);
                    } else {
                        LOG.info("步骤 {} 已自行写入版本号 {}", step.getClass().getSimpleName(), version);
                    }
                }
            }

            if (version < CURRENT_VERSION) {
                LOG.warn("迁移完成后版本为 {}，但当前代码版本为 {}，可能缺少迁移步骤", version, CURRENT_VERSION);
            }

            return MigrationResult.ok("Migrated from version " + currentVersion + " to " + version);

        } catch (Exception e) {
            LOG.error("迁移过程发生异常", e);
            return MigrationResult.fail("Migration failed with exception: " + e.getMessage());
        }
    }

    // ==================== 版本检测 ====================

    /**
     * 三态判定版本号
     * <p>
     * - 已纳入管理：schema_version 存在 → 读取版本号
     * - 老数据：schema_version 不存在 + 业务表/日志存在 → 版本 0
     * - 全新部署：schema_version 不存在 + 业务表/日志也不存在 → CURRENT_VERSION（mysql）/ 0（h2）
     */
    static int detectVersion(MigrationContext ctx) throws Exception {
        switch (ctx.mode()) {
            case MYSQL:
                return detectJdbcVersion(ctx, JdbcDialect.MysqlDialect.INSTANCE, true);
            case H2:
                return detectH2Version(ctx);
            default:
                return detectFileVersion(ctx);
        }
    }

    /**
     * JDBC 模式（mysql / h2）通用版本检测
     *
     * @param ctx               迁移上下文
     * @param dialect           方言
     * @param freshDeployHint   全新部署时是否直接写 CURRENT_VERSION（mysql 保留旧行为捷径；
     *                          h2 必须为 false：首启必走全链建表）
     */
    private static int detectJdbcVersion(MigrationContext ctx, JdbcDialect dialect,
                                         boolean freshDeployHint) throws SQLException {
        DataSource ds = ctx.dataSource();
        if (ds == null) {
            throw new IllegalStateException(ctx.mode() + " mode requires a DataSource");
        }

        try (Connection conn = ds.getConnection()) {
            if (dialect.tableExists(conn, SCHEMA_VERSION_TABLE)) {
                return readJdbcVersion(conn);
            }

            // schema_version 不存在，区分老数据 vs 全新部署
            if (dialect.tableExists(conn, ANCHOR_TABLE)) {
                LOG.info("schema_version 不存在但 {} 存在，判定为老数据（版本 0）", ANCHOR_TABLE);
                return 0;
            }

            LOG.info("schema_version 和 {} 均不存在，判定为全新部署", ANCHOR_TABLE);
            if (freshDeployHint) {
                // mysql：全新部署建表 + 写入当前版本号（旧行为捷径）
                initializeJdbcSchema(conn);
                return CURRENT_VERSION;
            }
            // h2：不做捷径，返回 0，让迁移链全量建表
            LOG.info("H2 全新部署不写版本号捷径，返回版本 0，由迁移链全量建表");
            return 0;
        }
    }

    /**
     * H2 嵌入式文件库版本检测
     * <p>
     * 与 mysql 不同：H2 全新部署不做"直接写 CURRENT_VERSION"捷径，确保首启必走全链建表。
     * schema_version 表不存在一律返回 0。
     */
    private static int detectH2Version(MigrationContext ctx) throws SQLException {
        return detectJdbcVersion(ctx, JdbcDialect.H2Dialect.INSTANCE, false);
    }

    private static int detectFileVersion(MigrationContext ctx) throws IOException {
        File dataDir = ctx.dataDir();
        if (dataDir == null) {
            throw new IllegalStateException("File mode requires a dataDir");
        }

        File versionFile = new File(dataDir, META_VERSION_FILE);
        if (versionFile.exists()) {
            return readFileVersion(versionFile);
        }

        // meta_version 不存在，区分老数据 vs 全新部署
        File logFile = new File(dataDir, METADATA_LOG_FILE);
        if (logFile.exists()) {
            LOG.info("meta_version 不存在但 {} 存在，判定为老数据（版本 0）", METADATA_LOG_FILE);
            return 0;
        }

        LOG.info("meta_version 和 {} 均不存在，判定为全新部署", METADATA_LOG_FILE);
        // 全新部署：写入当前版本号
        writeFileVersionAtomic(dataDir, CURRENT_VERSION);
        return CURRENT_VERSION;
    }

    // ==================== JDBC 通用版本操作 ====================

    /**
     * 旧的 tableExists——已被 {@link JdbcDialect#tableExists} 替代，
     * 保留以保持与外部调用者的兼容（如后续删掉可标记 deprecated）
     */
    @Deprecated
    static boolean tableExists(Connection conn, String tableName) throws SQLException {
        return JdbcDialect.MysqlDialect.INSTANCE.tableExists(conn, tableName);
    }

    private static int readJdbcVersion(Connection conn) throws SQLException {
        String sql = "SELECT version FROM " + SCHEMA_VERSION_TABLE + " ORDER BY version DESC LIMIT 1";
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt("version");
            }
        }
        // schema_version 表存在但为空，视为版本 0
        return 0;
    }

    /**
     * 全新部署时初始化 JDBC schema（mysql 专用捷径）
     * <p>
     * 创建 schema_version 表并写入当前版本号。
     * 注意：H2 不走此捷径（detectH2Version freshDeployHint=false），
     * 本方法仅 mysql 全新部署时调用。
     */
    private static void initializeJdbcSchema(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS `" + SCHEMA_VERSION_TABLE + "` ("
                        + "`version` INT NOT NULL COMMENT '当前 schema 版本', "
                        + "`upgraded_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                        + "PRIMARY KEY (`version`)"
                        + ") ENGINE=InnoDB CHARACTER SET=utf8mb4 COMMENT='schema 版本记录'"
        );
        conn.createStatement().executeUpdate(
                "INSERT INTO " + SCHEMA_VERSION_TABLE + " (version) VALUES (" + CURRENT_VERSION + ")"
        );
        LOG.info("全新部署：已创建 schema_version 表并写入版本号 {}", CURRENT_VERSION);
    }

    // ==================== 旧 mysql 方法（保留兼容，内部委托到通用方法）====================

    /**
     * @deprecated 使用 {@link #readJdbcVersion}，保留供外部兼容调用
     */
    @Deprecated
    static int readMysqlVersion(Connection conn) throws SQLException {
        return readJdbcVersion(conn);
    }

    /**
     * @deprecated 使用 {@link #initializeJdbcSchema}，保留供外部兼容调用
     */
    @Deprecated
    static void initializeMysqlSchema(Connection conn) throws SQLException {
        initializeJdbcSchema(conn);
    }

    // ==================== File 版本操作 ====================

    private static int readFileVersion(File versionFile) throws IOException {
        String content = new String(Files.readAllBytes(versionFile.toPath()), StandardCharsets.UTF_8).trim();
        if (content.isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(content);
        } catch (NumberFormatException e) {
            throw new IOException("Invalid version file content: " + content);
        }
    }

    /**
     * 原子写入版本号（file 模式）
     * 先写临时文件，再 renameTo 覆盖
     */
    static void writeFileVersionAtomic(File dataDir, int version) throws IOException {
        File tmpFile = new File(dataDir, META_VERSION_TMP);
        File versionFile = new File(dataDir, META_VERSION_FILE);

        // 写入临时文件
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8))) {
            writer.write(String.valueOf(version));
            writer.newLine();
            writer.flush();
        }

        // fsync 确保数据落盘
        try (FileOutputStream fos = new FileOutputStream(tmpFile, true)) {
            fos.getFD().sync();
        }

        // 原子替换
        if (!tmpFile.renameTo(versionFile)) {
            // renameTo 可能因跨文件系统失败，使用 Files.move 作为后备
            Files.move(tmpFile.toPath(), versionFile.toPath(), StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        }

        LOG.debug("版本号已原子写入: {} → {}", versionFile, version);
    }

    // ==================== 版本号写入（迁移步骤成功后调用） ====================

    private static void writeVersion(MigrationContext ctx, int version) throws Exception {
        StorageMode mode = ctx.mode();
        if (mode == StorageMode.MYSQL || mode == StorageMode.H2) {
            writeJdbcVersion(ctx, version);
        } else {
            writeFileVersionAtomic(ctx.dataDir(), version);
        }
    }

    /**
     * JDBC 模式（mysql / h2）写入版本号
     * <p>
     * 注意：此方法在迁移步骤的事务外调用，用于非事务性迁移步骤的版本号更新。
     * 如果迁移步骤本身在事务内更新了 schema_version，则此方法会执行 UPDATE（幂等）。
     * <p>
     * 表是否存在的检测走 {@link JdbcDialect#tableExists}，兼容 mysql 与 h2。
     */
    private static void writeJdbcVersion(MigrationContext ctx, int version) throws SQLException {
        DataSource ds = ctx.dataSource();
        JdbcDialect dialect = JdbcDialect.dialectFor(ctx.mode());
        try (Connection conn = ds.getConnection()) {
            // 确保 schema_version 表存在
            if (!dialect.tableExists(conn, SCHEMA_VERSION_TABLE)) {
                conn.createStatement().executeUpdate(
                        "CREATE TABLE IF NOT EXISTS `" + SCHEMA_VERSION_TABLE + "` ("
                                + "`version` INT NOT NULL COMMENT '当前 schema 版本', "
                                + "`upgraded_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                                + "PRIMARY KEY (`version`)"
                                + ") ENGINE=InnoDB CHARACTER SET=utf8mb4 COMMENT='schema 版本记录'"
                );
            }

            // 尝试 UPDATE，如果没有行被更新则 INSERT
            String updateSql = "UPDATE " + SCHEMA_VERSION_TABLE + " SET version = ?, upgraded_at = CURRENT_TIMESTAMP";
            try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
                stmt.setInt(1, version);
                int rows = stmt.executeUpdate();
                if (rows == 0) {
                    String insertSql = "INSERT INTO " + SCHEMA_VERSION_TABLE + " (version) VALUES (?)";
                    try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                        insertStmt.setInt(1, version);
                        insertStmt.executeUpdate();
                    }
                }
            }
        }
    }

    /**
     * @deprecated 使用 {@link #writeJdbcVersion}，保留供外部兼容调用
     */
    @Deprecated
    static void writeMysqlVersion(MigrationContext ctx, int version) throws SQLException {
        writeJdbcVersion(ctx, version);
    }

    // ==================== 步骤加载 ====================

    /**
     * 从 META-INF/migrations/ 目录加载迁移步骤
     * <p>
     * 每个 .properties 文件包含 class=全限定类名
     */
    static List<MigrationStep> loadSteps() {
        List<MigrationStep> steps = new ArrayList<>();

        try {
            Enumeration<URL> resources = MigrationRunner.class.getClassLoader()
                    .getResources(MIGRATIONS_DIR);

            while (resources.hasMoreElements()) {
                URL dirUrl = resources.nextElement();
                loadStepsFromDir(dirUrl, steps);
            }
        } catch (IOException e) {
            LOG.warn("扫描迁移步骤配置目录失败", e);
        }

        return steps;
    }

    private static void loadStepsFromDir(URL dirUrl, List<MigrationStep> steps) {
        // 从 classpath 的 jar 或目录中扫描 .properties 文件
        try {
            String protocol = dirUrl.getProtocol();
            if ("file".equals(protocol)) {
                File dir = new File(dirUrl.toURI());
                if (dir.isDirectory()) {
                    File[] files = dir.listFiles((d, name) -> name.endsWith(".properties"));
                    if (files != null) {
                        for (File file : files) {
                            loadStepFromFile(file, steps);
                        }
                    }
                }
            } else if ("jar".equals(protocol)) {
                // jar 包内扫描
                String jarPath = dirUrl.getPath();
                if (jarPath.startsWith("file:")) {
                    jarPath = jarPath.substring(5);
                }
                if (jarPath.contains("!")) {
                    jarPath = jarPath.substring(0, jarPath.indexOf("!"));
                }
                try (java.util.jar.JarFile jar = new java.util.jar.JarFile(jarPath)) {
                    Enumeration<java.util.jar.JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        java.util.jar.JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        if (name.startsWith(MIGRATIONS_DIR) && name.endsWith(".properties")
                                && !name.equals(MIGRATIONS_DIR)) {
                            try (InputStream is = jar.getInputStream(entry)) {
                                loadStepFromStream(is, steps);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("从目录 {} 加载迁移步骤失败", dirUrl, e);
        }
    }

    private static void loadStepFromFile(File file, List<MigrationStep> steps) {
        try (InputStream is = new FileInputStream(file)) {
            loadStepFromStream(is, steps);
        } catch (Exception e) {
            LOG.warn("从文件 {} 加载迁移步骤失败", file, e);
        }
    }

    private static void loadStepFromStream(InputStream is, List<MigrationStep> steps) throws IOException {
        Properties props = new Properties();
        props.load(is);
        String className = props.getProperty("class");
        if (className == null || className.isBlank()) {
            LOG.warn("迁移步骤配置缺少 class 属性");
            return;
        }
        try {
            Class<?> clazz = Class.forName(className);
            MigrationStep step = (MigrationStep) clazz.getDeclaredConstructor().newInstance();
            steps.add(step);
            LOG.debug("加载迁移步骤: {} ({} → {})", className, step.fromVersion(), step.toVersion());
        } catch (Exception e) {
            LOG.warn("实例化迁移步骤 {} 失败", className, e);
        }
    }
}
