package org.jnfs.registry.auth;

import org.jnfs.common.DataDirResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;

/**
 * File 模式用户存储
 * <p>
 * 使用 Java Properties 格式存储在 APP_HOME/dashboard_users.properties。
 * key=username, value=BCrypt 哈希串。文件头注释标注密码已加密。
 * <p>
 * 写入采用临时文件 + fsync + 原子 renameTo 模式（参考 MigrationRunner.writeFileVersionAtomic），
 * 保证崩溃安全。写方法加 synchronized 防止并发损坏。
 * <p>
 * 安全：仅接受/写入以 $2a$/$2b$/$2y$ 开头的 BCrypt 哈希，绝不落明文。
 */
public class FileUserStore implements UserStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileUserStore.class);

    private static final String USER_FILE_NAME = "dashboard_users.properties";
    private static final String USER_TMP_NAME = "dashboard_users.properties.tmp";
    private static final String FILE_HEADER = "JNFS Dashboard 用户密码哈希 - 密码已使用 BCrypt 加密，请勿手动修改";

    private final File userFile;
    private final File tmpFile;

    public FileUserStore() {
        this.userFile = DataDirResolver.resolve(USER_FILE_NAME);
        this.tmpFile = DataDirResolver.resolve(USER_TMP_NAME);
        LOG.info("FileUserStore: 用户文件路径: {}", userFile.getAbsolutePath());
    }

    @Override
    public String findPasswordHash(String username) {
        Properties props = loadProperties();
        return props.getProperty(username);
    }

    @Override
    public synchronized void saveUser(String username, String bcryptHash) {
        assertBcryptHash(bcryptHash);
        Properties props = loadProperties();
        if (props.containsKey(username)) {
            LOG.warn("FileUserStore: 用户 '{}' 已存在，跳过重复创建", username);
            return;
        }
        props.setProperty(username, bcryptHash);
        atomicWriteProperties(props);
        LOG.info("FileUserStore: 用户 '{}' 已保存", username);
    }

    @Override
    public synchronized boolean updatePassword(String username, String newBcryptHash) {
        assertBcryptHash(newBcryptHash);
        Properties props = loadProperties();
        if (!props.containsKey(username)) {
            LOG.warn("FileUserStore: 用户 '{}' 不存在，无法修改密码", username);
            return false;
        }
        props.setProperty(username, newBcryptHash);
        atomicWriteProperties(props);
        LOG.info("FileUserStore: 用户 '{}' 密码已更新", username);
        return true;
    }

    @Override
    public int userCount() {
        Properties props = loadProperties();
        // Properties 继承 Hashtable，keySet 包含默认属性；只统计非注释、非空值
        int count = 0;
        for (String key : props.stringPropertyNames()) {
            if (!props.getProperty(key).isEmpty()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void close() {
        // file 模式无资源需要关闭
    }

    // ==================== 内部方法 ====================

    private Properties loadProperties() {
        Properties props = new Properties();
        if (!userFile.exists()) {
            return props;
        }
        try (InputStream in = new FileInputStream(userFile)) {
            props.load(in);
        } catch (IOException e) {
            LOG.error("FileUserStore: 读取用户文件失败: {}", userFile, e);
        }
        return props;
    }

    /**
     * 原子写入 Properties 文件：先写临时文件 → fsync → renameTo 覆盖
     * 参考 MigrationRunner.writeFileVersionAtomic 模式
     */
    private void atomicWriteProperties(Properties props) {
        // 清理残留临时文件
        if (tmpFile.exists()) {
            tmpFile.delete();
        }

        // 写入临时文件
        try (OutputStream out = new FileOutputStream(tmpFile)) {
            props.store(out, FILE_HEADER);
        } catch (IOException e) {
            LOG.error("FileUserStore: 写入临时文件失败: {}", tmpFile, e);
            return;
        }

        // fsync 确保数据落盘
        try (FileOutputStream fos = new FileOutputStream(tmpFile, true)) {
            fos.getFD().sync();
        } catch (IOException e) {
            LOG.error("FileUserStore: fsync 失败", e);
            return;
        }

        // 原子替换
        if (!tmpFile.renameTo(userFile)) {
            try {
                Files.move(tmpFile.toPath(), userFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException ex) {
                LOG.error("FileUserStore: 原子替换用户文件失败", ex);
                return;
            }
        }

        // 尝试设置文件权限为仅 owner 可读写（Linux/Mac）
        setOwnerOnlyPermissions();
    }

    /**
     * 设置文件权限为 rw-------（仅 owner 可读写），Windows 下忽略
     */
    private void setOwnerOnlyPermissions() {
        try {
            if (userFile.exists()) {
                Set<PosixFilePermission> perms = new HashSet<>();
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_WRITE);
                Files.setPosixFilePermissions(userFile.toPath(), perms);
            }
        } catch (UnsupportedOperationException e) {
            // Windows 不支持 POSIX 权限，忽略
        } catch (IOException e) {
            LOG.warn("FileUserStore: 设置文件权限失败（非关键）: {}", e.getMessage());
        }
    }

    /**
     * 断言哈希值是 BCrypt 格式，防止明文写入文件
     */
    private void assertBcryptHash(String hash) {
        if (hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("密码哈希不能为空");
        }
        if (!hash.startsWith("$2a$") && !hash.startsWith("$2b$") && !hash.startsWith("$2y$")) {
            throw new IllegalArgumentException(
                    "密码哈希必须是 BCrypt 格式（以 $2a$/$2b$/$2y$ 开头），禁止存储明文密码");
        }
    }
}
