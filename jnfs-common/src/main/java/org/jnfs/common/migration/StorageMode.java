package org.jnfs.common.migration;

/**
 * 存储模式枚举
 * 对应配置中的 metadata.mode
 */
public enum StorageMode {
    FILE,
    MYSQL;

    public static StorageMode fromConfig(String mode) {
        if (mode == null || mode.isBlank()) {
            return FILE;
        }
        return "mysql".equalsIgnoreCase(mode) ? MYSQL : FILE;
    }
}
