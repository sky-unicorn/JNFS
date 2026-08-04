package org.jnfs.common.migration;

/**
 * 存储模式枚举
 * 对应 Registry 端 storage.mode（NameNode 启动时从 Registry 拉取，不再本地配置）
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
