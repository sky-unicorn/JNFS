package org.jnfs.common.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 存储模式枚举
 * <p>
 * 对应 Registry 端 storage.mode（NameNode 启动时从 Registry 拉取，不再本地配置）。
 * <ul>
 *   <li>H2    ：嵌入式 H2 文件库（运行时默认的单机模式，file 模式的替代实现，单副本）</li>
 *   <li>MYSQL ：远端 MySQL 模式（生产环境多副本）</li>
 *   <li>FILE  ：仅作为迁移框架内部的前置步骤使用（规整历史 namenode_meta.log），
 *               不是运行时存储模式，新部署不使用</li>
 * </ul>
 */
public enum StorageMode {
    FILE,
    MYSQL,
    H2;

    private static final Logger LOG = LoggerFactory.getLogger(StorageMode.class);

    /**
     * 从配置字符串解析存储模式
     * <ul>
     *   <li>"h2"    → H2</li>
     *   <li>"mysql" → MYSQL</li>
     *   <li>其它（含 "file" / null / 空白）→ H2，并打 WARN 提示 file 已退役</li>
     * </ul>
     */
    public static StorageMode fromConfig(String mode) {
        if (mode == null || mode.isBlank()) {
            LOG.warn("storage.mode 为空，已退役的 file 模式不再支持，默认走 H2 嵌入式文件库");
            return H2;
        }
        String normalized = mode.trim().toLowerCase();
        switch (normalized) {
            case "h2":
                return H2;
            case "mysql":
                return MYSQL;
            default:
                // 含已退役的 "file" 模式：旧配置自动回落到 H2 并提示
                LOG.warn("storage.mode='{}' 已退役（file 模式被 H2 嵌入式文件库替代），将走 H2", mode);
                return H2;
        }
    }
}
