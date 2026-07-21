package org.jnfs.common;

import java.io.File;

/**
 * 数据目录解析器
 *
 * 统一运行时数据文件（如 namenode_meta.log、node_id.dat、meta_version）的基准目录。
 * 优先使用启动脚本设置的 APP_HOME 系统属性，未设置时回退到 JVM 工作目录。
 *
 * <p>禁止在数据文件定位逻辑中直接依赖相对路径或 JVM 工作目录，以避免脚本工作目录不同导致
 * 迁移器与业务组件看到不同的数据文件位置。</p>
 */
public final class DataDirResolver {

    private DataDirResolver() {
        // 工具类，禁止实例化
    }

    /**
     * 获取数据目录根路径
     * @return APP_HOME 对应的目录，不存在时回退到 user.dir
     */
    public static File dataDir() {
        return new File(System.getProperty("APP_HOME", System.getProperty("user.dir")));
    }

    /**
     * 将指定文件名解析为数据目录下的 File
     * @param filename 文件名
     * @return 以数据目录为基准的文件对象
     */
    public static File resolve(String filename) {
        return new File(dataDir(), filename);
    }
}
