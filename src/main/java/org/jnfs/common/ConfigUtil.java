package org.jnfs.common;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * 配置加载工具类
 */
public class ConfigUtil {

    /**
     * 加载 YAML 配置文件
     * 优先从运行目录查找，如果没有则从 classpath 加载
     */
    public static Map<String, Object> loadConfig(String fileName) {
        Yaml yaml = new Yaml();
        
        // 1. 尝试从外部运行目录加载 (便于生产环境修改配置)
        try {
            if (Files.exists(Paths.get(fileName))) {
                System.out.println("加载外部配置文件: " + fileName);
                try (InputStream in = Files.newInputStream(Paths.get(fileName))) {
                    return yaml.load(in);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 2. 尝试从 classpath 加载
        System.out.println("加载 Classpath 配置文件: " + fileName);
        try (InputStream in = ConfigUtil.class.getClassLoader().getResourceAsStream(fileName)) {
            if (in == null) {
                throw new RuntimeException("找不到配置文件: " + fileName);
            }
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败: " + fileName, e);
        }
    }
}
