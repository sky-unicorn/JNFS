package org.jnfs.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * 应用主目录初始化器
 *
 * 在 logback 初始化之前，确保 APP_HOME 系统属性已正确设置。
 *
 * 优先级：
 * 1. 已通过 -DAPP_HOME=... 指定（打包运行场景）—— 不修改
 * 2. 向上查找父 pom.xml（包含 {@code <packaging>pom</packaging>}）推断项目根目录
 * 3. 回退到 user.dir
 *
 * 必须在 Server 启动类的 static 块中调用，且该 static 块必须位于 LOG 字段之前。
 */
public final class AppHomeInitializer {

    private AppHomeInitializer() {
    }

    /**
     * 确保 APP_HOME 系统属性已设置。幂等调用。
     */
    public static void init() {
        String appHome = System.getProperty("APP_HOME");
        if (appHome != null && !appHome.trim().isEmpty()) {
            return; // 已通过启动脚本设置
        }

        String projectRoot = findProjectRoot();
        System.setProperty("APP_HOME", projectRoot);
    }

    private static String findProjectRoot() {
        File dir = new File(System.getProperty("user.dir")).getAbsoluteFile();
        while (dir != null) {
            File pomFile = new File(dir, "pom.xml");
            if (pomFile.isFile() && isParentPom(pomFile)) {
                return dir.getAbsolutePath();
            }
            dir = dir.getParentFile();
        }
        return System.getProperty("user.dir");
    }

    private static boolean isParentPom(File pomFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(pomFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("<packaging>pom</packaging>")
                        || line.contains("<modules>")) {
                    return true;
                }
            }
        } catch (IOException ignored) {
        }
        return false;
    }
}
