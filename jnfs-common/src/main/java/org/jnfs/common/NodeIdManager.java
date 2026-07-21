package org.jnfs.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * 节点ID管理器
 * 负责节点唯一标识(node_id)的生成、持久化和读取
 *
 * 优先级：
 * 1. 配置文件指定 (server.node_id)
 * 2. 本地持久化文件 (node_id.dat)
 * 3. 自动生成 UUID 并持久化
 */
public final class NodeIdManager {

    private static final Logger LOG = LoggerFactory.getLogger(NodeIdManager.class);

    private static final String NODE_ID_FILE = "node_id.dat";

    private NodeIdManager() {
        // 工具类，禁止实例化
    }

    /**
     * 初始化节点ID
     * 按优先级获取：配置 > 本地文件 > 自动生成
     *
     * @param serverConfig server 配置节，可能包含 node_id 配置项
     * @return 节点ID
     */
    @SuppressWarnings("unchecked")
    public static String initialize(Map<String, Object> serverConfig) {
        // 1. 优先从配置文件读取
        if (serverConfig != null && serverConfig.containsKey("node_id")) {
            String configNodeId = (String) serverConfig.get("node_id");
            if (configNodeId != null && !configNodeId.trim().isEmpty()) {
                configNodeId = configNodeId.trim();
                LOG.info("使用配置文件指定的 node_id: {}", configNodeId);
                // 配置指定时也写入本地文件，以便其他组件读取
                saveNodeIdToFile(configNodeId);
                return configNodeId;
            }
        }

        // 2. 尝试从本地持久化文件读取
        String existingId = loadNodeIdFromFile();
        if (existingId != null) {
            LOG.info("从本地文件加载 node_id: {}", existingId);
            return existingId;
        }

        // 3. 自动生成 UUID 并持久化
        String generatedId = UUID.randomUUID().toString();
        LOG.info("自动生成 node_id: {}", generatedId);
        saveNodeIdToFile(generatedId);
        return generatedId;
    }

    /**
     * 从本地文件加载 node_id
     */
    private static String loadNodeIdFromFile() {
        File file = DataDirResolver.resolve(NODE_ID_FILE);
        if (!file.exists()) {
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = reader.readLine();
            if (line != null && !line.trim().isEmpty()) {
                return line.trim();
            }
        } catch (IOException e) {
            LOG.warn("读取 node_id 文件失败: {}", e.getMessage());
        }
        return null;
    }

    /**
     * 将 node_id 持久化到本地文件
     */
    private static void saveNodeIdToFile(String nodeId) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(DataDirResolver.resolve(NODE_ID_FILE)))) {
            writer.write(nodeId);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            LOG.warn("写入 node_id 文件失败: {}", e.getMessage());
        }
    }
}
