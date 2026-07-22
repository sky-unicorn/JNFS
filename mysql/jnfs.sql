/*
 Navicat Premium Data Transfer

 Source Server         : 100.10.11.204_文件服务器
 Source Server Type    : MySQL
 Source Server Version : 80020
 Source Host           : 100.10.11.204:3306
 Source Schema         : jnfs

 Target Server Type    : MySQL
 Target Server Version : 80020
 File Encoding         : 65001

 Date: 05/01/2026 14:35:49
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for schema_version
-- ----------------------------
DROP TABLE IF EXISTS `schema_version`;
CREATE TABLE `schema_version` (
  `version` int NOT NULL COMMENT '当前 schema 版本',
  `upgraded_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '升级时间',
  PRIMARY KEY (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='schema 版本记录';

-- ----------------------------
-- Table structure for node_registry
-- ----------------------------
DROP TABLE IF EXISTS `node_registry`;
CREATE TABLE `node_registry`  (
  `node_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '节点唯一标识',
  `node_type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '节点类型: DATANODE / NAMENODE',
  `host` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '节点IP地址',
  `port` int NOT NULL COMMENT '节点端口',
  `last_heartbeat` datetime(0) NOT NULL COMMENT '最后心跳时间',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`node_id`) USING BTREE,
  INDEX `idx_type`(`node_type`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '节点注册表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for file_location
-- ----------------------------
DROP TABLE IF EXISTS `file_location`;
CREATE TABLE `file_location`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `file_hash` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '关联 file_metadata.file_hash',
  `datanode_id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'DataNode节点ID (关联 node_registry.node_id)',
  `datanode_addr` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL COMMENT 'DataNode地址 (host:port, 兼容旧数据)',
  `status` tinyint NULL DEFAULT 1 COMMENT '状态: 1-正常, 0-损坏',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_hash_node`(`file_hash`, `datanode_id`) USING BTREE,
  INDEX `idx_node`(`datanode_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '文件存储位置映射表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of file_location
-- ----------------------------

-- ----------------------------
-- Table structure for file_metadata
-- ----------------------------
DROP TABLE IF EXISTS `file_metadata`;
CREATE TABLE `file_metadata`  (
  `storage_id` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '存储ID (UUID), 主键',
  `filename` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '原始文件名',
  `file_hash` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '文件哈希 (SHA-256)',
  `file_size` bigint NULL DEFAULT 0 COMMENT '文件大小 (字节)',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建时间',
  `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`storage_id`) USING BTREE,
  INDEX `idx_hash`(`file_hash`) USING BTREE,
  INDEX `idx_filename`(`filename`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '文件元数据表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for file_upload_lock
-- ----------------------------
DROP TABLE IF EXISTS `file_upload_lock`;
CREATE TABLE `file_upload_lock` (
  `file_hash` char(64) NOT NULL COMMENT '锁Key：文件的Hash值',
  `namenode_id` varchar(64) NOT NULL COMMENT '持有锁的服务节点标识',
  `expire_time` datetime NOT NULL COMMENT '锁过期时间(防止死锁)',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`file_hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文件上传分布式锁表';

-- ----------------------------
-- Records of file_metadata
-- ----------------------------

-- ----------------------------
-- Registry Dashboard 鉴权用户表
-- 说明：此表由 Registry 进程的 Dashboard 登录功能使用，与 NameNode 元数据表物理隔离、无外键依赖。
--       默认建议建在独立数据库 jnfs_registry（见 registry.yml: dashboard.auth.storage.mysql.database）；
--       若与 NameNode 共用同一数据库，可直接执行下方 DDL。
--       password_hash 仅存 BCrypt 哈希（$2a$...），绝不存明文。
-- ----------------------------
DROP TABLE IF EXISTS `dashboard_user`;
CREATE TABLE `dashboard_user` (
  `username` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '用户名',
  `password_hash` varchar(72) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'BCrypt 密码哈希',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`username`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = 'Dashboard 登录用户表' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
