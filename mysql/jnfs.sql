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
-- Table structure for file_location
-- ----------------------------
DROP TABLE IF EXISTS `file_location`;
CREATE TABLE `file_location`  (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `file_hash` char(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '关联 file_metadata.file_hash',
  `datanode_addr` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT 'DataNode地址 (host:port)',
  `status` tinyint NULL DEFAULT 1 COMMENT '状态: 1-正常, 0-损坏',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_hash_node`(`file_hash`, `datanode_addr`) USING BTREE,
  INDEX `idx_node`(`datanode_addr`) USING BTREE
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

SET FOREIGN_KEY_CHECKS = 1;
