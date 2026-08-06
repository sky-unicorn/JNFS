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

 Date: 05/08/2026 (V5 schema — H2 嵌入式文件库复用同一 schema，无 DDL 变更)
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
-- Records of schema_version (V5)
-- ----------------------------
INSERT INTO `schema_version` VALUES (5, NOW());

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
  `status` tinyint NOT NULL DEFAULT 1 COMMENT '状态: 1-正常(ACTIVE), 0-损坏(CORRUPT)',
  `replica_role` tinyint NOT NULL DEFAULT 0 COMMENT '0=PRIMARY,1=SECONDARY',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `uk_hash_node`(`file_hash`, `datanode_id`) USING BTREE,
  INDEX `idx_node`(`datanode_id`) USING BTREE,
  INDEX `idx_hash_status`(`file_hash`, `status`) USING BTREE
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
  `replication_factor` tinyint NOT NULL DEFAULT 1 COMMENT '目标副本数；1=单副本，2/3=组内节点数',
  `create_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT '创建时间',
  `update_time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),
  PRIMARY KEY (`storage_id`) USING BTREE,
  INDEX `idx_hash`(`file_hash`) USING BTREE,
  INDEX `idx_filename`(`filename`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci COMMENT = '文件元数据表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of file_metadata
-- ----------------------------

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
-- Table structure for replication_group
-- ----------------------------
DROP TABLE IF EXISTS `replication_group`;
CREATE TABLE `replication_group` (
  `group_id` varchar(64) NOT NULL COMMENT '组ID',
  `group_name` varchar(128) NOT NULL COMMENT '组名',
  `node_ids` varchar(512) NOT NULL COMMENT '组成员node_id列表,逗号分隔(2~3个)',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='冗余组配置表';

-- ----------------------------
-- Table structure for replica_sync_task
-- ----------------------------
DROP TABLE IF EXISTS `replica_sync_task`;
CREATE TABLE `replica_sync_task` (
  `task_id` varchar(64) NOT NULL COMMENT '任务ID',
  `file_hash` char(64) NOT NULL COMMENT '文件hash',
  `source_node` varchar(128) NOT NULL COMMENT '源节点(primary)',
  `target_node` varchar(128) NOT NULL COMMENT '目标节点',
  `status` tinyint NOT NULL DEFAULT 0 COMMENT '0=PENDING,1=IN_FLIGHT,2=DONE,3=FAILED',
  `retry_count` tinyint NOT NULL DEFAULT 0 COMMENT '累计失败次数(达4告警)',
  `file_size` bigint NOT NULL DEFAULT 0 COMMENT '文件大小(字节,用于限速与超时)',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`task_id`),
  UNIQUE KEY `uk_hash_target` (`file_hash`, `target_node`),
  INDEX `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账同步任务表';

-- ----------------------------
-- Table structure for replication_policy
-- ----------------------------
DROP TABLE IF EXISTS `replication_policy`;
CREATE TABLE `replication_policy` (
  `id` tinyint NOT NULL DEFAULT 1,
  `sync_window_start` varchar(5) NOT NULL DEFAULT '01:00',
  `sync_window_end` varchar(5) NOT NULL DEFAULT '03:00',
  `soft_deadline` varchar(5) NOT NULL DEFAULT '03:00',
  `rate_limit_mbps` int NOT NULL DEFAULT 50,
  `max_concurrency` int NOT NULL DEFAULT 4,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步策略配置';

-- ----------------------------
-- Records of replication_policy (seed row)
-- ----------------------------
INSERT IGNORE INTO `replication_policy` (`id`) VALUES (1);

-- ----------------------------
-- Table structure for replication_control
-- ----------------------------
DROP TABLE IF EXISTS `replication_control`;
CREATE TABLE `replication_control` (
  `id` tinyint NOT NULL DEFAULT 1,
  `manual_sync_requested` tinyint NOT NULL DEFAULT 0,
  `requested_at` datetime NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='对账控制信号';

-- ----------------------------
-- Records of replication_control (seed row)
-- ----------------------------
INSERT IGNORE INTO `replication_control` (`id`) VALUES (1);

-- ----------------------------
-- Table structure for node_drain
-- ----------------------------
DROP TABLE IF EXISTS `node_drain`;
CREATE TABLE `node_drain` (
  `node_id` varchar(128) NOT NULL COMMENT '节点ID（关联运行时节点，非外键）',
  `drain_status` tinyint NOT NULL DEFAULT 0 COMMENT '0=ACTIVE, 1=DRAINING',
  `drain_at` datetime NULL DEFAULT NULL COMMENT 'DRAINING 置位时间（清除时置 NULL）',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='节点排空状态表';

-- ----------------------------
-- Registry Dashboard 鉴权用户表
-- 说明：此表由 Registry 进程的 Dashboard 登录功能使用，与冗余元数据表同库（jnfs），无外键依赖。
--       统一存储配置见 registry.yml: storage（mode=mysql 时由 MysqlUserStore 共享同一 DataSource）。
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
