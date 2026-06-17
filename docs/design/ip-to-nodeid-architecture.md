# JNFS node_id 架构设计方案

## 1. 问题分析

### 1.1 现状
当前 JNFS 系统使用 `advertised_host:port` 作为服务器唯一标识。这个标识贯穿整个系统：
- **Registry**: `ConcurrentHashMap<String, NodeInfo>` 的 key 是 `host:port`
- **file_location 表**: `datanode_addr` 字段存储 `host:port`
- **MetadataEntry.address**: 存储 `host:port`
- **namenode_meta.log**: 存储 `host:port`
- **心跳协议**: DataNode 发送 `host:port|freeSpace`，NameNode 发送 `host:port`
- **LoadBalancer**: 基于 `host:port` 选择 DataNode
- **客户端**: commit 时携带 `host:port`，download 时 NameNode 返回 `host:port`

### 1.2 核心问题
当服务器物理机不变但 IP 变更后（如 DHCP 重新分配、机房迁移、网卡更换），`host:port` 随之改变，导致：
1. file_location 表中旧 `datanode_addr` 指向的 IP 无法访问，文件"丢失"
2. namenode_meta.log 中记录的旧地址无法定位文件
3. 无法将新 IP 与旧文件关联

### 1.3 设计目标
1. 引入持久化的 `node_id` 作为服务器唯一标识，与 IP 解耦
2. IP 变更时，系统自动更新 node_id -> IP 映射，旧文件仍可访问
3. 兼容 file 和 mysql 双存储模式
4. 向后兼容老版本客户端（至少可降级运行）

---

## 2. node_id 生成策略

### 2.1 优先级
1. **配置文件指定**（推荐生产环境使用）：运维在配置文件中显式设置 `node_id`
2. **自动生成 UUID**（备选）：首次启动时自动生成并持久化到本地文件

### 2.2 持久化机制
DataNode 和 NameNode 首次启动时：
- 检查配置文件中的 `node_id` 配置项
- 如果配置了，直接使用
- 如果未配置，检查本地持久化文件 `node_id.dat`（存放在当前工作目录）
  - 如果文件存在，读取并使用
  - 如果文件不存在，生成 UUID 并写入 `node_id.dat`

### 2.3 配置示例
```yaml
# datanode.yml / namenode.yml
server:
  port: 5369
  advertised_host: 127.0.0.1
  node_id: "dn-beijing-01"  # 可选，不配置则自动生成
```

---

## 3. 核心数据模型变更

### 3.1 Registry 层

#### 新增 node_registry 概念
Registry 内部维护两层映射：
```
nodeIdToInfo:  ConcurrentHashMap<node_id, NodeInfo>
                  NodeInfo { host:port, freeSpace, lastHeartbeat }

addressToNodeId: ConcurrentHashMap<host:port, node_id>
```

**关键行为**：
- DataNode 注册/心跳时携带 `node_id|host:port|freeSpace`
- Registry 收到心跳后：
  1. 更新 `nodeIdToInfo[node_id]` 的 host:port 和 freeSpace
  2. 更新 `addressToNodeId[host:port] = node_id`
  3. 如果 node_id 已存在但 host:port 不同（IP 变更），自动更新映射
- NameNode 查询 DataNode 列表时，返回格式变为 `node_id|host:port|freeSpace`

#### NameNode 注册同理
NameNode 注册/心跳时携带 `node_id|host:port`，Registry 维护相同的双层映射。

### 3.2 数据库层 (MySQL 模式)

#### 新增 node_registry 表
```sql
CREATE TABLE IF NOT EXISTS `node_registry` (
    `node_id` VARCHAR(128) NOT NULL PRIMARY KEY,
    `node_type` VARCHAR(20) NOT NULL COMMENT 'DATANODE / NAMENODE',
    `host` VARCHAR(100) NOT NULL,
    `port` INT NOT NULL,
    `last_heartbeat` DATETIME NOT NULL,
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY `idx_type` (`node_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### 修改 file_location 表
```sql
-- 新增 node_id 列
ALTER TABLE file_location ADD COLUMN `datanode_id` VARCHAR(128) DEFAULT NULL AFTER `file_hash`;

-- 兼容过渡期：datanode_addr 保留但允许为空
ALTER TABLE file_location MODIFY `datanode_addr` VARCHAR(100) NULL;
```

**查询逻辑变更**：
```sql
-- 新查询：JOIN node_registry 获取当前地址
SELECT m.filename, m.file_hash, m.storage_id, nr.host, nr.port
FROM file_metadata m
JOIN file_location l ON m.file_hash = l.file_hash
LEFT JOIN node_registry nr ON l.datanode_id = nr.node_id
WHERE m.file_hash = ?
```

### 3.3 文件存储层 (File 模式)

#### namenode_meta.log 格式变更
```
旧格式: ADD|filename|hash|host:port|storageId
新格式: ADD|filename|hash|node_id|storageId
```

兼容读取：解析时判断第4个字段是否为 `host:port` 格式（包含 `:` 且可解析为 `host:port`），如果是则视为旧格式。

### 3.4 MetadataEntry 变更
```java
public static class MetadataEntry {
    public final String filename;
    public final String hash;
    public final String nodeId;      // 改为 node_id（原 address）
    public final String storageId;
}
```

### 3.5 心跳协议变更

#### DataNode -> Registry
```
旧格式: host:port|freeSpace
新格式: node_id|host:port|freeSpace
```

#### NameNode -> Registry
```
旧格式: host:port
新格式: node_id|host:port
```

#### Registry -> NameNode (DataNode 列表)
```
旧格式: host:port|freeSpace,host:port|freeSpace,...
新格式: node_id|host:port|freeSpace,node_id|host:port|freeSpace,...
```

---

## 4. IP 变更时的自动恢复机制

### 4.1 流程
```
1. DataNode 重启（IP 从 10.0.0.1 变为 10.0.0.2）
2. DataNode 读取本地 node_id.dat，获得 node_id = "dn-beijing-01"
3. DataNode 向 Registry 发送心跳: "dn-beijing-01|10.0.0.2:5369|100GB"
4. Registry 发现 node_id "dn-beijing-01" 已存在，但 host:port 不同
5. Registry 自动更新 nodeIdToInfo["dn-beijing-01"].address = "10.0.0.2:5369"
6. Registry 更新 addressToNodeId["10.0.0.2:5369"] = "dn-beijing-01"
7. Registry 移除旧的 addressToNodeId["10.0.0.1:5369"] 映射
8. NameNode 下次拉取 DataNode 列表时，获得新的 host:port
9. NameNode 查询 file_location 时，通过 node_id JOIN node_registry 获取最新地址
10. 客户端下载文件时，NameNode 返回新的 IP 地址，文件可正常访问
```

### 4.2 关键点
- **无需人工干预**：整个过程自动完成
- **旧文件自动恢复**：file_location 存储的是 node_id，通过 Registry 的实时映射获取当前 IP
- **心跳驱动**：IP 变更通过心跳自动感知

---

## 5. 双存储模式兼容

### 5.1 MySQL 模式
- file_location 表新增 `datanode_id` 列
- 查询时 JOIN node_registry 获取当前 host:port
- 写入时存储 node_id

### 5.2 File 模式
- namenode_meta.log 格式升级
- 读取时兼容新旧两种格式
- 新增 `node_registry.log` 文件记录 node_id -> host:port 映射（由 NameNode 从 Registry 获取后本地缓存）

### 5.3 新增 NodeAddressResolver 工具类
```java
// 统一地址解析：将 node_id 解析为当前 host:port
public class NodeAddressResolver {
    // 从本地缓存（从 Registry 同步）中查找 node_id 对应的当前地址
    public static String resolve(String nodeIdOrAddress);
    
    // 判断字符串是 node_id 还是 host:port
    public static boolean isHostPort(String s);
}
```

---

## 6. 向后兼容策略

### 6.1 协议兼容
Registry 解析心跳 payload 时：
```java
// 兼容新旧格式
String[] parts = payload.split("\\|");
if (parts.length == 3) {
    // 新格式: node_id|host:port|freeSpace
    nodeId = parts[0];
    address = parts[1];
    freeSpace = parts[2];
} else if (parts.length == 2) {
    // 旧格式: host:port|freeSpace
    // 自动生成 node_id = address（向后兼容）
    address = parts[0];
    nodeId = address; // 用 host:port 作为 fallback node_id
    freeSpace = parts[1];
}
```

### 6.2 数据兼容
- file_location 的 `datanode_addr` 保留，新增 `datanode_id` 列
- 旧数据中 `datanode_id` 为 NULL，此时 fallback 使用 `datanode_addr`
- 新写入的数据同时填充 `datanode_id` 和 `datanode_addr`（过渡期）

### 6.3 客户端兼容
- 客户端仍然从 NameNode 获取 `host:port` 来连接 DataNode（NameNode 内部完成 node_id -> host:port 的转换）
- 客户端无需任何改动

---

## 7. 各模块变更清单

### 7.1 jnfs-common
| 文件 | 变更 |
|------|------|
| `CommandType.java` | 无需新增命令类型（复用现有心跳/注册命令） |
| `ConfigUtil.java` | 无需变更 |
| `HeartbeatSender.java` | 无需变更（payload 由调用方构造） |
| **新增** `NodeIdManager.java` | node_id 的生成、持久化、读取逻辑 |
| **新增** `NodeAddressResolver.java` | node_id -> host:port 解析 |

### 7.2 jnfs-registry
| 文件 | 变更 |
|------|------|
| `RegistryHandler.java` | 双层映射（nodeIdToInfo + addressToNodeId）；心跳解析兼容新旧格式；查询返回新格式 |
| `DashboardServer.java` | Dashboard 展示增加 node_id 列 |

### 7.3 jnfs-namenode
| 文件 | 变更 |
|------|------|
| `NameNodeServer.java` | 初始化 NodeIdManager；心跳 payload 改为 `node_id\|host:port`；discovery 解析新格式 |
| `NameNodeHandler.java` | MetadataEntry.address 改为 nodeId；commit 时存储 node_id；download 时通过 NodeAddressResolver 解析地址 |
| `MetadataCacheManager.java` | MetadataEntry.address 改为 nodeId |
| `MetadataManager.java` | logAddFile 写入 node_id；recover 兼容新旧格式 |
| `MySQLMetadataManager.java` | 建表 SQL 变更；查询 JOIN node_registry；写入 datanode_id |
| `LoadBalancer.java` | 接口不变，但节点列表格式变为 `node_id\|host:port\|freeSpace` |
| `WeightedRandomStrategy.java` | 解析新格式，返回 node_id（或 host:port，取决于下游需要） |
| **新增** `NodeRegistryCache.java` | 本地缓存 node_id -> host:port 映射（从 Registry 同步） |

### 7.4 jnfs-datanode
| 文件 | 变更 |
|------|------|
| `DataNodeServer.java` | 初始化 NodeIdManager；心跳 payload 改为 `node_id\|host:port\|freeSpace` |

### 7.5 jnfs-driver
| 文件 | 变更 |
|------|------|
| `JNFSDriver.java` | 无需变更（客户端仍然使用 host:port 连接 DataNode） |

### 7.6 配置文件
| 文件 | 变更 |
|------|------|
| `datanode.yml` | 新增 `server.node_id` 配置项 |
| `namenode.yml` | 新增 `server.node_id` 配置项 |

---

## 8. 数据流图

### 8.1 上传流程（变更后）
```
Client                    NameNode                  Registry                DataNode
  |                          |                         |                       |
  |-- PRE_UPLOAD(hash) ----->|                         |                       |
  |                          |                         |                       |
  |<-- ALLOW ----------------|                         |                       |
  |                          |                         |                       |
  |-- REQUEST_UPLOAD_LOC --->|                         |                       |
  |                          |-- 从 dataNodes 列表      |                       |
  |                          |   选择 node_id           |                       |
  |                          |-- NodeAddressResolver    |                       |
  |                          |   解析为 host:port       |                       |
  |<-- host:port ------------|                         |                       |
  |                          |                         |                       |
  |-- 上传文件到 DataNode ----------------------------->|                       |
  |<-- 上传成功 ----------------------------------------|                       |
  |                          |                         |                       |
  |-- COMMIT(name,hash,      |                         |                       |
  |       host:port) ------->|                         |                       |
  |                          |-- 将 host:port 转换为    |                       |
  |                          |   node_id（查本地缓存）   |                       |
  |                          |-- 持久化: node_id        |                       |
  |<-- storageId -------------|                         |                       |
```

### 8.2 下载流程（变更后）
```
Client                    NameNode                  Registry                DataNode
  |                          |                         |                       |
  |-- REQUEST_DOWNLOAD(storageId) ->|                   |                       |
  |                          |-- 查缓存/DB 获取        |                       |
  |                          |   node_id               |                       |
  |                          |-- NodeAddressResolver    |                       |
  |                          |   解析为 host:port       |                       |
  |<-- filename|hash|host:port|                        |                       |
  |                          |                         |                       |
  |-- 从 DataNode 下载文件 ---------------------------->|                       |
  |<-- 文件数据 ----------------------------------------|                       |
```

### 8.3 IP 变更恢复流程
```
DataNode (新IP)            Registry                  NameNode
  |                          |                         |
  |-- HEARTBEAT              |                         |
  |   node_id|newIP:port     |                         |
  |   |freeSpace ----------->|                         |
  |                          |-- 检测 node_id 已存在    |
  |                          |   但 host:port 不同      |
  |                          |-- 更新 nodeIdToInfo     |
  |                          |-- 更新 addressToNodeId  |
  |                          |                         |
  |                          |   (下次 discovery)       |
  |                          |<-- GET_DATANODES -------|
  |                          |-- 返回新格式列表         |
  |                          |   node_id|newIP|space   |
  |                          |------------------------>|
  |                          |                         |-- 更新本地缓存
  |                          |                         |   node_id -> newIP
  |                          |                         |
  |                          |   (客户端下载旧文件)      |
  |                          |                         |<-- REQUEST_DOWNLOAD
  |                          |                         |-- 查 file_location
  |                          |                         |   得到 node_id
  |                          |                         |-- 从缓存解析 newIP
  |                          |                         |-- 返回 newIP
  |<-- DOWNLOAD_REQUEST -------------------------------|
  |-- 文件数据 --------------------------------------->|
```

---

## 9. 实施顺序建议

1. **Phase 1: 基础设施** (Task #1, #5)
   - 数据库表变更（新增 node_registry，修改 file_location）
   - 新增 NodeIdManager、NodeAddressResolver 工具类
   - 配置文件模板更新

2. **Phase 2: Registry 改造** (Task #3)
   - RegistryHandler 双层映射
   - 心跳协议兼容新旧格式

3. **Phase 3: DataNode 改造** (Task #9)
   - 集成 NodeIdManager
   - 心跳 payload 变更

4. **Phase 4: NameNode 改造** (Task #2, #6)
   - 元数据存储改用 node_id
   - LoadBalancer 适配新格式
   - NodeRegistryCache 本地缓存

5. **Phase 5: 测试与审查** (Task #7)
   - 兼容性测试
   - IP 变更恢复测试
   - 双存储模式测试

---

## 10. 风险与注意事项

1. **过渡期数据一致性**：file_location 表新增 `datanode_id` 列后，旧数据该列为 NULL。查询时需要 fallback 到 `datanode_addr`。
2. **node_id 唯一性**：如果使用手动配置，运维需确保 node_id 全局唯一。建议在 Registry 注册时检测冲突。
3. **File 模式下的 node_registry**：File 模式没有 MySQL，需要新增 `node_registry.log` 文件来持久化 node_id -> host:port 映射。
4. **老版本 DataNode**：如果老版本 DataNode 不发送 node_id，Registry 用 `host:port` 作为 fallback node_id，功能降级但不会崩溃。
