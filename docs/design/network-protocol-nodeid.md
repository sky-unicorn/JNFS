# JNFS 网络协议设计：node_id 支持

## 1. 设计原则

1. **Packet 二进制格式不变**：不修改 Packet 的魔数、版本号、编解码逻辑
2. **Payload 格式升级**：仅修改 Packet.data 字段中的文本内容格式
3. **向后兼容**：接收方通过分隔符数量自动识别新旧格式
4. **客户端零改动**：客户端与 NameNode 之间的协议完全不变

---

## 2. 协议变更详情

### 2.1 DataNode -> Registry 心跳/注册

#### 涉及命令
- `REGISTRY_REGISTER` (30)
- `REGISTRY_HEARTBEAT` (32)

#### Payload 格式
```
旧格式: {host:port}|{freeSpace}
  示例: "192.168.1.10:5369|107374182400"

新格式: {node_id}|{host:port}|{freeSpace}
  示例: "dn-beijing-01|192.168.1.10:5369|107374182400"
```

#### 解析规则（Registry 端）
```
parts = payload.split("\\|")
if parts.length == 3:
    nodeId = parts[0]
    address = parts[1]
    freeSpace = parseLong(parts[2])
else if parts.length == 2:
    // 旧格式兼容
    address = parts[0]
    nodeId = address  // fallback: 用 host:port 作为 node_id
    freeSpace = parseLong(parts[1])
```

### 2.2 NameNode -> Registry 心跳/注册

#### 涉及命令
- `REGISTRY_REGISTER_NAMENODE` (35)
- `REGISTRY_HEARTBEAT_NAMENODE` (39)

#### Payload 格式
```
旧格式: {host:port}
  示例: "192.168.1.20:5368"

新格式: {node_id}|{host:port}
  示例: "nn-master-01|192.168.1.20:5368"
```

#### 解析规则（Registry 端）
```
parts = payload.split("\\|")
if parts.length == 2:
    nodeId = parts[0]
    address = parts[1]
else:
    // 旧格式兼容: 整个 payload 就是 host:port
    address = payload
    nodeId = address  // fallback
```

### 2.3 Registry -> NameNode DataNode 列表

#### 涉及命令
- `REGISTRY_RESPONSE_DATANODES` (34)

#### Payload 格式
```
旧格式: {host:port}|{freeSpace},{host:port}|{freeSpace},...
  示例: "192.168.1.10:5369|100G,192.168.1.11:5369|200G"

新格式: {node_id}|{host:port}|{freeSpace},{node_id}|{host:port}|{freeSpace},...
  示例: "dn-01|192.168.1.10:5369|100G,dn-02|192.168.1.11:5369|200G"
```

#### 解析规则（NameNode 端）
```
for each nodeEntry in response.split(","):
    parts = nodeEntry.split("\\|")
    if parts.length == 3:
        nodeId = parts[0]
        address = parts[1]
        freeSpace = parseLong(parts[2])
    else if parts.length == 2:
        // 旧格式兼容
        address = parts[0]
        nodeId = address  // fallback
        freeSpace = parseLong(parts[1])
```

### 2.4 Registry -> Driver NameNode 列表

#### 涉及命令
- `REGISTRY_RESPONSE_NAMENODES` (38)

#### Payload 格式
```
旧格式: {host:port},{host:port},...
  示例: "192.168.1.20:5368,192.168.1.21:5368"

新格式: {node_id}|{host:port},{node_id}|{host:port},...
  示例: "nn-01|192.168.1.20:5368,nn-02|192.168.1.21:5368"
```

#### 解析规则（Driver 端）
```
for each nodeEntry in response.split(","):
    if nodeEntry.contains("|"):
        parts = nodeEntry.split("\\|")
        nodeId = parts[0]
        address = parts[1]
    else:
        address = nodeEntry  // 旧格式兼容
    // Driver 只需要 host:port 来连接 NameNode，忽略 nodeId
```

### 2.5 客户端 <-> NameNode（不变）

以下协议**完全不变**：

| 命令 | 方向 | Payload | 说明 |
|------|------|---------|------|
| NAMENODE_CHECK_EXISTENCE (20) | C->N | `{hash}` | 秒传检查 |
| NAMENODE_RESPONSE_EXIST (21) | N->C | `{host:port}` | NameNode 内部已做 node_id -> host:port 转换 |
| NAMENODE_PRE_UPLOAD (23) | C->N | `{hash}` | 预上传 |
| NAMENODE_REQUEST_UPLOAD_LOC (10) | C->N | (empty) | 请求上传节点 |
| NAMENODE_RESPONSE_UPLOAD_LOC (11) | N->C | `{host:port}` | NameNode 内部已做转换 |
| NAMENODE_COMMIT_FILE (12) | C->N | `{filename}\|{hash}\|{host:port}` | 客户端仍传 host:port |
| NAMENODE_REQUEST_DOWNLOAD_LOC (14) | C->N | `{storageId}` | 请求下载位置 |
| NAMENODE_RESPONSE_DOWNLOAD_LOC (15) | N->C | `{filename}\|{hash}\|{host:port}` | NameNode 内部已做转换 |

### 2.6 客户端 <-> DataNode（不变）

| 命令 | 方向 | Payload | 说明 |
|------|------|---------|------|
| UPLOAD_REQUEST (1) | C->D | `{hash}` | 不变 |
| DOWNLOAD_REQUEST (3) | C->D | `{hash}` | 不变 |

---

## 3. NameNode 内部地址转换

NameNode 需要新增一个 `NodeAddressResolver`，在以下两个关键点完成 node_id -> host:port 的转换：

### 3.1 上传时 commit
```
客户端 commit 发送: filename|hash|host:port
NameNode 接收后:
  1. 从 dataNodes 列表中找到 host:port 对应的 node_id
  2. 将 node_id 写入 file_location.datanode_id
  3. 将 node_id 写入 MetadataEntry.address
```

### 3.2 下载时返回地址
```
NameNode 查询到 MetadataEntry.address = node_id
  1. 从本地 NodeRegistryCache 查找 node_id 对应的当前 host:port
  2. 返回 filename|hash|host:port 给客户端
```

### 3.3 上传时选择节点
```
LoadBalancer 从 dataNodes 列表中选择一个条目
  条目格式: "node_id|host:port|freeSpace"
  返回给客户端的: 仅 host:port 部分
```

---

## 4. NodeAddressResolver 设计

```java
package org.jnfs.common;

/**
 * 节点地址解析器
 * 维护 node_id -> host:port 的本地缓存映射
 * 由 NameNode 从 Registry 同步更新
 */
public class NodeAddressResolver {
    // node_id -> host:port
    private static volatile Map<String, String> nodeIdToAddress = Collections.emptyMap();
    // host:port -> node_id (反向)
    private static volatile Map<String, String> addressToNodeId = Collections.emptyMap();

    /**
     * 根据 node_id 获取当前 host:port
     * 如果 node_id 本身就是 host:port 格式（旧数据兼容），直接返回
     */
    public static String resolve(String nodeIdOrAddress) {
        if (isHostPort(nodeIdOrAddress)) {
            return nodeIdOrAddress; // 已经是地址，直接返回
        }
        String addr = nodeIdToAddress.get(nodeIdOrAddress);
        return addr != null ? addr : nodeIdOrAddress; // fallback
    }

    /**
     * 根据 host:port 查找对应的 node_id
     */
    public static String getNodeId(String address) {
        String nodeId = addressToNodeId.get(address);
        return nodeId != null ? nodeId : address; // fallback: 用地址作为 node_id
    }

    /**
     * 全量更新映射（从 Registry 拉取后调用）
     */
    public static void updateMapping(List<String> nodeEntries) {
        // nodeEntries 格式: "node_id|host:port|freeSpace"
        Map<String, String> newIdToAddr = new HashMap<>();
        Map<String, String> newAddrToId = new HashMap<>();
        for (String entry : nodeEntries) {
            String[] parts = entry.split("\\|");
            if (parts.length >= 2) {
                newIdToAddr.put(parts[0], parts[1]);
                newAddrToId.put(parts[1], parts[0]);
            }
        }
        nodeIdToAddress = Collections.unmodifiableMap(newIdToAddr);
        addressToNodeId = Collections.unmodifiableMap(newAddrToId);
    }

    /**
     * 判断字符串是否为 host:port 格式
     */
    public static boolean isHostPort(String s) {
        return s != null && s.matches("^[^|]+:\\d+$");
    }
}
```

---

## 5. 协议版本协商（预留）

当前 Packet.version = 1，本次变更不升级版本号。如果未来需要更激进的协议变更（如修改二进制格式），可以通过以下方式：

1. 客户端在首次连接时发送 `version` 字段
2. 服务端根据 version 选择对应的编解码逻辑
3. 当前所有节点使用 version=1，通过 payload 内容格式区分新旧

---

## 6. 各模块代码变更汇总

### jnfs-common（新增）
- `NodeIdManager.java` — node_id 生成/持久化/读取
- `NodeAddressResolver.java` — node_id <-> host:port 映射

### jnfs-registry
- `RegistryHandler.java` — 心跳解析兼容新旧格式；双层映射；查询返回新格式

### jnfs-namenode
- `NameNodeServer.java` — 心跳 payload 改为 `node_id|host:port`；discovery 解析新格式
- `NameNodeHandler.java` — commit 时做 host:port -> node_id 转换；download 时做 node_id -> host:port 转换
- `WeightedRandomStrategy.java` — 解析 `node_id|host:port|freeSpace` 格式，返回 host:port

### jnfs-datanode
- `DataNodeServer.java` — 心跳 payload 改为 `node_id|host:port|freeSpace`

### jnfs-driver
- `JNFSDriver.java` — discovery 解析兼容新格式（忽略 nodeId，只用 host:port）
