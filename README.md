# JNFS (Java Network File System)

JNFS 是一个轻量级的、基于 Java Netty 实现的分布式文件系统。它采用类似于 HDFS 的 Master-Slave 架构设计，支持文件上传、下载、秒传、流式解密和多路径存储。

## 🚀 核心特性

- **高性能通信**: 基于 Netty 实现的自定义二进制协议，支持 NIO 和零拷贝传输。
- **分布式架构**: 
  - **NameNode**: 管理文件元数据（文件名、Hash、位置）和负载均衡，支持分段锁高并发处理。
  - **DataNode**: 负责实际的数据块存储与读取，支持**多磁盘/多路径挂载**扩容。
  - **Registry**: 注册中心，负责 DataNode 的自动发现与心跳检测，具备自动过期清理机制。
- **智能传输**: 
  - **秒传**: 基于 SHA-256 实现文件去重，相同文件无需重复上传。
  - **流式传输**: 支持大文件流式读写，防止内存溢出。
  - **边下边解密**: 下载时实时流式解密，无需临时文件，降低延迟与磁盘占用。
- **安全机制**: 
  - 基础的 Token 鉴权。
  - AES 文件加密存储。
  - **安全加固**: 包含路径遍历防御、OOM 攻击防御、连接池防泄露等安全特性。
- **SDK 支持**: 提供开箱即用的 Java Client SDK (`jnfs-driver`)。

## 📦 模块说明

| 模块 | 说明 |
| :--- | :--- |
| `jnfs-common` | 通用组件库，包含 Packet 协议定义、编解码器、工具类 |
| `jnfs-registry` | 注册中心服务端，维护活跃的 DataNode 列表 |
| `jnfs-namenode` | 元数据管理节点，处理客户端请求，调度 DataNode |
| `jnfs-datanode` | 数据存储节点，处理文件流的实际读写 |
| `jnfs-driver` | 客户端 SDK，供第三方应用集成 JNFS |
| `jnfs-example` | 包含使用示例和集成测试代码 |
| `jnfs-distribution` | **打包模块**，用于生成包含所有依赖和脚本的最终发布包 |

## 🛠️ 快速开始

### 1. 环境准备
- JDK 17+
- Maven 3.6+

### 2. 构建发布包
在项目根目录下执行 Maven 打包命令 (使用 `dist` profile)：
```bash
mvn clean package -Pdist
```
构建成功后，在 `jnfs-distribution/target` 目录下会生成最终的发布包：
- `jnfs-dist-{version}.zip`
- `jnfs-dist-{version}.tar.gz`

### 3. 部署与启动
解压发布包到任意目录，进入 `bin` 文件夹即可启动服务。

**启动脚本**:
- Windows: `bin/start.bat`
- Linux/Mac: `bin/start.sh` (需先 `chmod +x bin/start.sh`)

**启动命令示例**:

1. **启动注册中心 (Registry)**
   ```bash
   ./bin/start.sh registry
   ```
   *默认端口: 5367*

2. **启动 NameNode**
   ```bash
   ./bin/start.sh namenode
   ```
   *默认端口: 5368*

3. **启动 DataNode**
   ```bash
   ./bin/start.sh datanode
   ```
   *默认端口: 5369*

4. **运行综合测试**
   ```bash
   ./bin/start.sh example
   ```
   *运行交互式测试控制台，包含上传、下载、并发、安全等测试场景。*

### ⚠️ 集群部署特别说明

若需部署 **NameNode 集群** (多实例运行)，请严格遵守以下约束：

1. **元数据存储必须使用 MySQL**:
   - 必须在 `conf/namenode.yml` 中设置 `metadata.mode: mysql`。
   - ❌ **严禁使用 `file` 模式**：`file` 模式仅将元数据存储在各节点的本地磁盘，会导致集群脑裂、数据不一致。
   - 所有 NameNode 节点必须配置连接到**同一个** MySQL 数据库实例。

2. **分布式协同**:
   - MySQL 模式下，系统会自动启用基于数据库的分布式锁 (`file_upload_lock` 表)，确保文件上传时的并发安全。

3. **注册中心高可用**:
   - 客户端和服务端配置 `registry.addresses` 时，建议填写所有 Registry 节点的地址 (逗号分隔)，以实现自动故障切换。

### 4. SDK 使用示例
JNFS 提供了灵活的 Java SDK，支持**单点直连**和**集群发现**两种模式。

#### 初始化方式

**方式一：直连模式 (简单测试)**
适用于开发测试或 NameNode 单点部署的场景。
```java
// 直接连接指定的 NameNode IP 和端口
JNFSDriver driver = new JNFSDriver("localhost", 5368);
```

**方式二：高可用模式 (推荐生产使用)**
连接注册中心，自动发现可用的 NameNode 集群，支持负载均衡和故障转移。
```java
// 连接 Registry，自动获取 NameNode 列表
JNFSDriver driver = JNFSDriver.useRegistry("localhost:5367,localhost:5368");
```

#### 完整代码示例

```java
import org.jnfs.driver.JNFSDriver;
import java.io.File;

public class Demo {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Driver (此处演示高可用模式)
        JNFSDriver driver = JNFSDriver.useRegistry("localhost:5367");

        try {
            // 2. 上传文件
            File file = new File("path/to/video.mp4");
            String storageId = driver.uploadFile(file);
            System.out.println("文件上传成功，存储ID: " + storageId);

            // 3. 下载文件 (支持流式解密)
            // 指定下载目录 (自动使用原文件名) 或 完整文件路径
            File downloadedFile = driver.downloadFile(storageId, "downloads/");
            System.out.println("文件已下载到: " + downloadedFile.getAbsolutePath());
            
        } finally {
            // 4. 关闭连接资源
            driver.close();
        }
    }
}
```

## ⚙️ 配置说明

发布包解压后的 `conf/` 目录下包含各服务的配置文件，修改后重启生效。

### datanode.yml (DataNode 配置)
```yaml
server:
  port: 5369
  # 自动获取本机 IP，也可手动指定广播地址
  # advertised_host: 192.168.1.100

storage:
  # 支持配置多个存储路径，实现单节点扩容
  paths:
    - D:/data/jnfs/storage1
    - D:/data/jnfs/storage2

registry:
  # 方式 1: 单点配置
  # host: localhost
  # port: 5367
  
  # 方式 2: 集群配置 (推荐)
  addresses: localhost:5367,localhost:5368
```

### namenode.yml (NameNode 配置)
```yaml
server:
  port: 5368

registry:
  # 方式 1: 单点配置
  # host: localhost
  # port: 5367
  
  # 方式 2: 集群配置 (推荐)
  addresses: localhost:5367,localhost:5368
  
# 元数据存储配置
metadata:
  # 存储模式:
  # - file: 本地文件存储 (仅适合单机部署，无法共享元数据)
  # - mysql: 数据库存储 (集群模式下必须使用 mysql，否则各节点元数据不一致且无法协同)
  mode: file 

  # MySQL 连接配置 (当 mode: mysql 时生效)
  mysql:
    host: localhost
    port: 3306
    database: jnfs
    user: root
    password: password
```

### registry.yml (Registry 配置)
```yaml
server:
  port: 5367
  
dashboard:
  port: 15367

heartbeat:
  timeout_ms: 30000 # 心跳超时时间 (毫秒)
```
```
