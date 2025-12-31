# JNFS (Java Network File System)

JNFS 是一个轻量级的、基于 Java Netty 实现的分布式文件系统。它采用类似于 HDFS 的 Master-Slave 架构设计，支持文件上传、下载、秒传和分布式存储。

## 🚀 核心特性

- **高性能通信**: 基于 Netty 实现的自定义二进制协议，支持 NIO 和零拷贝传输。
- **分布式架构**: 
  - **NameNode**: 管理文件元数据（文件名、Hash、位置）和负载均衡。
  - **DataNode**: 负责实际的数据块存储与读取。
  - **Registry**: 注册中心，负责 DataNode 的自动发现与心跳检测。
- **智能传输**: 
  - **秒传**: 基于 SHA-256 实现文件去重，相同文件无需重复上传。
  - **流式传输**: 支持大文件流式读写，防止内存溢出。
- **安全机制**: 基础的 Token 鉴权与 AES 文件加密传输。
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

## 🛠️ 快速开始

### 1. 环境准备
- JDK 17+
- Maven 3.6+

### 2. 构建项目
在项目根目录下执行 Maven 打包命令：
```bash
mvn clean package
```
构建成功后，各模块的 `target` 目录下会生成包含所有依赖的可执行 JAR 包 (Fat JAR)。

### 3. 启动服务
项目提供了便捷的启动脚本，位于 `bin/` 目录下。

**Windows 用户**:
直接双击运行 `bin/start-all.bat`，它会自动按顺序启动 Registry, NameNode 和 DataNode。

**Linux 用户**:
```bash
cd bin
chmod +x *.sh
./start-all.sh
```

**分步启动**:
你也可以单独启动各个组件（注意启动顺序）：
1. `start-registry` (端口 8000)
2. `start-namenode` (端口 9090)
3. `start-datanode` (端口 8080)

### 4. SDK 使用示例
JNFS 提供了灵活的 Java SDK，支持**单点直连**和**集群发现**两种模式。首先将 `jnfs-driver` 安装到本地 Maven 仓库或引入 Jar 包。

#### 初始化方式

**方式一：直连模式 (简单测试)**
适用于开发测试或 NameNode 单点部署的场景。
```java
// 直接连接指定的 NameNode IP 和端口
JNFSDriver driver = new JNFSDriver("localhost", 9090);
```

**方式二：高可用模式 (推荐生产使用)**
连接注册中心，自动发现可用的 NameNode 集群，支持负载均衡和故障转移。
```java
// 连接 Registry，自动获取 NameNode 列表
JNFSDriver driver = JNFSDriver.useRegistry("localhost", 8000);
```

#### 完整代码示例

```java
import org.jnfs.driver.JNFSDriver;
import java.io.File;

public class Demo {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Driver (此处演示高可用模式)
        JNFSDriver driver = JNFSDriver.useRegistry("localhost", 8000);

        try {
            // 2. 上传文件
            File file = new File("path/to/video.mp4");
            String storageId = driver.uploadFile(file);
            System.out.println("文件上传成功，存储ID: " + storageId);

            // 3. 下载文件
            // 注意：默认下载路径可通过修改 JNFSDriver 源码调整
            File downloadedFile = driver.downloadFile(storageId);
            System.out.println("文件已下载到: " + downloadedFile.getAbsolutePath());
            
        } finally {
            // 4. 关闭连接资源
            driver.close();
        }
    }
}
```

## ⚙️ 配置说明

JNFS 支持外部配置文件加载。在启动 JAR 包的同级目录下放置以下文件可覆盖默认配置：

*   **namenode.yml**: 配置服务端口、注册中心地址、元数据存储方式（支持 MySQL 或本地文件）。
*   **datanode.yml**: 配置服务端口、存储路径（默认为 `datanode_files`）、注册中心地址。
