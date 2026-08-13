# JNFS (Java Network File System)

JNFS 是一个轻量级的、基于 Java Netty 实现的分布式文件系统。它采用类似 HDFS 的 Master-Slave 架构，支持文件上传、下载、秒传去重、流式加密读写、多路径存储与多副本冗余。

## 核心特性

- **高性能通信**：基于 Netty 的自定义二进制协议，支持 NIO 与零拷贝传输，连接池复用。
- **分布式架构**：
  - **Registry**：注册中心，负责节点注册、心跳过期剔除与集群服务发现；**节点注册持久化**（重启不丢节点列表），内置 Web Dashboard。
  - **NameNode**：元数据管理节点，维护文件元数据（文件名、Hash、副本位置），负责选路调度与负载均衡；启动时**等待 Registry 就绪并重试拉取**存储配置。
  - **DataNode**：数据存储节点，负责文件块的读写，支持**多磁盘/多路径挂载**扩容，自动选择剩余空间最大的路径写入。
- **智能传输**：
  - **秒传去重**：基于 SHA-256 文件去重，相同文件不重复上传。
  - **流式传输**：大文件流式读写，防止内存溢出。
  - **边下边解密**：下载时实时流式解密，无需临时文件。
- **数据安全**：
  - Token 服务间鉴权 + AES 客户端加密存储。
  - 密文带 **HMAC 完整性校验**，数据被篡改会自动检出并清理脏文件。
  - 安全加固：路径遍历防御、OOM 攻击防御、协议边界校验、连接池防泄漏。
- **冗余存储与高可用**：
  - 文件级 primary/secondary 多副本，写入并发打副本、primary 必须成功。
  - 冗余组（Replication Group）定义副本策略，夜间对账同步补齐缺失副本。
  - 下载按副本顺序故障转移，单副本损坏自动切换。
- **节点排空（Drain）**：在线节点可标记排空，后续新写入不再选中，已有数据继续可读，为安全下线做准备。
- **双存储模式**：元数据存储支持 **H2 嵌入式库**（单机零依赖，默认，支持同机多磁盘多副本）与 **MySQL**（集群多副本，跨机物理隔离），两种模式平滑切换。
- **Web Dashboard**：基于 Vue 3 + Ant Design Vue 4 的单页应用（侧边栏导航），可视化集群监控 + 登录鉴权 + 冗余存储管理 API（组/策略/任务/排空）。
- **统一配置源**：NameNode 启动时从 Registry 拉取存储配置（AES 加密传输），避免多端重复配置。
- **SDK**：提供 Java 客户端 SDK（同步/异步初始化、直连/集群发现两种模式）。

## 架构

```
                         ┌──────────────────────┐
                         │      Registry        │
                         │  RPC: 5367           │
                         │  Dashboard: 15367    │
                         └───▲──────────▲───────┘
          注册/心跳/发现       │          │        注册/心跳/发现
          存储配置推送         │          │
          ┌───────────────────┤          ├───────────────────┐
          ▼                   │          │                   ▼
┌──────────────────┐          │          │          ┌──────────────────┐
│     NameNode     │◀─────────┘          └─────────▶│     DataNode     │
│  RPC: 5368       │                               │  RPC: 5369       │
│  元数据管理/选路   │                               │  文件块存储/读写   │
└──────────────────┘                               └──────────────────┘
          ▲                                                ▲
          │ 上传/下载/元数据请求                              │ 数据流 (零拷贝)
          │                                                │
┌──────────────────────────────────────────────────────────────────────┐
│                      客户端 (JNFSDriver SDK)                          │
│            直连模式 或 通过 Registry 集群发现                           │
└──────────────────────────────────────────────────────────────────────┘
```

> Dashboard 是一个 Vue SPA，构建期由 Maven 打包进 Registry 的 fat-jar（`classpath:/static/`），由 Registry 内嵌的 `DashboardServer` 静态文件处理器托管，**不单独起前端进程、也无需本机预装 Node**（构建时由 `frontend-maven-plugin` 自备下载）。

## 模块说明

| 模块 | 说明 |
| :--- | :--- |
| `jnfs-common` | 通用组件库：Packet 协议、编解码器、连接池、工具类、数据迁移框架 |
| `jnfs-dashboard-ui` | Dashboard 前端（Vue 3 + Ant Design Vue 4 SPA），无 Java 源码；构建期产出 `dist/` 并合入 Registry fat-jar |
| `jnfs-registry` | 注册中心服务端 + Web Dashboard（节点注册持久化、监控、鉴权、冗余管理 API） |
| `jnfs-namenode` | 元数据管理节点，处理客户端请求，调度 DataNode |
| `jnfs-datanode` | 数据存储节点，处理文件流的实际读写 |
| `jnfs-driver` | 客户端 SDK，供第三方应用集成 JNFS |
| `jnfs-example` | 综合测试工具（源码级运行，不随发布包分发） |
| `jnfs-distribution` | 打包模块，生成包含依赖 jar、配置与启动脚本的发布包 |

## 环境要求

- JDK 17+
- Maven 3.6+
- MySQL 8.0+（仅 `mysql` 存储模式需要）

## 快速开始

### 1. 构建发布包

在项目根目录执行：

```bash
mvn clean package
```

构建成功后，发布包位于 `jnfs-distribution/target/`：

- `jnfs-dist-1.0.0-SNAPSHOT.zip`
- `jnfs-dist-1.0.0-SNAPSHOT.tar.gz`

> Dashboard 前端由 `jnfs-dashboard-ui` 在构建期自动编译（`frontend-maven-plugin` 会自行下载指定版本的 Node/npm 到 `target/node`，不污染宿主环境）。**首次构建需要联网**下载 Node 工具链，后续构建可离线复用。

### 2. 解压后目录结构

```
jnfs-dist-1.0.0-SNAPSHOT/
├── bin/        # 启动/停止脚本 (start.sh/.bat, stop.sh/.bat)
├── conf/       # 各服务配置文件 (registry.yml / namenode.yml / datanode.yml + logback-*.xml)
├── lib/        # 全部依赖 jar
├── logs/       # 运行日志 (按服务名分文件)
└── pids/       # 运行期自动生成，存放各服务 PID 文件
```

### 3. 启动服务

```bash
# 启动全部服务 (registry + namenode + datanode)
./bin/start.sh

# 启动单个服务
./bin/start.sh registry
./bin/start.sh namenode
./bin/start.sh datanode
```

Windows 使用 `start.bat`，停止使用 `stop.sh` / `stop.bat`（支持优雅停止，等待 Shutdown Hook 收尾后强制结束）。

`start.sh` 在启动全部服务时，会从 `conf/registry.yml` 解析 Registry RPC 端口并**等待其就绪**（最长 60s）后再拉起 NameNode / DataNode，避免 Registry 首启初始化 H2 期间连接失败。

| 服务 | 默认端口 | 说明 |
| :--- | :--- | :--- |
| Registry RPC | 5367 | 注册中心 |
| NameNode RPC | 5368 | 元数据管理 |
| DataNode RPC | 5369 | 数据存储 |
| Dashboard HTTP | 15367 | Web 监控页面 |

启动后访问 `http://localhost:15367` 查看 Dashboard（默认账号 `admin/admin`，见配置说明）。

### 4. 存储模式选择

JNFS 元数据存储支持两种模式，由 **Registry 的 `storage.mode`** 统一决定：

| 模式 | 适用场景 | 说明 |
| :--- | :--- | :--- |
| `h2` (默认) | 单机 / 开发测试 | 嵌入式 H2 文件库，零外部依赖，开箱即用；支持冗余组/多副本（同机多磁盘部署）/对账同步/节点排空 |
| `mysql` | 生产集群 | 连接 MySQL，启用冗余组/多副本/对账同步/节点排空；支持 NameNode 多实例 |

> `file` 模式已退役。旧部署若仍配 `mode: file`，启动时会自动映射为 `h2`，并把历史 `namenode_meta.log` 数据迁移进 H2，无需改配置即可平滑升级。

## 配置说明

发布包解压后，编辑 `conf/` 下的配置文件，修改后重启生效。**三个服务端的 `security.token` 与 `security.aes-key` 必须完全一致。**

敏感信息（数据库密码、初始管理员密码）支持用环境变量注入，避免明文落盘：
- `JNFS_REGISTRY_MYSQL_PASSWORD`：Registry 的 MySQL 密码（配置文件 `password` 留空时读取）。
- `JNFS_DASHBOARD_ADMIN_PASSWORD`：Dashboard 初始管理员密码（配置文件 `password` 留空时读取）。

### conf/registry.yml

Registry 是唯一配置源：存储模式、MySQL 连接、Dashboard 鉴权都在这里配置。

```yaml
server:
  port: 5367

# 统一存储配置：一个 mode 同时决定鉴权后端与冗余 API 是否启用
storage:
  mode: h2                        # h2 | mysql
  mysql:                          # 仅 mode: mysql 时生效
    host: 127.0.0.1
    port: 3306
    database: jnfs
    user: jnfs
    password: ""                  # 留空则读环境变量 JNFS_REGISTRY_MYSQL_PASSWORD
  h2:
    path: ""                      # Registry 与 NameNode 共享同一 H2 文件库（混合模式）；留空用默认目录

dashboard:
  port: 15367
  auth:
    enabled: true                 # 是否启用登录鉴权
    initial-admin:
      username: admin
      password: admin             # 首次启动创建，之后建议清空（留空则读 JNFS_DASHBOARD_ADMIN_PASSWORD）
    session:
      timeout-seconds: 7200

heartbeat:
  timeout_ms: 30000               # 心跳超时 (毫秒)，超时剔除节点

security:
  token: "jnfs-secure-token-2025"
  aes-key: "jnfs-aes-key-256bit-secure-key!!"
```

### conf/namenode.yml

```yaml
server:
  port: 5368
  advertised_host: 127.0.0.1      # 对外广播 IP，多网卡/云服务器建议手动指定
  # node_id: "nn-master-01"       # 可选，不配则自动生成并持久化到 node_id.dat

registry:
  addresses: localhost:5367       # 支持逗号分隔的多个地址，自动故障切换

cache:
  enabled: true                   # NameNode 本地内存缓存
  max-size: 100000

security:
  token: "jnfs-secure-token-2025"
  aes-key: "jnfs-aes-key-256bit-secure-key!!"
```

> 存储模式与 MySQL 连接信息**不需要**在 namenode.yml 重复配置 —— NameNode 启动时从 Registry 拉取（AES 加密传输）。Registry 不可达或两端 `aes-key` 不一致时，NameNode 拒绝启动。

### conf/datanode.yml

```yaml
server:
  port: 5369
  advertised_host: 127.0.0.1
  # node_id: "dn-beijing-01"

storage:
  paths:                          # 支持多磁盘挂载点，自动选剩余空间最大的路径写入
    - D:/data/jnfs/storage

registry:
  addresses: localhost:5367

security:
  token: "jnfs-secure-token-2025"
  aes-key: "jnfs-aes-key-256bit-secure-key!!"
```

## 集群部署（MySQL 多副本）

多 NameNode/DataNode 组成生产集群的步骤：

1. **初始化数据库**：将 `mysql/jnfs.sql` 导入 MySQL（含 `schema_version` 表与全部业务表，当前 V6）。
2. **配置 Registry**：在 `conf/registry.yml` 设置 `storage.mode: mysql` 及连接信息。
3. **启动集群**：先启动 Registry，再依次启动各 NameNode / DataNode。
4. **一致性约束**：
   - 所有节点连接同一个 Registry，从 Registry 拉到**同一个** MySQL 库。
   - 所有节点 `security.aes-key` / `token` 一致。
   - 集群部署时**不应使用** `h2` 模式（H2 为单机库，多 NameNode 实例会导致 Brain Split）。`h2` 模式的多副本仅限同一台机器的不同磁盘部署，不提供跨机物理隔离；跨机冗余请用 `mysql` 模式。

MySQL 模式下（`mysql/jnfs.sql` 全量 schema）：

| 表 | 用途 |
| :--- | :--- |
| `schema_version` | schema 版本记录 |
| `node_registry` | 节点注册表（含 `free_space`，Registry 注册持久化 + 离线节点历史展示） |
| `file_metadata` | 文件元数据（storage_id / 文件名 / SHA-256 / 副本数） |
| `file_location` | 文件块存储位置映射（副本角色 primary/secondary + 状态） |
| `file_upload_lock` | 并发上传分布式锁 |
| `replication_group` | 冗余组定义 |
| `replication_policy` | 副本策略（同步窗口/速率/并发） |
| `replica_sync_task` | 对账同步任务 |
| `replication_control` | 手动同步触发控制 |
| `node_drain` | 节点排空状态（跨重启） |
| `dashboard_user` | Dashboard 鉴权用户 |

## SDK 使用示例

### 直连模式（简单测试）

```java
JNFSDriver driver = new JNFSDriver("localhost", 5368);
```

### 高可用模式（推荐生产）

连接 Registry，自动发现 NameNode 集群，支持负载均衡与故障转移：

```java
// 单 Registry
JNFSDriver driver = JNFSDriver.useRegistry("localhost:5367");

// 多 Registry 高可用（逗号分隔）
JNFSDriver driver = JNFSDriver.useRegistry("192.168.1.10:5367,192.168.1.11:5367");
```

### 完整示例

```java
import org.jnfs.driver.JNFSDriver;
import java.io.File;

public class Demo {
    public static void main(String[] args) throws Exception {
        // 直连单点 或 useRegistry(...) 集群发现
        JNFSDriver driver = new JNFSDriver("localhost", 5368);
        try {
            // 上传 (支持 File / byte[] / InputStream 三种入参)
            // 相同内容的文件再次上传会命中秒传，返回同一 storageId
            File file = new File("path/to/video.mp4");
            String storageId = driver.uploadFile(file);

            // 下载 (自动使用原文件名，边下边解密、HMAC 校验)
            File downloaded = driver.downloadFile(storageId, "downloads/");
            System.out.println("已下载到: " + downloaded.getAbsolutePath());
        } finally {
            driver.close();
        }
    }
}
```

### 连接状态检查

```java
// 同步初始化，返回连接状态
ConnectionStatus status = driver.initialize();
if (status.isOk()) { /* 连接成功（SUCCESS / PARTIAL_SUCCESS 均视为可用） */ }

// 或异步初始化
driver.initialize(cs -> System.out.println("初始化完成: " + cs.getState()));

// 随时获取最近一次连接状态
ConnectionStatus current = driver.getConnectionStatus();
```

连接状态取值：`SUCCESS`（全部正常）/ `PARTIAL_SUCCESS`（部分 Registry 不可达但已发现 NameNode）/ `REGISTRY_UNREACHABLE` / `NO_NAMENODE` / `TOKEN_INVALID` / `TIMEOUT`。

## 冗余存储与节点排空

### 冗余组与多副本

- 文件以 `file_hash` 为粒度持有 **primary/secondary** 副本角色（文件级，非节点级）。
- 写入时 Driver 并发向目标节点写密文，**primary 必须成功**，部分成功即提交，其余留给夜间对账补齐。
- 下载时按副本顺序故障转移，单副本失败自动切换、HMAC 失败也切换。

### 对账同步（夜间）

`ReplicaSyncScheduler` 定期扫描缺失副本并对账补齐，任务持久化到 `replica_sync_task`，启动时恢复未完成任务。亦可通过 Dashboard 或 `POST /api/replication/sync` 手动触发。

### 节点排空（Drain）

- 通过 Dashboard 或 `POST /api/nodes/{nodeId}/drain`（body `{"drain": true}`）标记节点排空（仅限在线且属于冗余组的节点）。
- 排空状态持久化到 `node_drain` 表，**NameNode 下次启动加载排空列表后**，该节点不再作为新文件写入目标，但已有数据继续可读；物理数据由同步任务在真正下线前搬运。
- `{"drain": false}` 可解除排空。组级分段锁避免排空后组内无可用节点。
- 需要在线提升某副本为主时，可用 `POST /api/nodes/{nodeId}/promote`（自动把旧 primary 降为 secondary，保证单 primary 不变式）。

## 文件管理（Dashboard）

Dashboard「文件管理」页可视化查询已上传文件：分页列表 + **存储节点 / 文件类型 / 存储编号**筛选，展示文件大小、类型、副本所在节点（主/备角色与损坏状态）、创建时间、存储 ID（首列）与哈希（可复制）。数据来源为与 NameNode 共享的元数据库（h2 / mysql 同库直查），不经过 NameNode RPC，对存储/下载链路零影响。

- API：`GET /api/files?page=&pageSize=&nodeId=&fileType=&storageId=`（服务端分页；`storageId` 为存储编号包含匹配）、`GET /api/files/types`（类型下拉候选，`unknown` 恒存在并置顶）。
- **文件类型识别（两级，不影响上传/下载性能）**：
  1. 上传提交时按文件名扩展名即时记录 `file_metadata.file_type`（微秒级纯函数）；
  2. 后台 `FileTypeDetectScheduler`（daemon、每 10s 一批 20 个、空闲退避）对 `file_type IS NULL`（无扩展名/扩展名不可靠）的文件，向 DataNode 读取**解密后的文件头 ≤8KB**（`DATA_HEAD_READ` 指令），用 Tika 内容嗅探兜底并回写；同时回填存量文件的 `file_size`（历史数据大小未知，记为 NULL，展示为"未知"）。
- `file` 模式（已退役）下文件 API 返回 503，页面展示空态提示。

## 综合测试工具（ExampleApp）

`jnfs-example` 模块提供交互式综合测试控制台（**源码级运行，不随发布包分发**），在 IDE 中运行 `org.jnfs.example.ExampleApp` 即可。启动时可选择连接方式：**注册中心（Registry，默认 5367）** 或 **直连 NameNode（默认 5368）**。包含 7 类测试：

1. 标准上传/下载测试
2. 连接池并发测试
3. 路径遍历漏洞测试
4. 资源泄漏测试
5. NameNode 分段锁并发测试
6. DataNode 重命名原子性模拟
7. 批量暴力测试（高并发上传下载 + SHA-256 完整性校验 + 大文件 OOM 验证 + 协议边界注入）

## 数据迁移与升级

JNFS 内置版本化迁移框架（`MigrationRunner`，当前 `CURRENT_VERSION = 7`），支持 `file → h2`、`file → mysql`、以及 schema 版本递增迁移：

- 迁移步骤位于 `jnfs-namenode/.../migration/`，注册于 `META-INF/migrations/`。
- JDBC 步骤经方言路由同时覆盖 `h2` / `mysql`（如 `JdbcV5ToV6` 为 `node_registry` 增加 `free_space` 列；`JdbcV6ToV7` 为 `file_metadata` 增加 `file_type` 列并回填存量扩展名类型、将历史 `file_size=0` 归一为 NULL）。
- 迁移失败拒绝启动（`System.exit(2)`），保证新旧数据不混跑。
- `mysql/jnfs.sql` 始终保持最新完整 schema（含 `schema_version` 表）。

## 常见问题

- **NameNode 启动报"拉取 storage 配置失败"**：Registry 未启动、未就绪（首启初始化 H2 较慢），或两端 `security.aes-key` 不一致。`start.sh` 已默认等待 Registry 就绪。
- **Dashboard 显示"同机部署"提示**：`storage.mode` 为 `h2`，冗余可用但副本仅限同机不同磁盘；跨机冗余请切到 `mysql`。
- **修改配置后不生效**：配置从运行目录 `conf/` 优先加载（classpath 内的为默认值），确认修改的是发布包 `conf/` 下的文件并已重启。
- **首次构建卡在下载 Node**：`jnfs-dashboard-ui` 需联网下载 Node 工具链；网络受限时可预先缓存 `target/node` 或改用离线镜像。
