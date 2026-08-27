package org.jnfs.common;

/**
 * 命令类型枚举
 * 定义了系统支持的所有操作指令
 */
public enum CommandType {
    /** 上传请求 (To DataNode) */
    UPLOAD_REQUEST((byte) 1),
    /** 上传响应 (From DataNode) */
    UPLOAD_RESPONSE((byte) 2),
    /** 下载请求 (To DataNode) */
    DOWNLOAD_REQUEST((byte) 3),
    /** 下载响应 (From DataNode - 文件元数据) */
    DOWNLOAD_RESPONSE((byte) 4),
    
    /** NameNode: 请求上传位置 */
    NAMENODE_REQUEST_UPLOAD_LOC((byte) 10),
    /** NameNode: 响应上传位置 */
    NAMENODE_RESPONSE_UPLOAD_LOC((byte) 11),
    /** NameNode: 提交文件元数据 (上传完成后) */
    NAMENODE_COMMIT_FILE((byte) 12),
    /** NameNode: 响应提交结果 */
    NAMENODE_RESPONSE_COMMIT((byte) 13),
    /** NameNode: 请求下载位置 (By Storage ID) */
    NAMENODE_REQUEST_DOWNLOAD_LOC((byte) 14),
    /** NameNode: 响应下载位置 (包含文件名、Hash、DataNode地址) */
    NAMENODE_RESPONSE_DOWNLOAD_LOC((byte) 15),
    
    /** NameNode: 检查文件是否存在 (秒传检查) */
    NAMENODE_CHECK_EXISTENCE((byte) 20),
    /** NameNode: 响应文件存在 (支持秒传) */
    NAMENODE_RESPONSE_EXIST((byte) 21),
    /** NameNode: 响应文件不存在 (需要上传) */
    NAMENODE_RESPONSE_NOT_EXIST((byte) 22),
    
    /** NameNode: 预上传申请 (并发控制) */
    NAMENODE_PRE_UPLOAD((byte) 23),
    /** NameNode: 允许上传 */
    NAMENODE_RESPONSE_ALLOW((byte) 24),
    /** NameNode: 正在上传中 (客户端需等待) */
    NAMENODE_RESPONSE_WAIT((byte) 25),
    
    // --- 注册中心相关指令 ---
    /** 服务注册 (To Registry) */
    REGISTRY_REGISTER((byte) 30),
    /** 服务注册响应 */
    REGISTRY_RESPONSE_REGISTER((byte) 31),
    /** 心跳 (To Registry) */
    REGISTRY_HEARTBEAT((byte) 32),
    /** 获取 DataNode 列表 (To Registry) */
    REGISTRY_GET_DATANODES((byte) 33),
    /** 响应 DataNode 列表 */
    REGISTRY_RESPONSE_DATANODES((byte) 34),
    
    /** NameNode 注册 (To Registry) */
    REGISTRY_REGISTER_NAMENODE((byte) 35),
    /** NameNode 注册响应 */
    REGISTRY_RESPONSE_REGISTER_NAMENODE((byte) 36),
    /** 获取 NameNode 列表 (To Registry) */
    REGISTRY_GET_NAMENODES((byte) 37),
    /** 响应 NameNode 列表 */
    REGISTRY_RESPONSE_NAMENODES((byte) 38),
    /** NameNode 心跳 */
    REGISTRY_HEARTBEAT_NAMENODE((byte) 39),

    // --- 副本拉取相关指令 ---
    /** 副本拉取请求 (target -> source: 我要拉 hash H) */
    DATA_REPLICA_PULL_REQUEST((byte) 40),
    /** 副本拉取响应 (source -> target: 返回文件长度) */
    DATA_REPLICA_PULL_RESPONSE((byte) 41),
    /** 副本数据块 (source -> target: 密文流，复用现有 ByteBuf 流式) */
    DATA_REPLICA_CHUNK((byte) 42),
    /** 副本提交 (target -> NameNode: 我已持有 H，登记 ACTIVE) */
    DATA_REPLICA_COMMIT((byte) 43),
    /** 副本拉取触发指令 (NameNode -> target DataNode: 协调目标节点开始从 source 拉取 hash H) */
    DATA_REPLICA_PULL_CMD((byte) 44),

    /** NameNode -> Registry: 启动时拉取存储模式与 MySQL 连接配置 */
    REGISTRY_GET_STORAGE_CONFIG((byte) 45),
    /** Registry -> NameNode: 响应存储配置（密文，AES-256-CTR + HMAC） */
    REGISTRY_RESPONSE_STORAGE_CONFIG((byte) 46),

    // --- 文件头读取相关指令（后台文件类型嗅探用，不影响上传/下载主链路） ---
    /** 文件头读取请求 (NameNode -> DataNode: 读文件解密后头部, 用于类型嗅探) */
    DATA_HEAD_READ_REQUEST((byte) 47),
    /** 文件头读取响应 (DataNode -> NameNode: [8B逻辑长度][明文头部≤8KB]) */
    DATA_HEAD_READ_RESPONSE((byte) 48),

    /** 错误消息 */
    ERROR((byte) -1);

    private final byte value;

    CommandType(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    /**
     * 根据字节值获取对应的命令类型
     * @param value 字节值
     * @return 对应的CommandType，如果未找到则返回ERROR
     */
    public static CommandType fromByte(byte value) {
        for (CommandType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        return ERROR;
    }
}
