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
    /** 下载响应 (From DataNode) */
    DOWNLOAD_RESPONSE((byte) 4),
    
    /** NameNode: 请求上传位置 */
    NAMENODE_REQUEST_UPLOAD_LOC((byte) 10),
    /** NameNode: 响应上传位置 */
    NAMENODE_RESPONSE_UPLOAD_LOC((byte) 11),
    /** NameNode: 提交文件元数据 (上传完成后) */
    NAMENODE_COMMIT_FILE((byte) 12),
    /** NameNode: 响应提交结果 */
    NAMENODE_RESPONSE_COMMIT((byte) 13),
    /** NameNode: 请求下载位置 */
    NAMENODE_REQUEST_DOWNLOAD_LOC((byte) 14),
    /** NameNode: 响应下载位置 */
    NAMENODE_RESPONSE_DOWNLOAD_LOC((byte) 15),

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
