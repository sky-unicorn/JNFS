package org.jnfs.common;

/**
 * 通信协议包
 * 定义了客户端与服务端交互的基本数据结构
 */
public class Packet {
    /** 协议版本号 */
    private byte version = 1;
    /** 命令类型 */
    private CommandType commandType;
    /** 安全令牌 */
    private String token;
    /** 数据体 */
    private byte[] data;

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
