package org.jnfs.comm;// FileTransferMessage.java
import java.io.Serializable;

public class FileTransferMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    // 消息类型
    public enum Type {
        FILE_INFO,    // 文件信息
        FILE_DATA,    // 文件数据
        TRANSFER_COMPLETE, // 传输完成
        RESPONSE      // 响应消息
    }

    private Type type;
    private String fileName;
    private long fileSize;
    private byte[] data;
    private int dataLength;
    private String message;
    private boolean success;

    // getter和setter方法
    public Type getType() { return type; }
    public void setType(Type type) { this.type = type; }
    public String getFileName() { return fileName; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public long getFileSize() { return fileSize; }
    public void setFileSize(long fileSize) { this.fileSize = fileSize; }
    public byte[] getData() { return data; }
    public void setData(byte[] data) { this.data = data; }
    public int getDataLength() { return dataLength; }
    public void setDataLength(int dataLength) { this.dataLength = dataLength; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
}
