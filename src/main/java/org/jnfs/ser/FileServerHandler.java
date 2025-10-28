package org.jnfs.ser;// FileServerHandler.java
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.jnfs.comm.FileTransferConstants;
import org.jnfs.comm.FileTransferMessage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileServerHandler extends ChannelInboundHandlerAdapter {
    private String fileName;
    private long fileSize;
    private long receivedSize;
    private FileOutputStream fos;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FileTransferMessage message) {
            switch (message.getType()) {
                case FILE_INFO:
                    handleFileInfo(message, ctx);
                    break;
                case FILE_DATA:
                    handleFileData(message, ctx);
                    break;
                case TRANSFER_COMPLETE:
                    handleTransferComplete(ctx);
                    break;
                default:
                    super.channelRead(ctx, msg);
            }
        }
    }

    private void handleFileInfo(FileTransferMessage message, ChannelHandlerContext ctx) throws IOException {
        this.fileName = message.getFileName();
        this.fileSize = message.getFileSize();
        this.receivedSize = 0;

        File file = new File(FileTransferConstants.UPLOAD_DIR + fileName);
        // 如果文件已存在，删除旧文件
        if (file.exists()) {
            file.delete();
        }

        this.fos = new FileOutputStream(file);
        System.out.println("开始接收文件: " + fileName + "，大小: " + fileSize + " bytes");

        // 发送响应
        sendResponse(ctx, true, "准备接收文件数据");
    }

    private void handleFileData(FileTransferMessage message, ChannelHandlerContext ctx) throws IOException {
        if (fos == null) {
            sendResponse(ctx, false, "未收到文件信息");
            return;
        }

        byte[] data = message.getData();
        int dataLength = message.getDataLength();

        fos.write(data, 0, dataLength);
        receivedSize += dataLength;

        // 打印进度
        if (fileSize > 0) {
            int progress = (int) ((receivedSize * 100) / fileSize);
            System.out.printf("接收进度: %d%%\r", progress);
        }
    }

    private void handleTransferComplete(ChannelHandlerContext ctx) throws IOException {
        if (fos != null) {
            fos.close();
        }

        System.out.println("\n文件接收完成: " + fileName);
        System.out.println("实际接收大小: " + receivedSize + " bytes");

        // 验证文件大小
        boolean success = receivedSize == fileSize;
        String message = success ? "文件上传成功" : "文件上传不完整";

        sendResponse(ctx, success, message);

        // 关闭连接
        ctx.close();
    }

    private void sendResponse(ChannelHandlerContext ctx, boolean success, String message) {
        FileTransferMessage response = new FileTransferMessage();
        response.setType(FileTransferMessage.Type.RESPONSE);
        response.setSuccess(success);
        response.setMessage(message);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (fos != null) {
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ctx.close();
    }
}
