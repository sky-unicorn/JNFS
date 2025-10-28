package org.jnfs.cli;// FileClientHandler.java
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.jnfs.comm.FileTransferMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class FileClientHandler extends ChannelInboundHandlerAdapter {
    private final String filePath;
    private File file;
    private FileInputStream fis;
    private long fileSize;
    private long sentSize;
    private static final int BUFFER_SIZE = 1024 * 8; // 8KB缓冲区

    public FileClientHandler(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            System.err.println("文件不存在或不是一个文件: " + filePath);
            ctx.close();
            return;
        }

        fileSize = file.length();
        fis = new FileInputStream(file);
        sentSize = 0;

        // 发送文件信息
        FileTransferMessage fileInfo = new FileTransferMessage();
        fileInfo.setType(FileTransferMessage.Type.FILE_INFO);
        fileInfo.setFileName(file.getName());
        fileInfo.setFileSize(fileSize);
        ctx.writeAndFlush(fileInfo);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FileTransferMessage message &&
            message.getType() == FileTransferMessage.Type.RESPONSE) {

            if (message.isSuccess()) {
                System.out.println("服务器响应: " + message.getMessage());

                // 如果是准备接收的响应，则开始发送文件数据
                if (message.getMessage().contains("准备接收文件数据")) {
                    sendFileData(ctx);
                } else {
                    // 其他响应，如上传完成
                    System.out.println("文件上传" + (message.isSuccess() ? "成功" : "失败") + ": " + message.getMessage());
                    ctx.close();
                }
            } else {
                System.err.println("服务器错误: " + message.getMessage());
                ctx.close();
            }
        }
    }

    private void sendFileData(ChannelHandlerContext ctx) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;

        while ((bytesRead = fis.read(buffer)) != -1) {
            FileTransferMessage dataMsg = new FileTransferMessage();
            dataMsg.setType(FileTransferMessage.Type.FILE_DATA);
            dataMsg.setData(buffer);
            dataMsg.setDataLength(bytesRead);

            ctx.writeAndFlush(dataMsg);
            sentSize += bytesRead;

            // 打印进度
            if (fileSize > 0) {
                int progress = (int) ((sentSize * 100) / fileSize);
                System.out.printf("发送进度: %d%%\r", progress);

            }
        }

        // 发送完成消息
        FileTransferMessage completeMsg = new FileTransferMessage();
        completeMsg.setType(FileTransferMessage.Type.TRANSFER_COMPLETE);
        ctx.writeAndFlush(completeMsg);

        System.out.println("\n文件数据发送完毕");
        fis.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (fis != null) {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ctx.close();
    }
}
