package org.jnfs.datanode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * DataNode 业务处理器 (原 ServerHandler)
 * 处理文件上传和下载的数据流
 */
public class DataNodeHandler extends SimpleChannelInboundHandler<Object> {

    private static final String STORAGE_PATH = "datanode_files";
    
    // 当前正在接收的文件写入通道
    private FileChannel currentFileChannel;
    // 当前文件输出流
    private FileOutputStream currentFos;
    // 当前文件名
    private String currentFileName;
    // 当前文件总大小
    private long currentFileSize;
    // 已接收字节数
    private long receivedBytes;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("客户端(或Driver)已连接: " + ctx.channel().remoteAddress());
        File storageDir = new File(STORAGE_PATH);
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Packet) {
            handlePacket(ctx, (Packet) msg);
        } else if (msg instanceof ByteBuf) {
            handleFileChunk(ctx, (ByteBuf) msg);
        }
    }

    private void handlePacket(ChannelHandlerContext ctx, Packet packet) {
        System.out.println("收到指令: " + packet.getCommandType());
        if (packet.getCommandType() == CommandType.UPLOAD_REQUEST) {
            initiateUpload(ctx, packet);
        } else if (packet.getCommandType() == CommandType.DOWNLOAD_REQUEST) {
            // TODO: 实现下载逻辑
            sendResponse(ctx, CommandType.ERROR, "暂未实现下载".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void initiateUpload(ChannelHandlerContext ctx, Packet packet) {
        byte[] data = packet.getData();
        if (data == null || data.length < 8) {
            sendResponse(ctx, CommandType.ERROR, "无效的元数据".getBytes(StandardCharsets.UTF_8));
            return;
        }

        long fileSize = 0;
        for (int i = 0; i < 8; i++) {
            fileSize = (fileSize << 8) | (data[i] & 0xFF);
        }
        
        String fileName = new String(data, 8, data.length - 8, StandardCharsets.UTF_8);
        
        System.out.println("准备接收文件: " + fileName + ", 大小: " + fileSize + " 字节");
        
        try {
            if (currentFos != null) {
                currentFos.close();
            }
            File file = new File(STORAGE_PATH, fileName);
            currentFos = new FileOutputStream(file);
            currentFileChannel = currentFos.getChannel();
            currentFileName = fileName;
            currentFileSize = fileSize;
            receivedBytes = 0;
            
            if (fileSize == 0) {
                finishUpload(ctx);
            }
        } catch (IOException e) {
            e.printStackTrace();
            sendResponse(ctx, CommandType.ERROR, ("服务端错误: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleFileChunk(ChannelHandlerContext ctx, ByteBuf chunk) {
        if (currentFileChannel == null) {
            return;
        }
        
        try {
            int readable = chunk.readableBytes();
            chunk.readBytes(currentFileChannel, receivedBytes, readable);
            receivedBytes += readable;
            
            if (receivedBytes >= currentFileSize) {
                finishUpload(ctx);
            }
        } catch (IOException e) {
            e.printStackTrace();
            closeCurrentFile();
            sendResponse(ctx, CommandType.ERROR, ("写入错误: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void finishUpload(ChannelHandlerContext ctx) {
        closeCurrentFile();
        System.out.println("文件存储完成: " + currentFileName);
        sendResponse(ctx, CommandType.UPLOAD_RESPONSE, ("上传成功: " + currentFileName).getBytes(StandardCharsets.UTF_8));
        
        // 重置状态
        currentFileName = null;
        currentFileSize = 0;
        receivedBytes = 0;
    }

    private void closeCurrentFile() {
        try {
            if (currentFileChannel != null) {
                currentFileChannel.close();
            }
            if (currentFos != null) {
                currentFos.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        currentFileChannel = null;
        currentFos = null;
    }

    private void sendResponse(ChannelHandlerContext ctx, CommandType type, byte[] data) {
        Packet response = new Packet();
        response.setCommandType(type);
        response.setData(data);
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeCurrentFile();
        ctx.close();
    }
}
