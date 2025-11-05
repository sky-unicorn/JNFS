package org.jnfs.ser;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class FileUploadServer {
    public static void main(String[] args) throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 1024)
             .childOption(ChannelOption.SO_KEEPALIVE, true)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                     ch.pipeline().addLast(new FileUploadDecoder());
                     ch.pipeline().addLast(new FileUploadServerHandler());
                 }
             });

            ChannelFuture f = b.bind(8080).sync();
            System.out.println("文件服务器启动，端口：8080");
            System.out.println("文件保存目录：C:\\Users\\liuti\\Desktop\\test\\");
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}

/**
 * 自定义协议解码器
 * 协议格式：[1字节类型][4字节长度][数据内容]
 * 类型：1=文件名（上传），2=文件内容（上传），3=文件结束（上传），4=下载请求，5=下载文件内容（服务端发送），6=下载结束（服务端发送）
 */
class FileUploadDecoder extends ByteToMessageDecoder {
    private enum State {
        READ_HEADER,
        READ_DATA
    }

    private State state = State.READ_HEADER;
    private byte dataType;
    private int dataLength;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state) {
            case READ_HEADER:
                if (in.readableBytes() < 5) { // 需要1字节类型 + 4字节长度
                    return;
                }

                dataType = in.readByte();
                dataLength = in.readInt();
                state = State.READ_DATA;
                // 继续处理，不break

            case READ_DATA:
                if (in.readableBytes() < dataLength) {
                    return;
                }

                ByteBuf data = in.readBytes(dataLength);

                if (dataType == 1) { // 文件名（上传）
                    String fileName = data.toString(CharsetUtil.UTF_8);
                    out.add(new FileNameMessage(fileName));
                } else if (dataType == 2) { // 文件内容（上传）
                    out.add(new FileContentMessage(data));
                } else if (dataType == 3) { // 文件结束（上传）
                    out.add(new FileEndMessage());
                } else if (dataType == 4) { // 下载请求
                    String downloadFileName = data.toString(CharsetUtil.UTF_8);
                    out.add(new DownloadRequestMessage(downloadFileName));
                }
                // 注意：类型5和6是服务端发送给客户端的，客户端需要解码，但服务端不会收到，所以这里不需要处理

                state = State.READ_HEADER;
                break;
        }
    }
}

// 自定义消息类型
class FileNameMessage {
    public final String fileName;

    public FileNameMessage(String fileName) {
        this.fileName = fileName;
    }
}

class FileContentMessage {
    public final ByteBuf content;

    public FileContentMessage(ByteBuf content) {
        this.content = content;
    }
}

class FileEndMessage {
    // 文件结束标记，无数据
}

class DownloadRequestMessage {
    public final String fileName;

    public DownloadRequestMessage(String fileName) {
        this.fileName = fileName;
    }
}

class FileUploadServerHandler extends ChannelInboundHandlerAdapter {
    private FileChannel outChannel; // 用于上传的文件通道
    private long totalBytesReceived = 0; // 上传接收的字节数
    private String fileName; // 上传的文件名
    private String savePath = "C:\\Users\\liuti\\Desktop\\test\\";

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        System.out.println("客户端连接: " + ctx.channel().remoteAddress());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FileNameMessage) {
            // 接收上传的文件名
            FileNameMessage fileNameMsg = (FileNameMessage) msg;
            this.fileName = fileNameMsg.fileName;

            try {
                // 创建文件目录（如果不存在）
                File dir = new File(savePath);
                dir.mkdirs();

                // 使用原始文件名保存
                String fullPath = savePath + fileName;

                // 使用FileChannel实现高效写入
                outChannel = FileChannel.open(
                    Paths.get(fullPath),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
                );

                System.out.println("开始接收文件: " + fileName);
                System.out.println("保存路径: " + fullPath);

            } catch (IOException e) {
                System.err.println("创建文件失败: " + e.getMessage());
                ctx.close();
            }
        } else if (msg instanceof FileContentMessage) {
            // 接收上传的文件内容
            FileContentMessage contentMsg = (FileContentMessage) msg;
            ByteBuf buf = contentMsg.content;

            try {
                int bytesToWrite = buf.readableBytes();
                if (bytesToWrite > 0 && outChannel != null) {
                    // 使用nioBuffer实现零拷贝写入
                    ByteBuffer nioBuffer = buf.nioBuffer();
                    while (nioBuffer.hasRemaining()) {
                        outChannel.write(nioBuffer);
                    }
                    totalBytesReceived += bytesToWrite;

                    // 每10MB输出一次进度
                    if (totalBytesReceived % (10 * 1024 * 1024) == 0) {
                        System.out.println("已接收: " + (totalBytesReceived / (1024 * 1024)) + "MB");
                    }
                }
            } catch (IOException e) {
                System.err.println("文件写入失败: " + e.getMessage());
                ctx.close();
            } finally {
                buf.release(); // 必须释放缓冲区
            }
        } else if (msg instanceof FileEndMessage) {
            // 上传文件传输完成
            System.out.println("文件接收完成: " + fileName);
            System.out.println("文件大小: " + (totalBytesReceived / (1024 * 1024)) + "MB");

            try {
                if (outChannel != null) {
                    outChannel.force(true);
                    outChannel.close();
                    outChannel = null;
                }
            } catch (IOException e) {
                System.err.println("关闭文件失败: " + e.getMessage());
            }

            // 重置状态，以便处理下一个文件
            totalBytesReceived = 0;
            fileName = null;
        } else if (msg instanceof DownloadRequestMessage) {
            // 处理下载请求
            DownloadRequestMessage downloadMsg = (DownloadRequestMessage) msg;
            String downloadFileName = downloadMsg.fileName;
            System.out.println("收到下载请求，文件名: " + downloadFileName);

            String fullPath = savePath + downloadFileName;
            File file = new File(fullPath);
            if (!file.exists() || !file.isFile()) {
                System.err.println("文件不存在: " + fullPath);
                // 可以发送一个错误消息给客户端，这里简单关闭连接
                ctx.close();
                return;
            }

            // 启动文件下载
            sendFile(ctx, file);
        }
    }

    private void sendFile(ChannelHandlerContext ctx, File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             FileChannel fileChannel = raf.getChannel()) {

            long fileSize = fileChannel.size();
            long bytesSent = 0;
            int chunkSize = 64 * 1024; // 64KB每块

            System.out.println("开始发送文件: " + file.getName());

            while (bytesSent < fileSize) {
                long remaining = fileSize - bytesSent;
                int sendSize = (int) Math.min(chunkSize, remaining);

                ByteBuffer buffer = ByteBuffer.allocate(sendSize);
                int bytesRead = fileChannel.read(buffer);
                if (bytesRead <= 0) {
                    break;
                }
                buffer.flip();

                // 构建消息：类型5（下载文件内容）
                ByteBuf message = Unpooled.buffer(5 + bytesRead);
                message.writeByte(5); // 类型5=下载文件内容
                message.writeInt(bytesRead); // 内容长度
                message.writeBytes(buffer); // 内容数据

                ctx.write(message); // 注意：这里使用write，不是writeAndFlush，为了最后一起flush
                bytesSent += bytesRead;

                // 每10MB输出一次进度
                if (bytesSent % (10 * 1024 * 1024) == 0) {
                    System.out.println("已发送: " + (bytesSent / (1024 * 1024)) + "MB");
                }
            }

            // 发送文件结束标记：类型6
            ByteBuf endMsg = Unpooled.buffer(5);
            endMsg.writeByte(6); // 类型6=下载结束
            endMsg.writeInt(0); // 无数据内容
            ctx.write(endMsg);

            // 一次性flush
            ctx.flush();
            System.out.println("文件发送完成: " + file.getName());

        } catch (Exception e) {
            System.err.println("发送文件失败: " + e.getMessage());
            e.printStackTrace();
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        try {
            if (outChannel != null) {
                outChannel.force(true);
                outChannel.close();
                System.out.println("连接关闭，文件保存完成: " + fileName);
            }
        } catch (IOException e) {
            System.err.println("关闭文件失败: " + e.getMessage());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("传输异常: " + cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}
