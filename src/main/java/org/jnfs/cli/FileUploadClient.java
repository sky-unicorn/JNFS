package org.jnfs.cli;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class FileUploadClient {
    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.SO_KEEPALIVE, true).handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new FileUploadClientDecoder());
                    ch.pipeline().addLast(new FileUploadClientHandler());
                }
            });

            ChannelFuture f = b.connect("localhost", 8080).sync();
            System.out.println("连接到文件服务器，开始上传文件...");
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}

/**
 * 客户端自定义协议解码器
 * 协议格式：[1字节类型][4字节长度][数据内容]
 * 类型：5=下载文件内容，6=下载结束
 */
class FileUploadClientDecoder extends ByteToMessageDecoder {
    private enum State {
        READ_HEADER, READ_DATA
    }

    private State state = State.READ_HEADER;
    private byte dataType;
    private int dataLength;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state) {
            case READ_HEADER:
                if (in.readableBytes() < 5) {
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

                if (dataType == 5) { // 下载文件内容
                    out.add(new DownloadContentMessage(data));
                } else if (dataType == 6) { // 下载结束
                    out.add(new DownloadEndMessage());
                }
                // 注意：类型1-4是客户端发送给服务端的，服务端需要解码，但客户端不会收到，所以这里不需要处理

                state = State.READ_HEADER;
                break;
        }
    }
}

// 客户端自定义消息类型
class DownloadContentMessage {
    public final ByteBuf content;

    public DownloadContentMessage(ByteBuf content) {
        this.content = content;
    }
}

class DownloadEndMessage {
    // 下载结束标记
}

class FileUploadClientHandler extends ChannelInboundHandlerAdapter {
    private String filePath = "E:\\back-up\\backup.7z";
    private RandomAccessFile raf;
    private FileChannel fileChannel;
    private long fileSize;
    private long bytesSent = 0;
    private static final int CHUNK_SIZE = 64 * 1024; // 64KB每块

    // 下载相关
    private FileChannel downloadFileChannel;
    private String downloadFileName = "C:\\Users\\liuti\\Desktop\\test\\dl\\backup.7z"; // 下载保存的文件名
    private long downloadBytesReceived = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // 示例：可以选择上传或下载
        // 上传文件
        // uploadFile(ctx);

        // 下载文件
        downloadFile(ctx, "backup.7z"); // 假设要下载的文件名为example.txt
    }

    /**
     * 上传文件
     */
    private void uploadFile(ChannelHandlerContext ctx) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                System.err.println("文件不存在: " + filePath);
                ctx.close();
                return;
            }

            String fileName = file.getName();
            fileSize = file.length();
            raf = new RandomAccessFile(filePath, "r");
            fileChannel = raf.getChannel();

            System.out.println("开始上传文件: " + fileName);
            System.out.println("文件大小: " + (fileSize / (1024 * 1024)) + "MB");

            // 1. 先发送文件名
            sendFileName(ctx, fileName);

        } catch (Exception e) {
            System.err.println("准备文件失败: " + e.getMessage());
            e.printStackTrace();
            ctx.close();
        }
    }

    /**
     * 下载文件
     *
     * @param fileName 要下载的文件名
     */
    private void downloadFile(ChannelHandlerContext ctx, String fileName) {
        try {
            // 创建下载保存的文件
            File downloadFile = new File(downloadFileName);
            raf = new RandomAccessFile(downloadFile, "rw");
            downloadFileChannel = raf.getChannel();
            System.out.println("开始下载文件: " + fileName);

            // 发送下载请求：类型4
            sendDownloadRequest(ctx, fileName);
        } catch (Exception e) {
            System.err.println("准备下载失败: " + e.getMessage());
            e.printStackTrace();
            ctx.close();
        }
    }

    private void sendFileName(ChannelHandlerContext ctx, String fileName) {
        try {
            byte[] fileNameBytes = fileName.getBytes(CharsetUtil.UTF_8);
            ByteBuf buffer = Unpooled.buffer(5 + fileNameBytes.length);

            // 协议格式：[1字节类型][4字节长度][数据内容]
            buffer.writeByte(1); // 类型1=文件名
            buffer.writeInt(fileNameBytes.length); // 文件名长度
            buffer.writeBytes(fileNameBytes); // 文件名内容

            ctx.writeAndFlush(buffer).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        System.out.println("文件名发送成功，开始发送文件内容...");
                        // 2.发送文件内容
                        sendFileContent(ctx);
                    } else {
                        System.err.println("发送文件名失败: " + future.cause().getMessage());
                        ctx.close();
                    }
                }
            });

        } catch (Exception e) {
            System.err.println("发送文件名失败: " + e.getMessage());
            e.printStackTrace();
            ctx.close();
        }
    }

    private void sendDownloadRequest(ChannelHandlerContext ctx, String fileName) {
        try {
            byte[] fileNameBytes = fileName.getBytes(CharsetUtil.UTF_8);
            ByteBuf buffer = Unpooled.buffer(5 + fileNameBytes.length);

            buffer.writeByte(4); // 类型4=下载请求
            buffer.writeInt(fileNameBytes.length); // 文件名长度
            buffer.writeBytes(fileNameBytes); // 文件名内容

            ctx.writeAndFlush(buffer).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        System.out.println("下载请求发送成功，等待文件内容...");
                    } else {
                        System.err.println("发送下载请求失败: " + future.cause().getMessage());
                        ctx.close();
                    }
                }
            });

        } catch (Exception e) {
            System.err.println("发送下载请求失败: " + e.getMessage());
            e.printStackTrace();
            ctx.close();
        }
    }

    private void sendFileContent(ChannelHandlerContext ctx) {
        if (bytesSent >= fileSize) {
            // 文件传输完成，发送结束标记
            sendFileEnd(ctx);
            return;
        }

        try {
            long remaining = fileSize - bytesSent;
            int chunkSize = (int) Math.min(CHUNK_SIZE, remaining);

            ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
            int bytesRead = fileChannel.read(buffer);

            if (bytesRead > 0) {
                buffer.flip();

                ByteBuf chunkBuffer = Unpooled.buffer(5 + bytesRead);
                chunkBuffer.writeByte(2); // 类型2=文件内容
                chunkBuffer.writeInt(bytesRead); // 内容长度
                chunkBuffer.writeBytes(buffer); // 内容数据

                ctx.writeAndFlush(chunkBuffer).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        if (future.isSuccess()) {
                            bytesSent += bytesRead;

                            // 每10MB输出一次进度
                            if (bytesSent % (10 * 1024 * 1024) == 0) {
                                System.out.println("已发送: " + (bytesSent / (1024 * 1024)) + "MB");
                            }

                            // 继续发送下一块
                            sendFileContent(ctx);
                        } else {
                            System.err.println("发送文件块失败: " + future.cause().getMessage());
                            ctx.close();
                        }
                    }
                });
            } else {
                // 读取完成，发送结束标记
                sendFileEnd(ctx);
            }
        } catch (Exception e) {
            System.err.println("发送文件内容失败: " + e.getMessage());
            e.printStackTrace();
            ctx.close();
        }
    }

    private void sendFileEnd(ChannelHandlerContext ctx) {
        try {
            if (raf != null) {
                raf.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        ByteBuf endBuffer = Unpooled.buffer(5);
        endBuffer.writeByte(3); // 类型3=文件结束
        endBuffer.writeInt(0); // 无数据内容

        ctx.writeAndFlush(endBuffer).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    System.out.println("文件上传完成，总大小: " + (bytesSent / (1024 * 1024)) + "MB");
                } else {
                    System.err.println("发送文件结束标记失败: " + future.cause().getMessage());
                }

                // 等待1秒后关闭连接
                ctx.channel().eventLoop().schedule(() -> {
                    ctx.close();
                }, 1, java.util.concurrent.TimeUnit.SECONDS);
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof DownloadContentMessage) {
            // 处理下载文件内容
            DownloadContentMessage contentMsg = (DownloadContentMessage) msg;
            ByteBuf buf = contentMsg.content;
            try {
                int bytesToWrite = buf.readableBytes();
                if (bytesToWrite > 0 && downloadFileChannel != null) {
                    ByteBuffer nioBuffer = buf.nioBuffer();
                    while (nioBuffer.hasRemaining()) {
                        downloadFileChannel.write(nioBuffer);
                    }
                    downloadBytesReceived += bytesToWrite;

                    if (downloadBytesReceived % (10 * 1024 * 1024) == 0) {
                        System.out.println("已接收: " + (downloadBytesReceived / (1024 * 1024)) + "MB");
                    }
                }
            } catch (IOException e) {
                System.err.println("写入下载文件失败: " + e.getMessage());
                ctx.close();
            } finally {
                buf.release();
            }
        } else if (msg instanceof DownloadEndMessage) {
            // 下载完成
            System.out.println("文件下载完成: " + downloadFileName);
            System.out.println("文件大小: " + (downloadBytesReceived / (1024 * 1024)) + "MB");

            try {
                if (downloadFileChannel != null) {
                    downloadFileChannel.force(true);
                    downloadFileChannel.close();
                    downloadFileChannel = null;
                }
                if (raf != null) {
                    raf.close();
                    raf = null;
                }
            } catch (IOException e) {
                System.err.println("关闭下载文件失败: " + e.getMessage());
            }

            // 可以关闭连接
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        try {
            if (raf != null) {
                raf.close();
            }
            if (downloadFileChannel != null) {
                downloadFileChannel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("连接关闭");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.err.println("客户端异常: " + cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}
