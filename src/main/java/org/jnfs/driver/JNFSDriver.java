package org.jnfs.driver;

import cn.hutool.crypto.digest.DigestUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JNFS Driver (SDK)
 * 提供给客户端应用使用的核心 API
 */
public class JNFSDriver {

    private final String nameNodeHost;
    private final int nameNodePort;
    private final EventLoopGroup group;
    private final String downloadPath; // 默认下载路径

    public JNFSDriver(String nameNodeHost, int nameNodePort) {
        this.nameNodeHost = nameNodeHost;
        this.nameNodePort = nameNodePort;
        this.group = new NioEventLoopGroup();
        this.downloadPath = "D:\\data\\jnfs\\download";
    }

    public void close() {
        group.shutdownGracefully();
    }

    /**
     * 上传文件
     * @return 存储编号 (Storage ID)
     */
    public String uploadFile(File file) throws Exception {
        if (!file.exists()) {
            throw new IOException("文件不存在: " + file.getAbsolutePath());
        }

        System.out.println("[Driver] 正在计算文件摘要...");
        String fileHash = DigestUtil.sha256Hex(file);
        System.out.println("[Driver] 文件摘要 (SHA256): " + fileHash);

        String existingAddr = requestUploadPermission(fileHash);
        
        if (existingAddr != null) {
            System.out.println("[Driver] 发现相同文件 (节点: " + existingAddr + ")，触发秒传...");
            String storageId = commitFile(file.getName(), fileHash, existingAddr);
            System.out.println("[Driver] 秒传成功！存储编号: " + storageId);
            return storageId;
        }

        System.out.println("[Driver] 获得上传许可，开始普通上传...");

        String dataNodeAddr = getDataNodeForUpload();
        System.out.println("[Driver] 获得上传节点: " + dataNodeAddr);
        
        String[] parts = dataNodeAddr.split(":");
        String dnHost = parts[0];
        int dnPort = Integer.parseInt(parts[1]);

        uploadToDataNode(dnHost, dnPort, file);
        System.out.println("[Driver] 文件数据传输完成");

        String storageId = commitFile(file.getName(), fileHash, dataNodeAddr);
        System.out.println("[Driver] 文件元数据提交完成，存储编号: " + storageId);
        
        return storageId;
    }

    /**
     * 下载文件
     * @param storageId 存储编号
     * @return 下载后的文件对象
     */
    public File downloadFile(String storageId) throws Exception {
        // 1. 询问 NameNode 获取文件位置
        String locInfo = getDownloadLocation(storageId);
        System.out.println("[Driver] 获取下载信息: " + locInfo);
        
        String[] parts = locInfo.split("\\|");
        if (parts.length != 3) {
            throw new IOException("无效的下载位置信息: " + locInfo);
        }
        
        String filename = parts[0];
        // String hash = parts[1];
        String address = parts[2];
        
        String[] addrParts = address.split(":");
        String dnHost = addrParts[0];
        int dnPort = Integer.parseInt(addrParts[1]);
        
        // 2. 准备本地下载目录
        File downloadDir = new File(downloadPath);
        if (!downloadDir.exists()) {
            downloadDir.mkdirs();
        }
        File targetFile = new File(downloadDir, filename);
        
        // 3. 从 DataNode 下载
        downloadFromDataNode(dnHost, dnPort, filename, targetFile);
        System.out.println("[Driver] 文件下载完成: " + targetFile.getAbsolutePath());
        
        return targetFile;
    }

    // --- 内部辅助方法 ---

    private String requestUploadPermission(String hash) throws Exception {
        while (true) {
            Packet response = sendRequestToNameNode(CommandType.NAMENODE_PRE_UPLOAD, hash.getBytes(StandardCharsets.UTF_8));
            CommandType type = response.getCommandType();

            if (type == CommandType.NAMENODE_RESPONSE_ALLOW) {
                return null;
            } else if (type == CommandType.NAMENODE_RESPONSE_EXIST) {
                return new String(response.getData(), StandardCharsets.UTF_8);
            } else if (type == CommandType.NAMENODE_RESPONSE_WAIT) {
                System.out.println("[Driver] 文件正在上传中，等待重试...");
                Thread.sleep(1000);
            } else {
                throw new IOException("预上传申请失败: " + type);
            }
        }
    }

    private String getDataNodeForUpload() throws Exception {
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_REQUEST_UPLOAD_LOC, new byte[0]);
        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("获取上传节点失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private String getDownloadLocation(String storageId) throws Exception {
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_REQUEST_DOWNLOAD_LOC, storageId.getBytes(StandardCharsets.UTF_8));
        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("获取下载节点失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private String commitFile(String filename, String hash, String dataNodeAddr) throws Exception {
        String payload = filename + "|" + hash + "|" + dataNodeAddr;
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_COMMIT_FILE, payload.getBytes(StandardCharsets.UTF_8));
        
        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("提交元数据失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        
        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private void uploadToDataNode(String host, int port, File file) throws Exception {
        Bootstrap b = new Bootstrap();
        SyncHandler handler = new SyncHandler();
        b.group(group)
         .channel(NioSocketChannel.class)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new PacketDecoder());
                 ch.pipeline().addLast(new PacketEncoder());
                 ch.pipeline().addLast(handler);
             }
         });

        ChannelFuture f = b.connect(host, port).sync();
        Channel channel = f.channel();

        long fileSize = file.length();
        byte[] fileNameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
        ByteBuffer metadataBuffer = ByteBuffer.allocate(8 + fileNameBytes.length);
        metadataBuffer.putLong(fileSize);
        metadataBuffer.put(fileNameBytes);

        Packet packet = new Packet();
        packet.setCommandType(CommandType.UPLOAD_REQUEST);
        packet.setData(metadataBuffer.array());

        channel.write(packet);
        channel.write(new DefaultFileRegion(file, 0, fileSize));
        channel.flush();

        Packet response = handler.getResponse();
        if (response.getCommandType() == CommandType.ERROR) {
            throw new IOException("DataNode 上传失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        
        channel.close().sync();
    }

    private void downloadFromDataNode(String host, int port, String filename, File targetFile) throws Exception {
        Bootstrap b = new Bootstrap();
        DownloadHandler handler = new DownloadHandler(targetFile);
        b.group(group)
         .channel(NioSocketChannel.class)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 // 下载不需要 Packet 解码器，因为 DataNode 会直接发送文件流 (Header后)
                 // 但为了复用协议，我们先发一个 Packet 请求
                 ch.pipeline().addLast(new PacketEncoder());
                 ch.pipeline().addLast(handler); // 自定义 Handler 处理混合流
             }
         });

        ChannelFuture f = b.connect(host, port).sync();
        Channel channel = f.channel();

        // 发送下载请求
        Packet request = new Packet();
        request.setCommandType(CommandType.DOWNLOAD_REQUEST);
        request.setData(filename.getBytes(StandardCharsets.UTF_8));
        channel.writeAndFlush(request);

        // 等待下载完成
        handler.waitForCompletion();
        channel.close().sync();
    }

    private Packet sendRequestToNameNode(CommandType type, byte[] data) throws Exception {
        Bootstrap b = new Bootstrap();
        SyncHandler handler = new SyncHandler();
        b.group(group)
         .channel(NioSocketChannel.class)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new PacketDecoder());
                 ch.pipeline().addLast(new PacketEncoder());
                 ch.pipeline().addLast(handler);
             }
         });

        ChannelFuture f = b.connect(nameNodeHost, nameNodePort).sync();
        Channel channel = f.channel();

        Packet packet = new Packet();
        packet.setCommandType(type);
        packet.setData(data);
        channel.writeAndFlush(packet);

        Packet response = handler.getResponse();
        channel.close().sync();

        return response;
    }

    // --- Handlers ---

    private static class SyncHandler extends SimpleChannelInboundHandler<Packet> {
        private final BlockingQueue<Packet> queue = new LinkedBlockingQueue<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
            queue.offer(msg);
        }

        public Packet getResponse() throws InterruptedException {
            Packet p = queue.poll(10, TimeUnit.SECONDS);
            if (p == null) {
                throw new RuntimeException("等待响应超时");
            }
            return p;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    /**
     * 专门用于处理下载流的 Handler
     * 状态机: 
     * 1. 接收 Packet 头 (DOWNLOAD_RESPONSE + Length)
     * 2. 接收文件内容 (Raw Bytes)
     */
    private static class DownloadHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final File targetFile;
        private FileChannel fileChannel;
        private boolean headerReceived = false;
        private long fileSize = -1;
        private long receivedBytes = 0;
        private final BlockingQueue<Boolean> completionSignal = new LinkedBlockingQueue<>();
        
        // 缓冲区用于暂存 Packet 数据解析
        private ByteBuf tempBuf; 

        public DownloadHandler(File targetFile) {
            this.targetFile = targetFile;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            tempBuf = ctx.alloc().buffer();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            if (!headerReceived) {
                // 累积数据直到足以解析 Packet 头
                tempBuf.writeBytes(msg);
                
                // 尝试解析 Packet
                // Magic(4) + Version(1) + Command(1) + Length(4) = 10 bytes
                if (tempBuf.readableBytes() >= 10) {
                    tempBuf.markReaderIndex();
                    int magic = tempBuf.readInt(); // 0xCAFEBABE
                    byte version = tempBuf.readByte();
                    byte command = tempBuf.readByte();
                    int length = tempBuf.readInt();
                    
                    if (tempBuf.readableBytes() >= length) {
                        byte[] data = new byte[length];
                        tempBuf.readBytes(data);
                        
                        if (command == CommandType.ERROR.getValue()) {
                            throw new IOException("服务端错误: " + new String(data));
                        }
                        
                        // 解析文件大小
                        String sizeStr = new String(data, StandardCharsets.UTF_8);
                        fileSize = Long.parseLong(sizeStr);
                        headerReceived = true;
                        
                        // 准备写入文件
                        if (targetFile.exists()) {
                            targetFile.delete();
                        }
                        FileOutputStream fos = new FileOutputStream(targetFile);
                        fileChannel = fos.getChannel();
                        
                        System.out.println("[Driver] 开始接收文件流，大小: " + fileSize);

                        // 处理 tempBuf 中剩余的字节 (属于文件内容)
                        if (tempBuf.readableBytes() > 0) {
                            writeToFile(tempBuf);
                        }
                    } else {
                        tempBuf.resetReaderIndex();
                    }
                }
            } else {
                // 纯文件流模式
                writeToFile(msg);
            }
        }

        private void writeToFile(ByteBuf buf) throws IOException {
            if (fileChannel != null) {
                int readable = buf.readableBytes();
                buf.readBytes(fileChannel, receivedBytes, readable);
                receivedBytes += readable;
                
                if (receivedBytes >= fileSize) {
                    System.out.println("[Driver] 下载完成");
                    fileChannel.close();
                    completionSignal.offer(true);
                }
            }
        }

        public void waitForCompletion() throws InterruptedException {
            Boolean result = completionSignal.poll(30, TimeUnit.MINUTES); // 大文件可能需要较长时间
            if (result == null) {
                throw new RuntimeException("下载超时");
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
            completionSignal.offer(false);
        }
    }
}
