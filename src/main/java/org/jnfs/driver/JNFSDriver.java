package org.jnfs.driver;

import cn.hutool.crypto.digest.DigestUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;
import org.jnfs.common.SecurityUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JNFS Driver (SDK)
 * 提供给客户端应用使用的核心 API
 * 
 * 升级：使用 Netty ChannelPool 复用 NameNode 连接
 */
public class JNFSDriver {

    private static final String CLIENT_TOKEN = "jnfs-secure-token-2025"; 

    private final String nameNodeHost;
    private final int nameNodePort;
    private final EventLoopGroup group;
    private final String downloadPath;
    
    // 连接池映射: Address -> Pool
    private final ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap;

    public JNFSDriver(String nameNodeHost, int nameNodePort) {
        this.nameNodeHost = nameNodeHost;
        this.nameNodePort = nameNodePort;
        this.group = new NioEventLoopGroup();
        this.downloadPath = "D:\\data\\jnfs\\download";
        
        // 初始化连接池
        this.poolMap = new AbstractChannelPoolMap<InetSocketAddress, SimpleChannelPool>() {
            @Override
            protected SimpleChannelPool newPool(InetSocketAddress key) {
                Bootstrap b = new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true);
                
                // 使用 FixedChannelPool 限制最大连接数，防止资源耗尽
                return new FixedChannelPool(b.remoteAddress(key), new NameNodeChannelPoolHandler(), 10);
            }
        };
    }

    public void close() {
        group.shutdownGracefully();
    }

    /**
     * 上传文件
     * 1. 客户端本地加密
     * 2. 上传密文到 DataNode
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

        // --- 加密环节 ---
        System.out.println("[Driver] 正在对文件进行本地加密...");
        File encryptedFile = new File(file.getParent(), file.getName() + ".enc");
        SecurityUtil.encryptFile(file, encryptedFile);
        System.out.println("[Driver] 加密完成，准备上传密文");

        try {
            System.out.println("[Driver] 获得上传许可，开始上传...");

            String dataNodeAddr = getDataNodeForUpload();
            System.out.println("[Driver] 获得上传节点: " + dataNodeAddr);
            
            String[] parts = dataNodeAddr.split(":");
            String dnHost = parts[0];
            int dnPort = Integer.parseInt(parts[1]);

            // 上传密文，但使用原始文件的 Hash (用于秒传和校验)
            uploadToDataNode(dnHost, dnPort, encryptedFile, fileHash);
            System.out.println("[Driver] 文件数据传输完成");

            String storageId = commitFile(file.getName(), fileHash, dataNodeAddr);
            System.out.println("[Driver] 文件元数据提交完成，存储编号: " + storageId);
            
            return storageId;
        } finally {
            // 清理临时密文文件
            if (encryptedFile.exists()) {
                encryptedFile.delete();
            }
        }
    }

    /**
     * 下载文件
     * 1. 下载密文
     * 2. 本地解密
     */
    public File downloadFile(String storageId) throws Exception {
        String locInfo = getDownloadLocation(storageId);
        System.out.println("[Driver] 获取下载信息: " + locInfo);
        
        String[] parts = locInfo.split("\\|");
        if (parts.length != 3) {
            throw new IOException("无效的下载位置信息: " + locInfo);
        }
        
        String filename = parts[0];
        String hash = parts[1]; // 获取 Hash 用于请求下载
        String address = parts[2];
        
        String[] addrParts = address.split(":");
        String dnHost = addrParts[0];
        int dnPort = Integer.parseInt(addrParts[1]);
        
        File downloadDir = new File(downloadPath);
        if (!downloadDir.exists()) {
            downloadDir.mkdirs();
        }
        
        // 先下载到临时密文文件
        File encryptedFile = new File(downloadDir, filename + ".enc");
        File targetFile = new File(downloadDir, filename);
        
        // DataNode 存储的是 Hash 命名的文件 (假设 DataNode 已按 Hash 存储)
        // 或者 DataNode 仍按原名存储? 根据之前的实现是按文件名存储
        // 这里需要与 DataNode 约定下载标识：现在改为传 Hash 下载
        downloadFromDataNode(dnHost, dnPort, hash, encryptedFile);
        System.out.println("[Driver] 密文下载完成: " + encryptedFile.getAbsolutePath());
        
        // --- 解密环节 ---
        System.out.println("[Driver] 正在解密文件...");
        SecurityUtil.decryptFile(encryptedFile, targetFile);
        System.out.println("[Driver] 解密完成: " + targetFile.getAbsolutePath());
        
        // 清理密文
        encryptedFile.delete();
        
        return targetFile;
    }

    // ... 辅助方法 ...

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
            } else if (type == CommandType.ERROR) {
                throw new IOException("错误: " + new String(response.getData(), StandardCharsets.UTF_8));
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

    private void uploadToDataNode(String host, int port, File file, String hash) throws Exception {
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
        // 关键修改: 上传时不再传文件名，而是传 Hash，作为 DataNode 的存储文件名
        byte[] hashBytes = hash.getBytes(StandardCharsets.UTF_8);
        ByteBuffer metadataBuffer = ByteBuffer.allocate(8 + hashBytes.length);
        metadataBuffer.putLong(fileSize);
        metadataBuffer.put(hashBytes);

        Packet packet = new Packet();
        packet.setCommandType(CommandType.UPLOAD_REQUEST);
        packet.setToken(CLIENT_TOKEN);
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

    private void downloadFromDataNode(String host, int port, String hash, File targetFile) throws Exception {
        Bootstrap b = new Bootstrap();
        DownloadHandler handler = new DownloadHandler(targetFile);
        b.group(group)
         .channel(NioSocketChannel.class)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast(new PacketEncoder());
                 ch.pipeline().addLast(handler);
             }
         });

        ChannelFuture f = b.connect(host, port).sync();
        Channel channel = f.channel();

        Packet request = new Packet();
        request.setCommandType(CommandType.DOWNLOAD_REQUEST);
        request.setToken(CLIENT_TOKEN);
        request.setData(hash.getBytes(StandardCharsets.UTF_8));
        channel.writeAndFlush(request);

        handler.waitForCompletion();
        channel.close().sync();
    }

    // --- 使用连接池发送请求 ---
    private Packet sendRequestToNameNode(CommandType type, byte[] data) throws Exception {
        InetSocketAddress address = new InetSocketAddress(nameNodeHost, nameNodePort);
        SimpleChannelPool pool = poolMap.get(address);
        
        Future<Channel> future = pool.acquire();
        Channel channel = future.sync().getNow(); // 阻塞获取连接
        
        SyncHandler handler = new SyncHandler();
        try {
            // 动态添加业务 Handler
            channel.pipeline().addLast("syncHandler", handler);
            
            Packet packet = new Packet();
            packet.setCommandType(type);
            packet.setToken(CLIENT_TOKEN);
            packet.setData(data);
            
            channel.writeAndFlush(packet);
            
            // 等待响应
            return handler.getResponse();
        } finally {
            // 清理 Handler 并释放连接回池
            if (channel.pipeline().get("syncHandler") != null) {
                channel.pipeline().remove("syncHandler");
            }
            pool.release(channel);
        }
    }

    // ... Handlers ...
    
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

    private static class DownloadHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final File targetFile;
        private FileChannel fileChannel;
        private boolean headerReceived = false;
        private long fileSize = -1;
        private long receivedBytes = 0;
        private final BlockingQueue<Boolean> completionSignal = new LinkedBlockingQueue<>();
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
                tempBuf.writeBytes(msg);
                
                if (tempBuf.readableBytes() >= 14) { 
                    tempBuf.markReaderIndex();
                    int magic = tempBuf.readInt();
                    byte version = tempBuf.readByte();
                    byte command = tempBuf.readByte();
                    
                    int tokenLength = tempBuf.readInt();
                    if (tempBuf.readableBytes() < tokenLength) {
                        tempBuf.resetReaderIndex();
                        return;
                    }
                    if (tokenLength > 0) tempBuf.skipBytes(tokenLength);
                    
                    if (tempBuf.readableBytes() < 4) {
                        tempBuf.resetReaderIndex();
                        return;
                    }
                    
                    int length = tempBuf.readInt();
                    
                    if (tempBuf.readableBytes() >= length) {
                        byte[] data = new byte[length];
                        tempBuf.readBytes(data);
                        
                        if (command == CommandType.ERROR.getValue()) {
                            throw new IOException("服务端错误: " + new String(data));
                        }
                        
                        String sizeStr = new String(data, StandardCharsets.UTF_8);
                        fileSize = Long.parseLong(sizeStr);
                        headerReceived = true;
                        
                        if (targetFile.exists()) {
                            targetFile.delete();
                        }
                        FileOutputStream fos = new FileOutputStream(targetFile);
                        fileChannel = fos.getChannel();
                        
                        System.out.println("[Driver] 开始接收文件流，大小: " + fileSize);

                        if (tempBuf.readableBytes() > 0) {
                            writeToFile(tempBuf);
                        }
                    } else {
                        tempBuf.resetReaderIndex();
                    }
                }
            } else {
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
            Boolean result = completionSignal.poll(30, TimeUnit.MINUTES);
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
