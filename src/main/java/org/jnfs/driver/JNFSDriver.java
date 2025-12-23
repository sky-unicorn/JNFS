package org.jnfs.driver;

import cn.hutool.crypto.digest.DigestUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.jnfs.common.CommandType;
import org.jnfs.common.Packet;
import org.jnfs.common.PacketDecoder;
import org.jnfs.common.PacketEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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

    public JNFSDriver(String nameNodeHost, int nameNodePort) {
        this.nameNodeHost = nameNodeHost;
        this.nameNodePort = nameNodePort;
        this.group = new NioEventLoopGroup();
    }

    public void close() {
        group.shutdownGracefully();
    }

    /**
     * 上传文件
     * 1. 计算文件 Hash
     * 2. 询问 NameNode 是否存在 (秒传检查)
     * 3. 如果存在 -> 提交元数据映射 (完成)
     * 4. 如果不存在 -> 获取 DataNode -> 上传 -> 提交元数据
     */
    public void uploadFile(File file) throws Exception {
        if (!file.exists()) {
            throw new IOException("文件不存在: " + file.getAbsolutePath());
        }

        // 1. 计算文件摘要 (Hash)
        System.out.println("[Driver] 正在计算文件摘要...");
        String fileHash = DigestUtil.sha256Hex(file);
        System.out.println("[Driver] 文件摘要 (SHA256): " + fileHash);

        // 2. 检查是否可以秒传
        String existingAddr = checkFileExistence(fileHash);

        if (existingAddr != null) {
            // --- 秒传逻辑 ---
            System.out.println("[Driver] 发现相同文件 (节点: " + existingAddr + ")，触发秒传...");
            // 提交新的文件名到 Hash 的映射，完成“上传”
            commitFile(file.getName(), fileHash, existingAddr);
            System.out.println("[Driver] 秒传成功！");
            return;
        }

        // --- 普通上传逻辑 ---
        System.out.println("[Driver] 未发现相同文件，开始普通上传...");

        // 3. 获取上传位置
        String dataNodeAddr = getDataNodeForUpload();
        System.out.println("[Driver] 获得上传节点: " + dataNodeAddr);

        // 解析 Host:Port
        String[] parts = dataNodeAddr.split(":");
        String dnHost = parts[0];
        int dnPort = Integer.parseInt(parts[1]);

        // 4. 上传数据到 DataNode
        uploadToDataNode(dnHost, dnPort, file);
        System.out.println("[Driver] 文件数据传输完成");

        // 5. 提交元数据到 NameNode (包含 Hash)
        commitFile(file.getName(), fileHash, dataNodeAddr);
        System.out.println("[Driver] 文件元数据提交完成");
    }

    // --- 内部辅助方法 ---

    /**
     * 检查文件是否存在
     * @return 如果存在返回存储地址，否则返回 null
     */
    private String checkFileExistence(String hash) throws Exception {
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_CHECK_EXISTENCE, hash.getBytes(StandardCharsets.UTF_8));

        if (response.getCommandType() == CommandType.NAMENODE_RESPONSE_EXIST) {
            return new String(response.getData(), StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    private String getDataNodeForUpload() throws Exception {
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_REQUEST_UPLOAD_LOC, new byte[0]);
        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("获取上传节点失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
        return new String(response.getData(), StandardCharsets.UTF_8);
    }

    private void commitFile(String filename, String hash, String dataNodeAddr) throws Exception {
        // Payload: filename|hash|dataNodeAddr
        String payload = filename + "|" + hash + "|" + dataNodeAddr;
        Packet response = sendRequestToNameNode(CommandType.NAMENODE_COMMIT_FILE, payload.getBytes(StandardCharsets.UTF_8));

        if (response.getCommandType() == CommandType.ERROR) {
             throw new IOException("提交元数据失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }
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

        // 构造上传请求
        long fileSize = file.length();
        byte[] fileNameBytes = file.getName().getBytes(StandardCharsets.UTF_8);
        ByteBuffer metadataBuffer = ByteBuffer.allocate(8 + fileNameBytes.length);
        metadataBuffer.putLong(fileSize);
        metadataBuffer.put(fileNameBytes);

        Packet packet = new Packet();
        packet.setCommandType(CommandType.UPLOAD_REQUEST);
        packet.setData(metadataBuffer.array());

        // 发送元数据
        channel.write(packet);
        // 发送文件内容 (Zero Copy)
        channel.write(new DefaultFileRegion(file, 0, fileSize));
        channel.flush();

        // 等待响应
        Packet response = handler.getResponse();
        if (response.getCommandType() == CommandType.ERROR) {
            throw new IOException("DataNode 上传失败: " + new String(response.getData(), StandardCharsets.UTF_8));
        }

        channel.close().sync();
    }

    /**
     * 发送请求给 NameNode 并同步等待响应
     */
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

    /**
     * 同步 Handler，用于等待响应结果
     */
    private static class SyncHandler extends SimpleChannelInboundHandler<Packet> {
        private final BlockingQueue<Packet> queue = new LinkedBlockingQueue<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Packet msg) {
            queue.offer(msg);
        }

        public Packet getResponse() throws InterruptedException {
            Packet p = queue.poll(10, TimeUnit.SECONDS); // 等待10秒
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
}
