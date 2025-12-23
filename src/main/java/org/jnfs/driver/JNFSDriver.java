package org.jnfs.driver;

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
     * 1. 询问 NameNode 获取 DataNode 地址
     * 2. 连接 DataNode 上传文件
     * 3. 告知 NameNode 上传完成 (Commit)
     */
    public void uploadFile(File file) throws Exception {
        if (!file.exists()) {
            throw new IOException("文件不存在: " + file.getAbsolutePath());
        }

        // 1. 获取上传位置
        String dataNodeAddr = getDataNodeForUpload();
        System.out.println("[Driver] 获得上传节点: " + dataNodeAddr);

        // 解析 Host:Port
        String[] parts = dataNodeAddr.split(":");
        String dnHost = parts[0];
        int dnPort = Integer.parseInt(parts[1]);

        // 2. 上传数据到 DataNode
        uploadToDataNode(dnHost, dnPort, file);
        System.out.println("[Driver] 文件数据传输完成");

        // 3. 提交元数据到 NameNode
        commitFile(file.getName(), dataNodeAddr);
        System.out.println("[Driver] 文件元数据提交完成");
    }

    // --- 内部辅助方法 ---

    private String getDataNodeForUpload() throws Exception {
        return sendRequestToNameNode(CommandType.NAMENODE_REQUEST_UPLOAD_LOC, new byte[0]);
    }

    private void commitFile(String filename, String dataNodeAddr) throws Exception {
        String payload = filename + "|" + dataNodeAddr;
        sendRequestToNameNode(CommandType.NAMENODE_COMMIT_FILE, payload.getBytes(StandardCharsets.UTF_8));
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
    private String sendRequestToNameNode(CommandType type, byte[] data) throws Exception {
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

        if (response.getCommandType() == CommandType.ERROR) {
            throw new IOException("NameNode 错误: " + new String(response.getData(), StandardCharsets.UTF_8));
        }

        return new String(response.getData(), StandardCharsets.UTF_8);
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
}
