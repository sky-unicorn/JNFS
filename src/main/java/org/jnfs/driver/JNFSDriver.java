package org.jnfs.driver;

import cn.hutool.crypto.digest.DigestUtil;
import io.netty.bootstrap.Bootstrap;
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
     * @return 存储编号 (Storage ID)
     */
    public String uploadFile(File file) throws Exception {
        if (!file.exists()) {
            throw new IOException("文件不存在: " + file.getAbsolutePath());
        }

        System.out.println("[Driver] 正在计算文件摘要...");
        String fileHash = DigestUtil.sha256Hex(file);
        System.out.println("[Driver] 文件摘要 (SHA256): " + fileHash);

        // 申请上传 (并发控制 + 秒传检查)
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
