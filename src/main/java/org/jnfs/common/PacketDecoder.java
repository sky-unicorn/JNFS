package org.jnfs.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 协议包解码器
 * 处理 TCP 粘包/拆包问题，并将字节流转换为 Packet 对象
 * 同时支持基于状态的大文件流式传输处理
 */
public class PacketDecoder extends ByteToMessageDecoder {
    private static final int MAGIC_NUMBER = 0xCAFEBABE;

    // 文件流传输状态管理：记录剩余需要读取的文件字节数
    private long fileBytesToRead = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 1. 如果处于文件流传输模式，直接透传数据
        if (fileBytesToRead > 0) {
            int readable = in.readableBytes();
            if (readable == 0) {
                return;
            }
            
            // 读取当前可读字节或剩余所需字节的较小值
            int toRead = (int) Math.min(readable, fileBytesToRead);
            // 使用 retain() 增加引用计数，因为 slice 后的 buffer 可能会在后续 handler 中被释放
            ByteBuf chunk = in.readRetainedSlice(toRead);
            out.add(chunk);
            
            fileBytesToRead -= toRead;
            return;
        }

        // 2. 普通协议包解码
        // 校验最小长度: magic(4) + version(1) + command(1) + tokenLen(4) + dataLen(4) + streamLen(8) = 22 字节
        if (in.readableBytes() < 22) { 
            return;
        }

        in.markReaderIndex();
        int magic = in.readInt();
        // 校验魔数
        if (magic != MAGIC_NUMBER) {
            ctx.close();
            return;
        }

        byte version = in.readByte();
        byte command = in.readByte();
        
        // 读取 Token
        int tokenLength = in.readInt();
        if (in.readableBytes() < tokenLength) {
            in.resetReaderIndex();
            return;
        }
        String token = null;
        if (tokenLength > 0) {
            byte[] tokenBytes = new byte[tokenLength];
            in.readBytes(tokenBytes);
            token = new String(tokenBytes, StandardCharsets.UTF_8);
        }
        
        // 读取 Data
        // 需再次检查长度，因为上面可能刚读完 token，但 data 还没到
        if (in.readableBytes() < 4) {
            in.resetReaderIndex();
            return;
        }
        
        int length = in.readInt();

        // 校验数据包完整性
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        byte[] data = new byte[length];
        in.readBytes(data);

        // 读取流数据长度
        if (in.readableBytes() < 8) {
            in.resetReaderIndex();
            return;
        }
        long streamLength = in.readLong();

        Packet packet = new Packet();
        packet.setVersion(version);
        packet.setCommandType(CommandType.fromByte(command));
        packet.setToken(token);
        packet.setData(data);
        packet.setStreamLength(streamLength);

        out.add(packet);
        
        // 3. 检查是否有后续流数据，如果有，则切换到文件流模式
        if (streamLength > 0) {
             // 设置状态，后续字节将作为文件内容直接读取
             this.fileBytesToRead = streamLength;
        }
    }
}
