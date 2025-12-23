package org.jnfs.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * 协议包编码器
 * 将 Packet 对象编码为字节流发送
 * 协议格式: Magic(4) + Version(1) + Command(1) + Length(4) + Data(N)
 */
public class PacketEncoder extends MessageToByteEncoder<Packet> {
    private static final int MAGIC_NUMBER = 0xCAFEBABE;

    @Override
    protected void encode(ChannelHandlerContext ctx, Packet msg, ByteBuf out) throws Exception {
        // 1. 写入魔数 (4字节)
        out.writeInt(MAGIC_NUMBER);
        // 2. 写入版本号 (1字节)
        out.writeByte(msg.getVersion());
        // 3. 写入指令类型 (1字节)
        out.writeByte(msg.getCommandType().getValue());
        
        // 4. 写入数据长度 (4字节) 和 数据内容
        if (msg.getData() != null) {
            out.writeInt(msg.getData().length);
            out.writeBytes(msg.getData());
        } else {
            out.writeInt(0);
        }
    }
}
