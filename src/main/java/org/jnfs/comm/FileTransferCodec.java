package org.jnfs.comm;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

public class FileTransferCodec {

    // 长度字段编码器（解决粘包/拆包）
    public static class LengthFieldEncoder extends LengthFieldPrepender {
        public LengthFieldEncoder() {
            super(4); // 长度字段占4个字节
        }
    }

    // 对象编码器（将对象序列化为字节）
    public static class FileEncoder extends ObjectEncoder {
        // 直接使用Netty的ObjectEncoder，无需重写
    }

    // 帧解码器（根据长度字段分割数据包）
    public static class FrameDecoder extends LengthFieldBasedFrameDecoder {
        public FrameDecoder() {
            super(
                FileTransferConstants.MAX_FRAME_LENGTH, // 最大帧长度
                0, 4, // 长度字段偏移量0，长度4字节
                0, 4 // 跳过长度字段本身（4字节）
            );
        }
    }

    // 对象解码器（将字节反序列化为对象）
    public static class FileDecoder extends ObjectDecoder {
        public FileDecoder() {
            super(
                FileTransferConstants.MAX_FRAME_LENGTH,
                ClassResolvers.weakCachingConcurrentResolver(
                    FileTransferMessage.class.getClassLoader()
                )
            );
        }
    }
}
