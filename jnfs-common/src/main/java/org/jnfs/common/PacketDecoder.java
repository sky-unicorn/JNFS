package org.jnfs.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 协议包解码器
 * 处理 TCP 粘包/拆包问题，并将字节流转换为 Packet 对象
 * 同时支持基于状态的大文件流式传输处理
 */
public class PacketDecoder extends ByteToMessageDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(PacketDecoder.class);
    private static final int MAGIC_NUMBER = 0xCAFEBABE;
    // 最大允许的 Token 长度 (4KB)
    private static final int MAX_TOKEN_LENGTH = 4096;
    // 最大允许的 Data 长度 (16MB) - 防止 OOM 攻击
    private static final int MAX_DATA_LENGTH = 16 * 1024 * 1024;
    // 最大允许的 Stream 长度 (默认 1TB) - 仅作为 DoS 抑制, 非内存安全边界
    //
    // 设计说明 (挑刺审查第3轮 P0-3):
    //   1. OOM 防护已由流式解码器承担: fileBytesToRead 只是个 long 计数器, 每个 chunk 经
    //      readRetainedSlice 透传后即释放, 不在堆内累积。故本上限不是真正的内存安全边界。
    //   2. 本上限仅起 DoS 抑制作用: 防止异常/恶意的 streamLength 让连接长期挂起。
    //      下载侧另有 DownloadHandler 30 分钟超时兜底。
    //   3. 单流传输路径 (JNFSDriver.uploadToDataNode / downloadFromDataNode、
    //      DataNodeHandler.initiateUpload) 直接把 file.length() 写入 streamLength,
    //      使用 DefaultFileRegion 零拷贝传输 (基于 long 偏移/计数, 原生支持 >2GB)。
    //      代码中无分片/分块协议, 故本上限必须容纳大文件, 否则会拒掉正常的 >2GB 上传/下载。
    //
    // 可通过系统属性 -Djnfs.packet.maxStreamLength=<bytes> 覆盖默认值 (例如更低以收紧 DoS 防护)。
    private static final long MAX_STREAM_LENGTH = initMaxStreamLength();

    private static long initMaxStreamLength() {
        final long defaultLimit = 1L * 1024 * 1024 * 1024 * 1024; // 1TB
        String prop = System.getProperty("jnfs.packet.maxStreamLength");
        if (prop != null && !prop.trim().isEmpty()) {
            try {
                long parsed = Long.parseLong(prop.trim());
                if (parsed > 0) {
                    return parsed;
                }
                LOG.warn("非法的 jnfs.packet.maxStreamLength 配置 '{}' (必须 >0), 使用默认值 {}",
                        prop, defaultLimit);
            } catch (NumberFormatException e) {
                LOG.warn("无法解析 jnfs.packet.maxStreamLength 配置 '{}', 使用默认值 {}",
                        prop, defaultLimit);
            }
        }
        return defaultLimit;
    }

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

        // 安全校验: Token 长度
        if (tokenLength < 0 || tokenLength > MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException("非法 Token 长度: " + tokenLength);
        }

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

        // 安全校验: Data 长度
        if (length < 0 || length > MAX_DATA_LENGTH) {
            throw new IllegalArgumentException("非法数据包长度: " + length);
        }

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

        // 安全校验: Stream 长度上限 (DoS 抑制, 非内存边界, 详见 MAX_STREAM_LENGTH 注释)
        // 用 ctx.close() 拒绝连接而非抛异常 (抛异常会让 ByteToMessageDecoder 进入异常状态)
        // 已读完整包头, 无需 resetReaderIndex, 直接关闭连接
        if (streamLength < 0 || streamLength > MAX_STREAM_LENGTH) {
            LOG.warn("拒绝连接: 非法 streamLength={} (允许范围 0 ~ {}), remote={}",
                    streamLength, MAX_STREAM_LENGTH, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        Packet packet = new Packet();
        packet.setVersion(version);
        packet.setCommandType(CommandType.fromByte(command));
        packet.setToken(token);
        packet.setData(data);
        packet.setStreamLength(streamLength);

        out.add(packet);

        // 3. 检查是否有后续流数据，如果有，则切换到文件流模式
        if (streamLength > 0) {
            // 防御性断言：streamLength 已通过 MAX_STREAM_LENGTH 上限校验 (DoS 抑制, 非内存边界)
            // 设置状态，后续字节将作为文件内容直接透传
            this.fileBytesToRead = streamLength;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 连接断开时重置流式传输状态, 防止状态残留影响后续逻辑
        // (即便 ByteToMessageDecoder 实例通常每连接独立, 重置仍提供防御性保证)
        if (fileBytesToRead > 0) {
            LOG.warn("连接关闭, 仍有 {} 字节未完成流式接收, remote={}",
                    fileBytesToRead, ctx.channel().remoteAddress());
            fileBytesToRead = 0;
        }
        super.channelInactive(ctx);
    }
}
