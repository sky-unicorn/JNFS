package org.jnfs.common;

import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;

/**
 * Netty 业务 Handler 通用辅助方法
 * 统一提供 Token 校验、响应发送等共享逻辑
 *
 * 使用场景：
 * - 所有 SimpleChannelInboundHandler 的 sendResponse
 * - 替换 RegistryHandler / NameNodeHandler / DataNodeHandler 中重复的 Token 校验
 */
public final class NettyHandlerHelper {

    private NettyHandlerHelper() {
        // 工具类，禁止实例化
    }

    /**
     * 校验 Token 是否有效
     */
    public static boolean validateToken(String token) {
        return Constants.getValidToken().equals(token);
    }

    /**
     * 发送响应包
     *
     * @param ctx Channel 上下文
     * @param type 命令类型
     * @param data 响应数据
     */
    public static void sendResponse(ChannelHandlerContext ctx, CommandType type, byte[] data) {
        Packet response = new Packet();
        response.setCommandType(type);
        response.setData(data);
        ctx.writeAndFlush(response);
    }

    /**
     * 发送错误响应 (UTF-8 字符串)
     */
    public static void sendError(ChannelHandlerContext ctx, String message) {
        sendResponse(ctx, CommandType.ERROR, message.getBytes(StandardCharsets.UTF_8));
    }
}
