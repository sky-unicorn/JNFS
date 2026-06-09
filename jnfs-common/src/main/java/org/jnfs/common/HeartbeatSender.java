package org.jnfs.common;

import io.netty.channel.Channel;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.function.Function;

/**
 * 心跳发送工具类
 * 封装通过连接池向 Registry 广播心跳的通用流程：acquire → write → release
 *
 * 使用场景：
 * - NameNode 向 Registry 发送 REGISTRY_HEARTBEAT_NAMENODE 心跳
 * - DataNode 向 Registry 发送 REGISTRY_HEARTBEAT 心跳
 * - 任何需要通过 ChannelPoolMap 广播消息的场景
 *
 * 示例 (NameNode 心跳)：
 * <pre>
 * HeartbeatSender.broadcast(logger, registryPoolMap, registryAddresses,
 *     CommandType.REGISTRY_HEARTBEAT_NAMENODE,
 *     addr -> (advertisedHost + ":" + port).getBytes(StandardCharsets.UTF_8));
 * </pre>
 *
 * 示例 (DataNode 心跳)：
 * <pre>
 * HeartbeatSender.broadcast(logger, registryPoolMap, registryAddresses,
 *     CommandType.REGISTRY_HEARTBEAT,
 *     addr -> (advertisedHost + ":" + port + "|" + totalFreeSpace).getBytes(StandardCharsets.UTF_8));
 * </pre>
 */
public final class HeartbeatSender {

    private HeartbeatSender() {
        // 工具类，禁止实例化
    }

    /**
     * 向所有目标地址广播心跳包（使用默认错误处理：日志记录）
     *
     * @param log               调用方的 Logger
     * @param poolMap           连接池映射
     * @param addresses         目标地址集合
     * @param commandType       心跳命令类型
     * @param payloadGenerator  payload 生成器，输入目标地址，输出心跳数据
     */
    public static void broadcast(Logger log,
                                 io.netty.channel.pool.ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap,
                                 Collection<InetSocketAddress> addresses,
                                 CommandType commandType,
                                 Function<InetSocketAddress, byte[]> payloadGenerator) {
        broadcast(log, poolMap, addresses, commandType, payloadGenerator, null);
    }

    /**
     * 向所有目标地址广播心跳包（支持自定义错误处理）
     *
     * @param log               调用方的 Logger
     * @param poolMap           连接池映射
     * @param addresses         目标地址集合
     * @param commandType       心跳命令类型
     * @param payloadGenerator  payload 生成器，输入目标地址，输出心跳数据
     * @param errorHandler      自定义错误处理器（可为 null，使用默认日志记录）
     */
    public static void broadcast(Logger log,
                                 io.netty.channel.pool.ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap,
                                 Collection<InetSocketAddress> addresses,
                                 CommandType commandType,
                                 Function<InetSocketAddress, byte[]> payloadGenerator,
                                 ErrorHandler errorHandler) {
        for (InetSocketAddress addr : addresses) {
            SimpleChannelPool pool = poolMap.get(addr);
            Future<Channel> future = pool.acquire();

            future.addListener((FutureListener<Channel>) f -> {
                if (f.isSuccess()) {
                    Channel ch = f.getNow();
                    try {
                        byte[] data = payloadGenerator.apply(addr);
                        Packet packet = new Packet();
                        packet.setCommandType(commandType);
                        packet.setToken(Constants.getValidToken());
                        packet.setData(data);

                        ch.writeAndFlush(packet).addListener(writeFuture -> pool.release(ch));
                    } catch (Exception e) {
                        pool.release(ch);
                        if (errorHandler != null) {
                            errorHandler.onException(addr, e);
                        } else {
                            log.error("发送心跳异常 ({}) : {}", addr, e.getMessage());
                        }
                    }
                } else {
                    if (errorHandler != null) {
                        errorHandler.onConnectFailure(addr, f.cause());
                    } else {
                        log.warn("连接注册中心失败 ({}) : {}", addr, f.cause().getMessage());
                    }
                }
            });
        }
    }

    /**
     * 便捷方法：使用字符串 payload 广播心跳
     * 字符串将按 UTF-8 编码为字节数组
     *
     * @param log               调用方的 Logger
     * @param poolMap           连接池映射
     * @param addresses         目标地址集合
     * @param commandType       心跳命令类型
     * @param payloadGenerator  payload 生成器，输入目标地址，输出心跳字符串
     */
    public static void broadcastString(Logger log,
                                       io.netty.channel.pool.ChannelPoolMap<InetSocketAddress, SimpleChannelPool> poolMap,
                                       Collection<InetSocketAddress> addresses,
                                       CommandType commandType,
                                       Function<InetSocketAddress, String> payloadGenerator) {
        broadcast(log, poolMap, addresses, commandType,
                addr -> payloadGenerator.apply(addr).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 错误处理接口，允许调用方自定义心跳发送过程中的异常处理逻辑
     */
    @FunctionalInterface
    public interface ErrorHandler {

        /**
         * 心跳包构造或写入时发生异常
         *
         * @param addr 目标地址
         * @param e    异常
         */
        void onException(InetSocketAddress addr, Exception e);

        /**
         * 连接目标地址失败（默认委托给 onException）
         *
         * @param addr   目标地址
         * @param cause  失败原因
         */
        default void onConnectFailure(InetSocketAddress addr, Throwable cause) {
            onException(addr, cause instanceof Exception ? (Exception) cause : new Exception(cause));
        }
    }
}
