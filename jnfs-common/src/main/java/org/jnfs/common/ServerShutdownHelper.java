package org.jnfs.common;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.SimpleChannelPool;
import org.slf4j.Logger;

import java.util.Map;

/**
 * 服务端资源释放辅助类
 * 统一管理调度器、连接池、EventLoopGroup 的关闭顺序
 *
 * 使用场景：
 * - NameNodeServer.shutdown()
 * - DataNodeServer.shutdown()
 * - 任何需要清理 Netty 服务端资源的场景
 */
public final class ServerShutdownHelper {

    private ServerShutdownHelper() {
        // 工具类，禁止实例化
    }

    /**
     * 关闭所有提供的 ScheduledExecutorService
     */
    public static void shutdownSchedulers(java.util.concurrent.ScheduledExecutorService... schedulers) {
        if (schedulers == null) return;
        for (java.util.concurrent.ScheduledExecutorService s : schedulers) {
            if (s != null && !s.isShutdown()) {
                s.shutdownNow();
            }
        }
    }

    /**
     * 关闭 ChannelPoolMap 中所有的 SimpleChannelPool
     */
    public static void closePoolMap(ChannelPoolMap<?, SimpleChannelPool> poolMap) {
        if (poolMap == null) return;
        if (poolMap instanceof Iterable) {
            for (Object poolObj : (Iterable<?>) poolMap) {
                if (poolObj instanceof SimpleChannelPool) {
                    closeQuietly((SimpleChannelPool) poolObj);
                } else if (poolObj instanceof Map.Entry) {
                    Object value = ((Map.Entry<?, ?>) poolObj).getValue();
                    if (value instanceof SimpleChannelPool) {
                        closeQuietly((SimpleChannelPool) value);
                    }
                }
            }
        }
    }

    private static void closeQuietly(SimpleChannelPool pool) {
        try {
            pool.close();
        } catch (Exception ignore) {
            // 关闭时的异常忽略
        }
    }

    /**
     * 优雅关闭 EventLoopGroup
     */
    public static void shutdownEventLoopGroup(EventLoopGroup group) {
        if (group != null && !group.isShutdown()) {
            group.shutdownGracefully();
        }
    }

    /**
     * 一站式关闭：scheduler + poolMap + workerGroup
     *
     * @param logger      用于输出日志
     * @param serverName  服务名 (例如 "NameNodeServer")
     * @param runningFlag 外部持有的运行标志引用 (此方法会将其置为 false)
     * @param schedulers  所有需要关闭的调度器
     * @param poolMap     连接池 Map (可为 null)
     * @param workerGroup Worker EventLoopGroup (可为 null)
     */
    public static void shutdownAll(Logger logger,
                                   String serverName,
                                   java.util.concurrent.atomic.AtomicBoolean runningFlag,
                                   java.util.concurrent.ScheduledExecutorService[] schedulers,
                                   ChannelPoolMap<?, SimpleChannelPool> poolMap,
                                   EventLoopGroup workerGroup) {
        if (runningFlag != null && !runningFlag.compareAndSet(true, false)) {
            return; // 已关闭，幂等返回
        }
        if (logger != null) {
            logger.info("正在停止 {} 资源...", serverName);
        }

        shutdownSchedulers(schedulers);
        closePoolMap(poolMap);
        shutdownEventLoopGroup(workerGroup);

        if (logger != null) {
            logger.info("{} 资源释放完成", serverName);
        }
    }
}
