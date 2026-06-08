package org.jnfs.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 守护线程工厂
 * 统一创建 JNFS 模块中所有后台调度线程 (Daemon 模式)
 *
 * 使用场景：
 * - 心跳线程
 * - 服务发现线程
 * - 垃圾回收线程
 * - 节点清理线程
 * - 客户端刷新线程
 */
public class DaemonThreadFactory implements ThreadFactory {

    private final String namePrefix;
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * @param namePrefix 线程名前缀 (例如 "NameNode-Heartbeat")
     */
    public DaemonThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, namePrefix + "-" + counter.incrementAndGet());
        t.setDaemon(true);
        return t;
    }
}
