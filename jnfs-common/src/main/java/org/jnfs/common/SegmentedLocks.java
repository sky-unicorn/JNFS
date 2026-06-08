package org.jnfs.common;

/**
 * 分段锁工具类
 * 将全局锁拆分为多个分段锁，减小锁粒度，提升并发性能
 *
 * 使用场景：
 * - Hash / 文件名分桶的并发控制
 * - 缓存分片
 * - 任何需要按 Key 隔离的临界区
 */
public class SegmentedLocks {

    private final Object[] locks;

    /**
     * 创建 128 段的分段锁
     */
    public SegmentedLocks() {
        this(128);
    }

    /**
     * @param segmentCount 分段数，建议使用 2 的幂 (例如 64, 128, 256)
     */
    public SegmentedLocks(int segmentCount) {
        if (segmentCount <= 0) {
            throw new IllegalArgumentException("segmentCount 必须为正数: " + segmentCount);
        }
        this.locks = new Object[segmentCount];
        for (int i = 0; i < segmentCount; i++) {
            locks[i] = new Object();
        }
    }

    /**
     * 根据 Key 获取对应的分段锁
     *
     * @param key 任意 String Key
     * @return 该 Key 对应的分段锁对象
     */
    public Object getLock(String key) {
        if (key == null) {
            return locks[0];
        }
        // 注意：Math.abs(Integer.MIN_VALUE) 仍为负数，此处用 |0 替代
        return locks[(key.hashCode() & 0x7FFFFFFF) % locks.length];
    }

    /**
     * 获取分段数
     */
    public int getSegmentCount() {
        return locks.length;
    }
}
