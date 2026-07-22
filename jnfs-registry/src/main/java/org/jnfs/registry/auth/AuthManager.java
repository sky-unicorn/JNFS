package org.jnfs.registry.auth;

import cn.hutool.crypto.digest.BCrypt;
import org.jnfs.common.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Dashboard 鉴权管理器
 * <p>
 * 职责：
 * - 登录校验（BCrypt.checkpw）
 * - 内存 session 管理（ConcurrentHashMap + 随机 token，重启失效）
 * - 登出、改密（改密后该用户所有 session 失效）
 * - 登录失败限流（连续失败达上限后锁定一段时间，带锁定截止时间戳，窗口过后自动解锁）
 * - 过期 session 定时清理（守护线程）
 */
public class AuthManager {

    private static final Logger LOG = LoggerFactory.getLogger(AuthManager.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String SESSION_COOKIE = "JNFS_SESSION";

    /** 同一用户名连续登录失败达到此阈值后锁定 */
    private static final int MAX_FAILED_ATTEMPTS = 5;
    /** 锁定时长（秒） */
    private static final long LOCKOUT_SECONDS = 300;
    /** 过期 session 清理周期（秒） */
    private static final long CLEAN_INTERVAL_SECONDS = 60;

    private final UserStore userStore;
    private final long sessionTimeoutSeconds;

    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    /** 登录失败记录：username → 失败信息 */
    private final Map<String, FailedInfo> failedAttempts = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleaner;

    public AuthManager(UserStore userStore, long sessionTimeoutSeconds) {
        this.userStore = userStore;
        this.sessionTimeoutSeconds = sessionTimeoutSeconds;

        // 守护线程定时清理过期 session（复用 DaemonThreadFactory，禁止手写匿名 ThreadFactory）
        this.cleaner = Executors.newSingleThreadScheduledExecutor(
                new DaemonThreadFactory("Dashboard-SessionCleaner"));
        cleaner.scheduleAtFixedRate(this::cleanupExpiredSessions,
                CLEAN_INTERVAL_SECONDS, CLEAN_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public long getSessionTimeoutSeconds() {
        return sessionTimeoutSeconds;
    }

    public static String getSessionCookieName() {
        return SESSION_COOKIE;
    }

    /**
     * 登录校验，成功返回 session token，失败返回 null
     */
    public String login(String username, String password) {
        if (username == null || password == null || username.isEmpty() || password.isEmpty()) {
            return null;
        }

        // 检查锁定状态
        FailedInfo info = failedAttempts.get(username);
        if (info != null && info.isLocked()) {
            LOG.warn("登录被拒绝：用户 '{}' 已锁定（连续失败 {} 次），请稍后重试", username, info.count);
            return null;
        }

        String storedHash = userStore.findPasswordHash(username);
        if (storedHash == null || !BCrypt.checkpw(password, storedHash)) {
            // 登录失败，累加失败计数
            recordFailedAttempt(username);
            return null;
        }

        // 登录成功，清除失败计数
        failedAttempts.remove(username);

        // 生成 session token
        String token = generateToken();
        sessions.put(token, new Session(username, Instant.now().plusSeconds(sessionTimeoutSeconds)));
        LOG.info("用户 '{}' 登录成功", username);
        return token;
    }

    /**
     * 校验 session token，有效则返回用户名，否则返回 null
     */
    public String validateSession(String token) {
        if (token == null || token.isEmpty()) {
            return null;
        }
        Session session = sessions.get(token);
        if (session == null) {
            return null;
        }
        if (session.isExpired()) {
            sessions.remove(token);
            return null;
        }
        return session.username;
    }

    /**
     * 登出
     */
    public void logout(String token) {
        if (token != null) {
            Session removed = sessions.remove(token);
            if (removed != null) {
                LOG.info("用户 '{}' 登出", removed.username);
            }
        }
    }

    /**
     * 修改密码：校验旧密码 → 更新哈希 → 该用户所有 session 失效
     *
     * @return true=成功
     */
    public boolean changePassword(String username, String oldPassword, String newPassword) {
        if (username == null || oldPassword == null || newPassword == null || newPassword.isEmpty()) {
            return false;
        }
        String storedHash = userStore.findPasswordHash(username);
        if (storedHash == null || !BCrypt.checkpw(oldPassword, storedHash)) {
            LOG.warn("修改密码失败：旧密码错误, username='{}'", username);
            return false;
        }
        String newHash = BCrypt.hashpw(newPassword);
        boolean ok = userStore.updatePassword(username, newHash);
        if (ok) {
            // 使该用户的所有 session 失效，强制重新登录
            sessions.entrySet().removeIf(e -> username.equals(e.getValue().username));
            LOG.info("用户 '{}' 密码已修改，其所有 session 已失效", username);
        }
        return ok;
    }

    /**
     * 释放资源：关闭清理线程与用户存储
     */
    public void shutdown() {
        cleaner.shutdownNow();
        userStore.close();
        LOG.info("AuthManager 已关闭");
    }

    // ==================== 内部方法 ====================

    private void recordFailedAttempt(String username) {
        FailedInfo info = failedAttempts.compute(username, (k, prev) -> {
            int count = (prev == null) ? 1 : prev.count + 1;
            Instant lockedUntil = (count >= MAX_FAILED_ATTEMPTS)
                    ? Instant.now().plusSeconds(LOCKOUT_SECONDS)
                    : null;
            return new FailedInfo(count, lockedUntil);
        });
        if (info.count >= MAX_FAILED_ATTEMPTS && info.lockedUntil != null) {
            LOG.warn("用户 '{}' 连续登录失败 {} 次，已锁定 {} 秒", username, info.count, LOCKOUT_SECONDS);
        } else {
            LOG.warn("用户 '{}' 登录失败，累计 {} 次（{} 次后锁定）",
                    username, info.count, MAX_FAILED_ATTEMPTS);
        }
    }

    private String generateToken() {
        byte[] bytes = new byte[32];
        SECURE_RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private void cleanupExpiredSessions() {
        int before = sessions.size();
        sessions.entrySet().removeIf(e -> e.getValue().isExpired());
        int removed = before - sessions.size();
        if (removed > 0) {
            LOG.debug("清理了 {} 个过期 session", removed);
        }
    }

    /** 单个 session */
    private static class Session {
        final String username;
        final Instant expireAt;

        Session(String username, Instant expireAt) {
            this.username = username;
            this.expireAt = expireAt;
        }

        boolean isExpired() {
            return Instant.now().isAfter(expireAt);
        }
    }

    /** 登录失败记录 */
    private static class FailedInfo {
        final int count;
        final Instant lockedUntil;

        FailedInfo(int count, Instant lockedUntil) {
            this.count = count;
            this.lockedUntil = lockedUntil;
        }

        boolean isLocked() {
            return lockedUntil != null && Instant.now().isBefore(lockedUntil);
        }
    }
}
