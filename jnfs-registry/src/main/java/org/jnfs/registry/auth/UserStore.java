package org.jnfs.registry.auth;

/**
 * Dashboard 用户存储接口
 * <p>
 * 支持 file 和 mysql 两种存储模式，由 RegistryServer 根据配置选择实现。
 * 密码字段仅存储 BCrypt 哈希（形如 $2a$10$...），绝不存储明文。
 */
public interface UserStore {

    /**
     * 查询用户的 BCrypt 哈希
     *
     * @param username 用户名
     * @return BCrypt 哈希字符串，用户不存在时返回 null
     */
    String findPasswordHash(String username);

    /**
     * 保存用户（首次初始化时调用）
     *
     * @param username   用户名
     * @param bcryptHash BCrypt 哈希（必须以 $2a$ / $2b$ / $2y$ 开头）
     */
    void saveUser(String username, String bcryptHash);

    /**
     * 修改密码（更新哈希）
     *
     * @param username      用户名
     * @param newBcryptHash 新的 BCrypt 哈希
     * @return true=成功
     */
    boolean updatePassword(String username, String newBcryptHash);

    /**
     * 用户总数（用于判断是否需要初始化初始管理员）
     *
     * @return 用户数量
     */
    int userCount();

    /**
     * 关闭资源（如 HikariDataSource）
     */
    void close();
}
