package org.jnfs.common.migration;

/**
 * 迁移步骤执行结果
 */
public final class MigrationResult {

    private final boolean success;
    private final String message;

    private MigrationResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public static MigrationResult ok() {
        return new MigrationResult(true, "Migration completed successfully");
    }

    public static MigrationResult ok(String message) {
        return new MigrationResult(true, message);
    }

    public static MigrationResult fail(String message) {
        return new MigrationResult(false, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isFailed() {
        return !success;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "MigrationResult{success=" + success + ", message='" + message + "'}";
    }
}
