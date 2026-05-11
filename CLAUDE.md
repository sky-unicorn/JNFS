# JNFS Project Rules

## Database MCP Usage

When using database MCP tools, **only use `jnfs-db` MCP**. Do not use `anal-business-db`, `anal-system-db`, or `gridfoundation-db` unless explicitly instructed.

## Storage Compatibility

The system supports two storage modes: **file** and **mysql**. When writing or modifying any code that involves data storage (reading, writing, querying, deleting), you **must ensure compatibility with both modes**, not just one. This includes:

- All data access logic must work correctly under both `file` and `mysql` storage modes.
- Do not implement or test against only one mode and assume the other works.
- When changing storage-related code, verify the change is valid for both modes.
