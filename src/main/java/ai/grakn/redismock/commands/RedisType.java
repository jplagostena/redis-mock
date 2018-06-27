package ai.grakn.redismock.commands;

/**
 * Types of Redis elements
 *
 * The different types that can be returned are: string, list, set, zset and hash.
 *
 * https://redis.io/commands/type
 *
 */
public enum RedisType {
    STRING,
    LIST,
    SET,
    ZSET,
    HASH,
    NONE;

    public String getName() {
        return this.name().toLowerCase();
    }
}
