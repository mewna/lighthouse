package com.mewna.lighthouse.service;

import com.mewna.lighthouse.Lighthouse;
import io.vertx.core.Future;
import io.vertx.redis.RedisOptions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author amy
 * @since 9/14/18.
 */
public interface LighthouseService {
    String SHARD_ID_QUERY = "lighthouse:query:shard-id";
    
    /**
     * @return The ID of this node's service.
     */
    @Nonnull
    String id();
    
    /**
     * @return This node's shard id. May be {@code -1}.
     */
    int shardId();
    
    LighthouseService shardId(@Nonnegative int a);
    
    /**
     * Start this Redis service instance.
     *
     * @param options The redis connection options.
     *
     * @return A future that resolves when the Redis service is fully started.
     */
    @Nonnull
    Future<LighthouseService> init(@Nonnull RedisOptions options);
    
    /**
     * @param connectCallback The callback to be invoked when a shard id has
     *                        been acquired.
     *
     * @return A future that resolves when the shard has been connected.
     */
    @Nonnull
    Future<Void> connect(@Nonnull BiFunction<Integer, Integer, Future<Boolean>> connectCallback);
    
    /**
     * Lock for sharding. Successful acquisition of the lock means nothing but
     * the current shard can lock.
     *
     * @return A future that resolves when lock acquisition succeeds or fails.
     */
    @Nonnull
    Future<Boolean> lock();
    
    /**
     * Release the lock from {@link #lock()}. Should only be called if this
     * process is the one that acquired the lock.
     *
     * @return A future that resolves when the lock is released.
     */
    @Nonnull
    Future<Void> unlock();
    
    /**
     * Release the current shard ID. Don't call this directly, use
     * {@link Lighthouse#release()}.
     */
    void release();
    
    @Nonnull
    Set<Integer> getAllShards();
    
    @Nonnull
    Future<Set<Integer>> getKnownShards();
}
