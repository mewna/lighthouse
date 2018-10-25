package com.mewna.lighthouse;

import com.mewna.lighthouse.cluster.LighthouseCluster;
import com.mewna.lighthouse.pubsub.LighthousePubsub;
import com.mewna.lighthouse.service.LighthouseService;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author amy
 * @since 9/13/18.
 */
@SuppressWarnings("unused")
public interface Lighthouse {
    /**
     * Create a new lighthouse instance and get started!
     *
     * @param shardCount      The number of shards for this lighthouse
     *                        instance.
     * @param healthcheckPort The port to run the healthcheck server on. In
     *                        production, this should probably be {@code 80}.
     * @param redisHost       The hostname of the Redis server to use.
     * @param redisAuth       The password used to access the Redis server. Not
     *                        optional.
     * @param bootCallback    The callback used when booting the shard for this
     *                        node.
     * @param messageHandler  The callback used when receiving non-internal
     *                        pubsub messages.
     *
     * @return A new lighthouse instance.
     */
    static Lighthouse lighthouse(@Nonnegative final int shardCount,
                                 @Nonnegative final int healthcheckPort, @Nonnull final String redisHost,
                                 @Nonnull final String redisAuth,
                                 @Nonnull final BiFunction<Integer, Integer, Future<Boolean>> bootCallback,
                                 @Nonnull final Function<JsonObject, JsonObject> messageHandler) {
        return new LighthouseImpl(shardCount, healthcheckPort, redisHost, redisAuth, bootCallback,
                messageHandler);
    }
    
    /**
     * The vert.x instance being used for this lighthouse instance. If you're
     * using something else that relies on vert.x (ex.
     * <a href="https://github.com/mewna/catnip">catnip</a>), you can use this
     * to pass down the vert.x instance instead of creating multiple.
     *
     * @return A vert.x instance. Will not be null.
     */
    @Nonnull
    Vertx vertx();
    
    /**
     * Start the lighthouse instance. Will set up pubsub, redis, etc.
     *
     * @return A {@link Future} that is resolved once the lighthouse components
     * are fully started.
     */
    @Nonnull
    Future<Lighthouse> init();
    
    /**
     * Start the shard for this lighthouse instance.
     *
     * @return A future that resolves when a shard id and total are acquired.
     */
    @Nonnull
    Future<Void> startShard();
    
    /**
     * The pubsub client used by this lighthouse instance. Used for fetching
     * shard ids. May also be useful for eg. distributed eval commands.
     *
     * @return The pubsub client for this instance.
     */
    @Nonnull
    LighthousePubsub pubsub();
    
    /**
     * The Redis service used by this lighthouse instance. You
     * <strong>probably</strong> don't want to use this yourself; it's mainly
     * exposed for internal usage. If you need eg. distributed locking, you may
     * find it slightly useful.
     *
     * @return The Redis service for this instance.
     */
    @Nonnull
    LighthouseService service();
    
    /**
     * The cluster handler for this lighthouse instance. You probably don't
     * want to be touching this yourself.
     *
     * @return The cluster handler for this instance.
     */
    @Nonnull
    LighthouseCluster cluster();
    
    /**
     * The shard count being used by this lighthouse instance. Will never be
     * negative. Should never equal zero.
     *
     * @return The shard count for this lighthouse instance.
     */
    @Nonnegative
    int shardCount();
    
    /**
     * The callback that's invoked when it's time to actually boot a shard. You
     * probably don't want to call this method ever.
     *
     * @return The shard boot callback.
     */
    @Nonnull
    BiFunction<Integer, Integer, Future<Boolean>> bootCallback();
    
    /**
     * Release the current shard ID.
     */
    void release();
}
