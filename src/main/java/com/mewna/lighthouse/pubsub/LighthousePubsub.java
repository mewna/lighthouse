package com.mewna.lighthouse.pubsub;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisOptions;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * @author amy
 * @since 9/14/18.
 */
public interface LighthousePubsub {
    /**
     * Start the pubsub instance
     *
     * @param options Options for connecting to Redis
     *
     * @return A future that resolves when the pubsub client is ready
     */
    Future<LighthousePubsub> init(@Nonnull RedisOptions options);
    
    /**
     * Send a pubsub message to all connected services
     *
     * @param payload The message to send
     *
     * @return A future that resolves when all service nodes have responded to
     * the pubsub message.
     */
    Future<Collection<JsonObject>> pubsub(@Nonnull JsonObject payload);
    
    default JsonObject payload(@Nonnull final String nonce, @Nonnull final String sender, @Nonnull final String target,
                               @Nonnull final String mode, @Nonnull final JsonObject data) {
        return new JsonObject()
                .put("nonce", nonce)
                .put("sender", sender)
                .put("target", target)
                .put("mode", mode)
                .put("d", data);
    }
}
