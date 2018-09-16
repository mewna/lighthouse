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
    Future<LighthousePubsub> init(@Nonnull RedisOptions options);
    
    Future<Collection<JsonObject>> pubsub(@Nonnull JsonObject payload);
    
    default JsonObject payload(@Nonnull final String nonce, @Nonnull final String sender, @Nonnull final String target,
                               @Nonnull final JsonObject data) {
        return new JsonObject()
                .put("nonce", nonce)
                .put("sender", sender)
                .put("target", target)
                .put("d", data);
    }
}
