package com.mewna.lighthouse;

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
    static Lighthouse lighthouse(@Nonnegative final int shardCount, @Nonnull final String consulHost,
                                 @Nonnegative final int healthcheckPort, @Nonnull final String redisHost,
                                 @Nonnull final String redisAuth,
                                 @Nonnull final BiFunction<Integer, Integer, Boolean> bootCallback,
                                 @Nonnull final Function<JsonObject, JsonObject> messageHandler) {
        return new LighthouseImpl(shardCount, consulHost, healthcheckPort, redisHost, redisAuth, bootCallback,
                messageHandler);
    }
    
    @Nonnull
    Vertx vertx();
    
    @Nonnull
    Future<Lighthouse> init();
    
    @Nonnull
    Future<Void> startShard();
    
    @Nonnull
    LighthousePubsub pubsub();
    
    @Nonnull
    LighthouseService service();
    
    @Nonnegative
    int shardCount();
    
    @Nonnull
    BiFunction<Integer, Integer, Boolean> bootCallback();
}
