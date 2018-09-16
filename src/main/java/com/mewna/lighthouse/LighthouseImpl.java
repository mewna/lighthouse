package com.mewna.lighthouse;

import com.mewna.lighthouse.pubsub.LighthousePubsub;
import com.mewna.lighthouse.pubsub.RedisPubsub;
import com.mewna.lighthouse.service.ConsulService;
import com.mewna.lighthouse.service.LighthouseService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author amy
 * @since 9/13/18.
 */
@SuppressWarnings("FieldCanBeLocal")
@Accessors(fluent = true, chain = true)
@RequiredArgsConstructor
public final class LighthouseImpl implements Lighthouse {
    @Getter
    private final int shardCount;
    private final String consulHost;
    private final int healthcheckPort;
    private final String redisHost;
    private final String redisAuth;
    @Getter
    private final BiFunction<Integer, Integer, Boolean> bootCallback;
    @Getter
    private final Function<JsonObject, JsonObject> messageHandler;
    @Getter
    private final Vertx vertx = Vertx.vertx();
    @Getter
    private LighthousePubsub pubsub;
    @Getter
    private LighthouseService service;
    
    @Nonnull
    @Override
    public Future<Lighthouse> init() {
        service = new ConsulService(this, healthcheckPort);
        pubsub = new RedisPubsub(this, messageHandler);
        
        final Future<Lighthouse> future = Future.future();
        
        CompositeFuture.all(Arrays.asList(
                service.init(consulHost),
                pubsub.init(new RedisOptions().setHost(redisHost).setAuth(redisAuth))
        )).setHandler(res -> {
            if(res.succeeded()) {
                future.complete(this);
            } else {
                future.fail(res.cause());
            }
        });
        return future;
    }
    
    @Nonnull
    @Override
    public Future<Void> startShard() {
        return service.connect(bootCallback);
    }
}
