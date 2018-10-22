package com.mewna.lighthouse;

import com.mewna.lighthouse.cluster.LighthouseCluster;
import com.mewna.lighthouse.cluster.RedisCluster;
import com.mewna.lighthouse.pubsub.LighthousePubsub;
import com.mewna.lighthouse.pubsub.RedisPubsub;
import com.mewna.lighthouse.service.LighthouseService;
import com.mewna.lighthouse.service.RedisService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
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
    private final int healthcheckPort;
    private final String redisHost;
    private final String redisAuth;
    @Getter
    private final BiFunction<Integer, Integer, Boolean> bootCallback;
    @Getter
    private final Function<JsonObject, JsonObject> messageHandler;
    @Getter
    private final Vertx vertx = Vertx.vertx(new VertxOptions());
    @Getter
    private LighthousePubsub pubsub;
    @Getter
    private LighthouseCluster cluster;
    @Getter
    private LighthouseService service;
    
    @Nonnull
    @Override
    public Future<Lighthouse> init() {
        pubsub = new RedisPubsub(this, messageHandler);
        cluster = new RedisCluster(this);
        service = new RedisService(this);
        
        final Future<Lighthouse> future = Future.future();
        
        CompositeFuture.all(Arrays.asList(
                pubsub.init(new RedisOptions().setHost(redisHost).setAuth(redisAuth)),
                cluster.init(new RedisOptions().setHost(redisHost).setAuth(redisAuth), healthcheckPort),
                service.init(new RedisOptions().setHost(redisHost).setAuth(redisAuth))
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
        // return service.connect(bootCallback);
        throw new UnsupportedOperationException("TODO");
    }
}
