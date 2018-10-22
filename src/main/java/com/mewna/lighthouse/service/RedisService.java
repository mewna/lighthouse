package com.mewna.lighthouse.service;

import com.mewna.lighthouse.Lighthouse;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.op.SetOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author amy
 * @since 10/22/18.
 */
@Accessors(fluent = true)
@RequiredArgsConstructor
public class RedisService implements LighthouseService {
    private static final String LIGHTHOUSE_LOCK_NAME = "lighthouse:sharding:lock";
    
    private final UUID id = UUID.randomUUID();
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Lighthouse lighthouse;
    @Getter
    private int shardId = -1;
    private RedisClient client;
    
    @Nonnull
    @Override
    public String id() {
        return id.toString();
    }
    
    @Nonnull
    @Override
    public Future<LighthouseService> init(@Nonnull final RedisOptions options) {
        final var future = Future.<LighthouseService>future();
        
        client = RedisClient.create(lighthouse.vertx(), options);
        future.complete(this);
        
        return future;
    }
    
    @Nonnull
    @Override
    public Future<Void> connect(@Nonnull final BiFunction<Integer, Integer, Future<Boolean>> connectCallback) {
        final Future<Void> future = Future.future();
        tryConnect(future, connectCallback);
        return future;
    }
    
    private void tryConnect(final Future<Void> future, final BiFunction<Integer, Integer, Future<Boolean>> connectCallback) {
        getKnownServiceCount().setHandler(countRes -> {
            if(countRes.succeeded()) {
                final int serviceCount = countRes.result();
                if(serviceCount < lighthouse.shardCount()) {
                    logger.warn("== Not enough nodes to start sharding ({} < {}), queueing retry...", serviceCount, lighthouse.shardCount());
                    queueRetry(future, connectCallback);
                } else {
                    // Try to lock
                    // Only set if not exists, and expire in 30 seconds
                    client.setWithOptions(LIGHTHOUSE_LOCK_NAME, id(), new SetOptions().setNX(true).setEX(30), lock -> {
                        if(lock.succeeded() && lock.result().equals("OK")) {
                            final int shardCount = lighthouse.shardCount();
                            final Set<Integer> allIds = getAllShards();
                            
                            getKnownShards().setHandler(res -> {
                                if(res.succeeded()) {
                                    final Set<Integer> knownIds = res.result();
                                    logger.info("Acquired known IDs: {}", knownIds);
                                    allIds.removeAll(knownIds);
                                    if(allIds.isEmpty()) {
                                        unlockFail(future, "No IDs left!");
                                        queueRetry(future, connectCallback);
                                    } else {
                                        // We have some IDs available, just grab the first one and run with it
                                        final Optional<Integer> maybeId = allIds.stream().limit(1).findFirst();
                                        if(maybeId.isPresent()) {
                                            final int id = maybeId.get();
                                            shardId = id;
    
                                            connectCallback.apply(id, shardCount).setHandler(shard -> {
                                                if(shard.succeeded()) {
                                                    if(shard.result()) {
                                                        unlock(future);
                                                    } else {
                                                        lighthouse.vertx().setTimer(5_500L, __ -> unlock(future));
                                                    }
                                                } else {
                                                    unlockFail(future, "Shard boot failed");
                                                }
                                            });
                                        } else {
                                            logger.error("== Failed shard id acquisition");
                                            // unlockFail(future, "Failed shard id acquisition");
                                            unlock(Future.future());
                                            queueRetry(future, connectCallback);
                                        }
                                    }
                                } else {
                                    logger.error("== Failed fetching known shards", res.cause());
                                    client.del(LIGHTHOUSE_LOCK_NAME, __ -> {
                                    });
                                    queueRetry(future, connectCallback);
                                }
                            });
                        } else {
                            queueRetry(future, connectCallback);
                        }
                    });
                }
            } else {
                queueRetry(future, connectCallback);
            }
        });
    }
    
    private void unlock(final Future<Void> future) {
        client.del(LIGHTHOUSE_LOCK_NAME, unlockRes -> {
            if(unlockRes.succeeded()) {
                future.complete(null);
            } else {
                future.fail(unlockRes.cause());
            }
        });
    }
    
    @SuppressWarnings("SameParameterValue")
    private void unlockFail(@Nonnull final Future<Void> future, @Nonnull final String reason) {
        client.del(LIGHTHOUSE_LOCK_NAME, unlockRes -> {
            logger.warn("== Unlocked with failure {}", reason);
            if(unlockRes.succeeded()) {
                future.fail(reason);
            } else {
                future.fail(unlockRes.cause());
            }
        });
    }
    
    private void queueRetry(final Future<Void> future, final BiFunction<Integer, Integer, Future<Boolean>> connectCallback) {
        lighthouse.vertx().setTimer(1_000L, __ -> tryConnect(future, connectCallback));
    }
    
    private Future<Integer> getKnownServiceCount() {
        final Future<Integer> future = Future.future();
        
        lighthouse.cluster().knownServices().setHandler(res -> {
            if(res.succeeded()) {
                future.complete(res.result().size());
            } else {
                future.fail(res.cause());
            }
        });
        
        return future;
    }
    
    private Set<Integer> getAllShards() {
        return IntStream.range(0, lighthouse.shardCount()).boxed().collect(Collectors.toSet());
    }
    
    private Future<Set<Integer>> getKnownShards() {
        final Future<Set<Integer>> future = Future.future();
        
        final Future<Collection<JsonObject>> futureIds = lighthouse.pubsub()
                .pubsub(new JsonObject().put("__lighthouse:type", SHARD_ID_QUERY));
        
        futureIds.setHandler(res -> {
            if(res.succeeded()) {
                final Set<Integer> shards = res.result().stream().map(e -> e.getInteger("shard"))
                        .filter(e -> e >= 0).collect(Collectors.toSet());
                future.complete(shards);
            } else {
                future.fail(res.cause());
            }
        });
        
        return future;
    }
}
