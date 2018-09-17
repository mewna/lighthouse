package com.mewna.lighthouse.service;

import com.mewna.lighthouse.Lighthouse;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.consul.*;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author amy
 * @since 9/14/18.
 */
@Accessors(fluent = true, chain = true)
@SuppressWarnings("unused")
@RequiredArgsConstructor
public class ConsulService implements LighthouseService {
    private static final String CONSUL_SHARDING_LOCK = "lighthouse-consul-sharding-lock";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final UUID id = UUID.randomUUID();
    private final Lighthouse lighthouse;
    private ConsulClient client;
    @Getter
    private int shardId = -1;
    
    @Nonnull
    @Override
    public String id() {
        return id.toString();
    }
    
    @Nonnull
    @Override
    public Future<LighthouseService> init(@Nonnull final String host) {
        final Future<LighthouseService> future = Future.future();
        
        client = ConsulClient.create(lighthouse.vertx(), new ConsulClientOptions().setHost(host));
        future.complete(this);
        
        return future;
    }
    
    @Nonnull
    @Override
    public Future<Void> connect(@Nonnull final BiFunction<Integer, Integer, Boolean> connectCallback) {
        final Future<Void> future = Future.future();
        
        tryDoConnect(future, connectCallback);
        
        return future;
    }
    
    private void tryLock(@Nonnull final Future<Void> future, @Nonnull final Runnable workedCallback,
                         @Nonnull final Runnable failedCallback) {
        final SessionOptions sessionOpts = new SessionOptions()
                .setName("lighthouse-sharding-lock")
                .setTtl(15_000L)
                .setLockDelay(10L)
                .setBehavior(SessionBehavior.DELETE);
        client.createSessionWithOptions(sessionOpts, sessionRes -> {
            if(sessionRes.succeeded()) {
                final String session = sessionRes.result();
                logger.info("Attempting Consul lock acquisition...");
                final KeyValueOptions lockOpts = new KeyValueOptions()
                        //.setCasIndex(0L)
                        .setAcquireSession(session);
                client.putValueWithOptions(CONSUL_SHARDING_LOCK, id(), lockOpts, lockRes -> {
                    // Returns `true` if we acquire the lock, `false` otherwise.
                    if(lockRes.succeeded() && lockRes.result()) {
                        logger.info("Acquired consul lock!");
                        workedCallback.run();
                    } else {
                        logger.error("== Failed consul lock acquisition (success={} lock={})",
                                lockRes.succeeded(), lockRes.result());
                        failedCallback.run();
                    }
                });
            } else {
                future.fail("Couldn't acquire consul session");
            }
        });
    }
    
    private void tryDoConnect(@Nonnull final Future<Void> future,
                              @Nonnull final BiFunction<Integer, Integer, Boolean> connectCallback) {
        logger.info("== Starting connect...");
        getKnownServiceCount().setHandler(countRes -> {
            if(countRes.succeeded()) {
                final int serviceCount = countRes.result();
                if(serviceCount < lighthouse.shardCount()) {
                    logger.warn("== Not enough nodes to start sharding ({} < {}), queueing retry...", serviceCount, lighthouse.shardCount());
                    queueRetry(future, connectCallback);
                } else {
                    tryLock(future, () -> {
                        // Worked
                        // We have a lock, start shard
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
                                        final boolean didResume = connectCallback.apply(id, shardCount);
                                        if(didResume) {
                                            // Unlock immediately
                                            unlock(future);
                                        } else {
                                            // Unlock later
                                            lighthouse.vertx().setTimer(5_500L, __ -> unlock(future));
                                        }
                                    } else {
                                        logger.error("== Failed shard id acquisition");
                                        // unlockFail(future, "Failed shard id acquisition");
                                        unlock(Future.future());
                                        queueRetry(future, connectCallback);
                                    }
                                }
                            } else {
                                logger.error("== Failed fetching known shards", res.cause());
                                // unlockFail(future, "Couldn't fetch known shards");
                                unlock(Future.future());
                                queueRetry(future, connectCallback);
                            }
                        });
                    }, () -> {
                        logger.error("== Failed locking");
                        queueRetry(future, connectCallback);
                    });
                }
            } else {
                logger.warn("== Couldn't get known service count, queueing retry...");
                queueRetry(future, connectCallback);
            }
        });
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
        return IntStream.range(0, lighthouse.shardCount()).boxed().distinct().collect(Collectors.toSet());
    }
    
    private Future<Set<Integer>> getKnownShards() {
        final Future<Set<Integer>> future = Future.future();
        
        final Future<Collection<JsonObject>> futureIds = lighthouse.pubsub()
                .pubsub(new JsonObject().put("__lighthouse:type", SHARD_ID_QUERY));
        
        futureIds.setHandler(res -> {
            if(res.succeeded()) {
                logger.info("Got known shard pubsub data: {}", res.result());
                final Set<Integer> shards = res.result().stream().map(e -> e.getInteger("shard"))
                        .filter(e -> e >= 0).collect(Collectors.toSet());
                future.complete(shards);
            } else {
                future.fail(res.cause());
            }
        });
        
        return future;
    }
    
    private void queueRetry(@Nonnull final Future<Void> future,
                            @Nonnull final BiFunction<Integer, Integer, Boolean> connectCallback) {
        lighthouse.vertx().setTimer(2_500L, __ -> tryDoConnect(future, connectCallback));
    }
    
    private void unlock(@Nonnull final Future<Void> future) {
        client.deleteValue(CONSUL_SHARDING_LOCK, unlockRes -> {
            logger.info("== Unlocked consul");
            if(unlockRes.succeeded()) {
                future.complete(null);
            } else {
                future.fail(unlockRes.cause());
            }
        });
    }
    
    @SuppressWarnings("SameParameterValue")
    private void unlockFail(@Nonnull final Future<Void> future, @Nonnull final String reason) {
        client.deleteValue(CONSUL_SHARDING_LOCK, unlockRes -> {
            logger.warn("== Unlocked consul with failure {}", reason);
            if(unlockRes.succeeded()) {
                future.fail(reason);
            } else {
                future.fail(unlockRes.cause());
            }
        });
    }
}
