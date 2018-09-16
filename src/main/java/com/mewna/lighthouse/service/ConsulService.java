package com.mewna.lighthouse.service;

import com.mewna.lighthouse.Lighthouse;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.consul.*;
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
 * @since 9/14/18.
 */
@Accessors(fluent = true, chain = true)
@SuppressWarnings("unused")
@RequiredArgsConstructor
public class ConsulService implements LighthouseService {
    private static final String CONSUL_SERVICE_NAME = "lighthouse-consul-node";
    private static final String CONSUL_SHARDING_LOCK = "lighthouse-consul-sharding-lock";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final UUID id = UUID.randomUUID();
    private final Lighthouse lighthouse;
    private final int healthcheckPort;
    @SuppressWarnings("FieldCanBeLocal")
    private HttpServer server;
    private ConsulClient client;
    @Getter
    private int shardId = -1;
    
    private final Thread shutdownHook = new Thread(() -> unlock(Future.future()));
    
    @Nonnull
    @Override
    public String id() {
        return id.toString();
    }
    
    @Nonnull
    @Override
    public Future<LighthouseService> init(@Nonnull final String host) {
        final Future<LighthouseService> future = Future.future();
        
        final Future<Void> serverFuture = Future.future();
        final Future<Void> serviceFuture = Future.future();
        final Future<Void> checkFuture = Future.future();
        
        client = ConsulClient.create(lighthouse.vertx(), new ConsulClientOptions().setHost(host));
        
        server = lighthouse.vertx().createHttpServer(new HttpServerOptions().setPort(healthcheckPort))
                .requestHandler(req -> req.response().setStatusCode(200).end("OK")).listen(res -> {
                    if(res.succeeded()) {
                        logger.info("Healthcheck server listening on port {}", healthcheckPort);
                        serverFuture.complete(null);
                    } else {
                        serverFuture.fail(res.cause());
                    }
                });
        
        final ServiceOptions serviceOptions = new ServiceOptions()
                .setName(CONSUL_SERVICE_NAME)
                .setId(id());
        client.registerService(serviceOptions, res -> {
            if(res.succeeded()) {
                logger.info("Successfully registered {} id {}", CONSUL_SERVICE_NAME, id());
                serviceFuture.complete(null);
            } else {
                logger.error("Couldn't register service in consul!", res.cause());
                serviceFuture.fail(res.cause());
            }
        });
        
        final CheckOptions checkOptions = new CheckOptions()
                .setId(id())
                .setName(id() + " healthcheck")
                .setHttp("http://" + LighthouseService.getIp() + ':' + healthcheckPort)
                .setServiceId(id())
                .setStatus(CheckStatus.PASSING)
                .setInterval("500ms")
                .setDeregisterAfter("10s");
        client.registerCheck(checkOptions, res -> {
            if(res.succeeded()) {
                logger.info("Healthchecks registered for {} id {}", CONSUL_SERVICE_NAME, id());
                checkFuture.complete(null);
            } else {
                logger.error("Couldn't register service healthchecks in consul!", res.cause());
                checkFuture.fail(res.cause());
            }
        });
        
        CompositeFuture.all(serverFuture, serviceFuture, checkFuture).setHandler(res -> {
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
    public Future<Void> connect(@Nonnull final BiFunction<Integer, Integer, Boolean> connectCallback) {
        final Future<Void> future = Future.future();
        
        tryLock(future, connectCallback);
        
        return future;
    }
    
    private Future<Integer> getKnownServiceCount() {
        final Future<Integer> future = Future.future();
        
        getAllServices().setHandler(res -> {
            if(res.succeeded()) {
                future.complete(res.result().getList().size());
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
                logger.info("Got pubsub data: {}", res.result());
                final Set<Integer> shards = res.result().stream().map(e -> e.getInteger("shard"))
                        .filter(e -> e >= 0).collect(Collectors.toSet());
                future.complete(shards);
            } else {
                future.fail(res.cause());
            }
        });
        
        return future;
    }
    
    private void tryLock(@Nonnull final Future<Void> future,
                         @Nonnull final BiFunction<Integer, Integer, Boolean> connectCallback) {
        logger.info("== Starting connect...");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        getKnownServiceCount().setHandler(countRes -> {
            if(countRes.succeeded()) {
                final int serviceCount = countRes.result();
                if(serviceCount < lighthouse.shardCount()) {
                    logger.warn("== Not enough nodes to start sharding ({} < {}), queueing retry...", serviceCount, lighthouse.shardCount());
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                    queueRetry(future, connectCallback);
                } else {
                    logger.info("Attempting Consul lock acquisition...");
                    final KeyValueOptions lockOpts = new KeyValueOptions().setCasIndex(0L).setReleaseSession("");
                    client.putValueWithOptions(CONSUL_SHARDING_LOCK, id(), lockOpts, lockRes -> {
                        // Returns `true` if we acquire the lock, `false` otherwise.
                        if(lockRes.succeeded() && lockRes.result()) {
                            logger.info("Acquired consul lock!");
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
                                    } else {
                                        // We have some IDs available, just grab the first one and run with it
                                        final Optional<Integer> maybeId = allIds.stream().limit(1).findFirst();
                                        if(maybeId.isPresent()) {
                                            final int id = maybeId.get();
                                            shardId = id;
                                            final boolean didResume = connectCallback.apply(id, shardCount);
                                            if(didResume) {
                                                // Unlock immediately
                                                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                                                unlock(future);
                                            } else {
                                                // Unlock later
                                                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                                                lighthouse.vertx().setTimer(5_500L, __ -> unlock(future));
                                            }
                                        } else {
                                            Runtime.getRuntime().removeShutdownHook(shutdownHook);
                                            logger.error("== Failed shard id acquisition");
                                            unlockFail(future, "Failed shard id acquisition");
                                        }
                                    }
                                } else {
                                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                                    logger.error("== Failed fetching known shards");
                                    unlockFail(future, "Couldn't fetch known shards");
                                }
                            });
                        } else {
                            Runtime.getRuntime().removeShutdownHook(shutdownHook);
                            logger.error("== Failed consul lock acquisition (success={} lock={}), requeueing...",
                                    lockRes.succeeded(), lockRes.result());
                            queueRetry(future, connectCallback);
                        }
                    });
                }
            } else {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                logger.warn("== Couldn't get known service count, queueing retry...");
                queueRetry(future, connectCallback);
            }
        });
    }
    
    private void queueRetry(@Nonnull final Future<Void> future,
                            @Nonnull final BiFunction<Integer, Integer, Boolean> connectCallback) {
        lighthouse.vertx().setTimer(1_000L, __ -> tryLock(future, connectCallback));
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
    
    @Nonnull
    @Override
    public Future<ServiceEntryList> getAllServices() {
        final Future<ServiceEntryList> future = Future.future();
        client.healthServiceNodes(CONSUL_SERVICE_NAME, true, res -> {
            if(res.succeeded()) {
                future.complete(res.result());
            } else {
                future.fail(res.cause());
            }
        });
        return future;
    }
}