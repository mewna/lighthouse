package com.mewna.lighthouse.pubsub;

import com.mewna.lighthouse.Lighthouse;
import com.mewna.lighthouse.service.LighthouseService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.consul.CheckStatus;
import io.vertx.ext.consul.Service;
import io.vertx.ext.consul.ServiceEntry;
import io.vertx.ext.consul.ServiceEntryList;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author amy
 * @since 9/14/18.
 */
@Accessors(fluent = true, chain = true)
@RequiredArgsConstructor
public class RedisPubsub implements LighthousePubsub {
    private static final String LIGHTHOUSE_REDIS_PUBSUB_CHANNEL = "lighthouse:node:pubsub";
    private static final String PUBSUB_EVENT_ADDRESS = "io.vertx.redis." + LIGHTHOUSE_REDIS_PUBSUB_CHANNEL;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, Future<JsonObject>> pending = new ConcurrentHashMap<>();
    
    private final Lighthouse lighthouse;
    private final Function<JsonObject, JsonObject> messageHandler;
    
    @Getter
    private RedisClient publisher;
    @Getter
    private RedisClient listener;
    
    @Override
    public Future<LighthousePubsub> init(@Nonnull final RedisOptions options) {
        final Future<LighthousePubsub> future = Future.future();
        logger.info(">> Connecting to redis...");
        publisher = RedisClient.create(lighthouse.vertx(), options);
        listener = RedisClient.create(lighthouse.vertx(), options);
        setupPubsub().setHandler(res -> {
            if(res.succeeded()) {
                logger.info("Lighthouse pubsub connected to Redis!");
                future.complete(this);
            } else {
                future.fail(res.cause());
            }
        });
        return future;
    }
    
    private Future<Void> setupPubsub() {
        final Future<Void> future = Future.future();
        listener.subscribe(LIGHTHOUSE_REDIS_PUBSUB_CHANNEL, res -> {
            if(res.succeeded()) {
                lighthouse.vertx().eventBus().<JsonObject>consumer(PUBSUB_EVENT_ADDRESS, msg -> {
                    final JsonObject value = msg.body().getJsonObject("value");
                    final String channel = value.getString("channel");
                    final String message = value.getString("message");
                    if(channel.equalsIgnoreCase(LIGHTHOUSE_REDIS_PUBSUB_CHANNEL)) {
                        final JsonObject payload = new JsonObject(message);
                        final JsonObject data = payload.getJsonObject("d");
                        final String nonce = payload.getString("nonce");
                        final String sender = payload.getString("sender");
                        final String target = payload.getString("target");
                        if(target.equals(lighthouse.service().id())) {
                            // If the target is this service, then we need to handle it
                            // and publish a response
                            if(pending.containsKey(nonce)) {
                                // If we have the nonce, then we were waiting on it for a response
                                // and can resolve the future now
                                logger.debug("[Service] [{}] Completed nonce {} with data {}", lighthouse.service().id(),
                                        nonce, data.encodePrettily());
                                final Future<JsonObject> pendingFuture = pending.remove(nonce);
                                pendingFuture.complete(data);
                            } else {
                                // Note that we handle some messages ourselves, and will only call the
                                // message handler callback if it ISN'T an internal message
                                final String maybeType = data.getString("__lighthouse:type", null);
                                final JsonObject response;
                                if(LighthouseService.SHARD_ID_QUERY.equals(maybeType)) {
                                    // Fetch shard id
                                    response = new JsonObject().put("id", lighthouse.service().id())
                                            .put("shard", lighthouse.service().shardId());
                                } else {
                                    logger.debug("[Service] [{}] Invoking user-provided messagehandler for message: {}",
                                            lighthouse.service().id(), payload.encodePrettily());
                                    // User-provided handler
                                    response = messageHandler.apply(data);
                                }
                                // We don't need to specify a target service here since the initiator
                                // will accept it via nonce, not service id
                                publish(payload(nonce, lighthouse.service().id(), sender, response));
                            }
                        }
                    }
                });
                future.complete(null);
            } else {
                future.fail(res.cause());
            }
        });
        return future;
    }
    
    @Override
    public Future<Collection<JsonObject>> pubsub(@Nonnull final JsonObject payload) {
        final Future<Collection<JsonObject>> future = Future.future();
        
        lighthouse.service().getAllServices().setHandler(res -> {
            if(res.succeeded()) {
                final ServiceEntryList list = res.result();
                final List<Future<JsonObject>> futures = new ArrayList<>();
                list.getList().stream().filter(e -> e.getChecks().stream()
                        .allMatch(c -> c.getStatus() == CheckStatus.PASSING))
                        .map(ServiceEntry::getService)
                        .map(Service::getId)
                        // Ignore self
                        .filter(e -> !lighthouse.service().id().equals(e))
                        .forEach(e -> {
                            // For each service, generate a nonce and publish
                            final Future<JsonObject> pubsubFuture = Future.future();
                            final String nonce = UUID.randomUUID().toString();
                            logger.debug("[Service] [{}] Sending to service {} with nonce {}",
                                    lighthouse.service().id(), e, nonce);
                            pending.put(nonce, pubsubFuture);
                            futures.add(pubsubFuture);
                            lighthouse.vertx().setTimer(5_000L, __ -> {
                                // Fail futures if they're not completed within 5s
                                // Basically just trying to avoid leaking memory if the futures
                                // don't get completed for whatever reason
                                if(!pubsubFuture.isComplete()) {
                                    pubsubFuture.fail("Timeout (5000ms)");
                                }
                            });
                            publish(payload(nonce, lighthouse.service().id(), e, payload));
                        });
                // This is so fucking bad omfg
                // CompositeFuture is stupid and takes a List<Future>
                // BUT THIS COMPLETELY IGNORES THE FACT THAT FUTURES CAN HAVE A TYPE PARAMETER
                // This leads to this dumbass fix
                // Since it doesn't take a type parameter, we can't directly pass the futures list
                // So we resolve it by converting to an array and wrapping it in a list to avoid the type parameter memes
                //
                // This is apparently due to the fact that their code generator does not understand wildcards:
                // https://github.com/eclipse-vertx/vert.x/issues/2627#issuecomment-421537706
                // /shrug
                CompositeFuture.all(Arrays.asList(futures.toArray(new Future[0]))).setHandler(pubsubResolve -> {
                    if(pubsubResolve.succeeded()) {
                        // Also CompositeFuture is gay and won't return, like, the data inside the futures
                        // yeah idk either
                        // That's why we store them in a list and resolve them here
                        final List<JsonObject> output = new ArrayList<>();
                        futures.forEach(e -> output.add(e.result()));
                        future.complete(output);
                    } else {
                        future.fail(pubsubResolve.cause());
                    }
                });
            } else {
                future.fail(res.cause());
            }
        });
        
        return future;
    }
    
    @SuppressWarnings("UnusedReturnValue")
    private Future<Long> publish(final JsonObject payload) {
        final Future<Long> future = Future.future();
        publisher.publish(LIGHTHOUSE_REDIS_PUBSUB_CHANNEL, payload.encode(), res -> {
            if(res.succeeded()) {
                future.complete(res.result());
            } else {
                future.fail(res.cause());
            }
        });
        return future;
    }
}
