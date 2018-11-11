package com.mewna.lighthouse.pubsub;

import com.mewna.lighthouse.Lighthouse;
import com.mewna.lighthouse.service.LighthouseService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
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
public class HttpPubsub implements LighthousePubsub {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final Lighthouse lighthouse;
    private final Function<JsonObject, JsonObject> messageHandler;
    
    @Getter
    private WebClient client;
    
    @Override
    public Future<LighthousePubsub> init() {
        final Future<LighthousePubsub> future = Future.future();
        logger.info(">> Setting up pubsub...");
        client = WebClient.create(lighthouse.vertx());
        lighthouse.cluster().registerRoute("/pubsub", this::handleRequest);
        future.complete(this);
        return future;
    }
    
    private void handleRequest(@Nonnull final HttpServerRequest req) {
        final Buffer buffer = Buffer.buffer();
        
        req.handler(buffer::appendBuffer).endHandler(__ -> {
            final String message = buffer.toString();
            final JsonObject payload = new JsonObject(message);
            final JsonObject data = payload.getJsonObject("d");
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
            req.response().setStatusCode(200).end(response.encode());
        });
    }
    
    @Override
    public Future<Collection<JsonObject>> pubsub(@Nonnull final JsonObject payload) {
        final Future<Collection<JsonObject>> future = Future.future();
        
        lighthouse.cluster().knownServiceIps().setHandler(res -> {
            if(res.succeeded()) {
                final Map<String, String> ips = res.result();
                final List<Future<JsonObject>> futures = new ArrayList<>();
                if(ips.size() != lighthouse.shardCount()) {
                    logger.warn("[Service] [{}] Expecting pubsub with {} services, but found {}: {}",
                            lighthouse.service().id(), lighthouse.shardCount(), ips.size(), ips);
                }
                ips.values().forEach(e -> {
                    // For each service, generate a nonce and publish
                    final Future<JsonObject> pubsubFuture = Future.future();
                    final String nonce = UUID.randomUUID().toString();
                    logger.debug("[Service] [{}] Sending to service {} with nonce {}",
                            lighthouse.service().id(), e, nonce);
                    futures.add(pubsubFuture);
                    lighthouse.vertx().setTimer(5_000L, __ -> {
                        // Fail futures if they're not completed within 5s
                        // Basically just trying to avoid leaking memory if the futures
                        // don't get completed for whatever reason
                        if(!pubsubFuture.isComplete()) {
                            pubsubFuture.fail("Timeout (5000ms)");
                        }
                    });
                    
                    client.postAbs("http://" + e + "/pubsub")
                            .sendJsonObject(payload(nonce, payload),
                                    result -> {
                                        if(result.succeeded()) {
                                            pubsubFuture.complete(result.result().bodyAsJsonObject());
                                        } else {
                                            pubsubFuture.fail(result.cause());
                                        }
                                    });
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
}
