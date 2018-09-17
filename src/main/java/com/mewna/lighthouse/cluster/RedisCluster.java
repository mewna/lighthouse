package com.mewna.lighthouse.cluster;

import com.mewna.lighthouse.Lighthouse;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * @author amy
 * @since 9/16/18.
 */
@Accessors(fluent = true, chain = true)
@SuppressWarnings("unused")
@RequiredArgsConstructor
public class RedisCluster implements LighthouseCluster {
    private static final String LIGHTHOUSE_CLUSTER_KEY = "lighthouse:cluster:discovery";
    
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final Lighthouse lighthouse;
    
    @Getter
    private RedisClient redis;
    
    @Getter
    private HttpServer pingServer;
    
    @Getter
    private WebClient client;
    
    @Getter
    private int port;
    
    @Nonnull
    @Override
    public String id() {
        return lighthouse.service().id();
    }
    
    @Nonnull
    @Override
    public Future<LighthouseCluster> init(@Nonnull final RedisOptions options, @Nonnegative final int port) {
        final Future<LighthouseCluster> future = Future.future();
        this.port = port;
        
        redis = RedisClient.create(lighthouse.vertx(), options);
        client = WebClient.create(lighthouse.vertx());
        pingServer = lighthouse.vertx().createHttpServer(new HttpServerOptions().setPort(port))
                .requestHandler(req -> {
                    if(req.path().equalsIgnoreCase("/")) {
                        req.response().setStatusCode(200).end("OK");
                    }
                });
        
        pingServer.listen(res -> {
            if(res.succeeded()) {
                future.complete(this);
            } else {
                future.fail(res.cause());
            }
        });
        
        lighthouse.vertx().setPeriodic(1_000L, __ -> updateRegistry());
        
        return future;
    }
    
    @Nonnull
    @Override
    public Future<List<String>> knownServices() {
        final Future<List<String>> future = Future.future();
        
        redis.hgetall(LIGHTHOUSE_CLUSTER_KEY, res -> {
            if(res.succeeded()) {
                final JsonObject result = res.result();
                future.complete(new ArrayList<>(result.getMap().keySet()));
            } else {
                future.fail(res.cause());
            }
        });
        
        return future;
    }
    
    private void updateRegistry() {
        redis.hset(LIGHTHOUSE_CLUSTER_KEY, id(), LighthouseCluster.getIp() + ':' + port, res -> {
            if(res.succeeded()) {
                logger.debug("Updated self in Redis cluster discovery~");
                pingServices();
            } else {
                logger.warn("Couldn't update service discovery info!", res.cause());
            }
        });
    }
    
    private void pingServices() {
        // Ping all known services and remove bad ones from the cluster
        // If they're still alive, they'll get unregistered
        redis.hgetall(LIGHTHOUSE_CLUSTER_KEY, query -> {
            if(query.succeeded()) {
                final JsonObject entries = query.result();
                entries.getMap().forEach((k, v) -> {
                    final String ip = (String) v;
                    client.getAbs("http://" + ip).send(res -> {
                        if(res.succeeded()) {
                            logger.debug("Successfully pinged {} @ {}.", k, ip);
                        } else {
                            logger.warn("Couldn't ping {} @ {}!", k, ip, res.cause());
                            // Don't care too much about if it works or not, since ex. race conditions
                            // around deleting it may cause failures that don't really matter
                            redis.hdel(LIGHTHOUSE_CLUSTER_KEY, k, __ -> {
                            });
                        }
                    });
                });
            } else {
                logger.warn("Couldn't fetch services to ping!", query.cause());
            }
        });
    }
}
