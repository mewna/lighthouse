package com.mewna.lighthouse.service;

import io.vertx.core.Future;
import io.vertx.redis.RedisOptions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.function.BiFunction;

/**
 * @author amy
 * @since 9/14/18.
 */
public interface LighthouseService {
    String SHARD_ID_QUERY = "lighthouse:query:shard-id";
    
    @Nonnull
    static String getIp() {
        final String podIpEnv = System.getenv("POD_IP");
        if(podIpEnv != null) {
            return podIpEnv;
        }
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch(final UnknownHostException e) {
            throw new IllegalStateException("DNS broken? Can't resolve localhost!", e);
        }
    }
    
    /**
     * @return The ID of this node's service.
     */
    @Nonnull
    String id();
    
    /**
     * @return This node's shard id. May be {@code -1}.
     */
    int shardId();
    
    /**
     * Start this Redis service instance.
     *
     * @param options The redis connection options.
     *
     * @return A future that resolves when the Redis service is fully started.
     */
    @Nonnull
    Future<LighthouseService> init(@Nonnull RedisOptions options);
    
    /**
     * @param connectCallback The callback to be invoked when a shard id has
     *                        been acquired.
     *
     * @return A future that resolves when the shard has been connected.
     */
    @Nonnull
    Future<Void> connect(@Nonnull BiFunction<Integer, Integer, Future<Boolean>> connectCallback);
}
