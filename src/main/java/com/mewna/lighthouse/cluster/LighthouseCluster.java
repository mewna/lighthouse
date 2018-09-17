package com.mewna.lighthouse.cluster;

import io.vertx.core.Future;
import io.vertx.redis.RedisOptions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author amy
 * @since 9/16/18.
 */
public interface LighthouseCluster {
    @Nonnull
    String id();
    
    @Nonnull
    Future<LighthouseCluster> init(@Nonnull RedisOptions options, @Nonnegative final int port);
    
    @Nonnull
    Future<List<String>> knownServices();
    
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
}
