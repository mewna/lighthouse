package com.mewna.lighthouse.service;

import io.vertx.core.Future;
import io.vertx.ext.consul.ServiceEntryList;

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
     * @return The ID of this node's Consul service.
     */
    @Nonnull
    String id();
    
    /**
     * @return This node's shard id. Will never be negative. May be {@code 0}.
     */
    @Nonnegative
    int shardId();
    
    /**
     * Start this Consul service instance.
     *
     * @param host The hostname of the Consul server.
     *
     * @return A future that resolves when the Consul service is fully started.
     */
    @Nonnull
    Future<LighthouseService> init(@Nonnull String host);
    
    /**
     * @param connectCallback The callback to be invoked when a shard id has
     *                        been acquired.
     *
     * @return A future that resolves when the shard has been connected.
     */
    @Nonnull
    Future<Void> connect(@Nonnull BiFunction<Integer, Integer, Boolean> connectCallback);
    
    /**
     * @return A list of all known services and the corresponding healthcheck
     * statuses.
     */
    @Nonnull
    Future<ServiceEntryList> getAllServices();
}
