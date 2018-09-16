package com.mewna.lighthouse.service;

import io.vertx.core.Future;
import io.vertx.ext.consul.ServiceEntryList;
import io.vertx.ext.consul.ServiceList;
import lombok.*;
import lombok.experimental.Accessors;

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
    String id();
    
    @Nonnegative
    int shardId();
    
    @Nonnull
    Future<LighthouseService> init(@Nonnull String host);
    
    @Nonnull
    Future<Void> connect(@Nonnull BiFunction<Integer, Integer, Boolean> connectCallback);
    
    @Nonnull
    Future<ServiceEntryList> getAllServices();
    
    @Nonnull
    static String getIp() {
        try {
            return Inet4Address.getLocalHost().getHostAddress();
        } catch(final UnknownHostException e) {
            throw new IllegalStateException("DNS broken? Can't resolve localhost!", e);
        }
    }
}
