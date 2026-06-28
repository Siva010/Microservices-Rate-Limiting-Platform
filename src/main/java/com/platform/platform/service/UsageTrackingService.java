package com.platform.platform.service;

import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class UsageTrackingService {

    private final PlatformRedisStore store;

    public Mono<Void> recordAllowed(String tenantId, String clientId) {
        return store.incrementAllowed(tenantId, clientId).then();
    }

    public Mono<Void> recordDenied(String tenantId, String clientId) {
        return store.incrementDenied(tenantId, clientId).then();
    }

    public Mono<Map<String, Long>> getUsage(String tenantId, String clientId) {
        return Mono.zip(
                store.getAllowedCount(tenantId, clientId),
                store.getDeniedCount(tenantId, clientId)
        ).map(tuple -> {
            Map<String, Long> usage = new HashMap<>();
            usage.put("allowed", tuple.getT1());
            usage.put("denied", tuple.getT2());
            return usage;
        });
    }
}