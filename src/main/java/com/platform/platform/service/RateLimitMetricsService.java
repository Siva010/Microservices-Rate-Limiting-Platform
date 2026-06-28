package com.platform.platform.service;

import com.platform.platform.model.ClientIdentity;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

@Service
public class RateLimitMetricsService {

    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, Counter> allowedCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> deniedCounters = new ConcurrentHashMap<>();

    public RateLimitMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordAllowed(String routeId, ClientIdentity identity) {
        counter(allowedCounters, "rate_limit_allowed_total", routeId, identity).increment();
    }

    public void recordDenied(String routeId, ClientIdentity identity) {
        counter(deniedCounters, "rate_limit_denied_total", routeId, identity).increment();
    }

    private Counter counter(ConcurrentHashMap<String, Counter> cache,
                            String metricName,
                            String routeId,
                            ClientIdentity identity) {
        String cacheKey = routeId + ":" + identity.tenantId() + ":" + identity.clientId();
        return cache.computeIfAbsent(cacheKey, key -> Counter.builder(metricName)
                .description("Rate limit decisions")
                .tag("route", routeId)
                .tag("tenant", identity.tenantId())
                .tag("identity_type", identity.identityType().name())
                .register(meterRegistry));
    }
}