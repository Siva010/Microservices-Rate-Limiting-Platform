package com.platform.platform.service;

import com.platform.platform.event.PlatformConfigChangedEvent;
import com.platform.platform.model.RateLimitPolicy;
import com.platform.platform.model.ResolvedPolicy;
import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.model.Tenant;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PolicyService {

    private final PlatformRedisStore store;

    private volatile Map<String, ServiceDefinition> servicesById = Map.of();
    private volatile Map<String, Tenant> tenantsById = Map.of();
    private volatile List<RateLimitPolicy> policies = List.of();

    public Mono<ResolvedPolicy> resolvePolicy(String routeId, String tenantId) {
        return refreshIfEmpty()
                .doOnError(e -> log.debug("Non-fatal error during policy cache refresh for route {}: {}", routeId, e.getMessage()))
                .onErrorResume(e -> Mono.empty())
                .then(Mono.fromSupplier(() -> resolveFromCache(routeId, tenantId)));
    }

    public Mono<RateLimitPolicy> createPolicy(RateLimitPolicy policy) {
        return validatePolicy(policy).then(Mono.defer(() -> store.savePolicy(policy)));
    }

    public Mono<RateLimitPolicy> updatePolicy(String id, RateLimitPolicy updated) {
        updated.setId(id);
        return validatePolicy(updated).then(Mono.defer(() -> store.savePolicy(updated)));
    }

    public Mono<Boolean> deletePolicy(String id) {
        return store.deletePolicy(id);
    }

    public Mono<RateLimitPolicy> getPolicy(String id) {
        return store.findPolicy(id);
    }

    public Mono<List<RateLimitPolicy>> listPolicies() {
        return store.findAllPolicies().collectList();
    }

    @EventListener
    public void onConfigChanged(PlatformConfigChangedEvent event) {
        if (List.of("tenant", "service", "policy").contains(event.entityType())) {
            refreshCache()
                    .doOnError(e -> log.warn("Async cache refresh on config change failed", e))
                    .onErrorResume(e -> Mono.empty())
                    .subscribe(v -> {}, e -> log.warn("Cache refresh subscription error", e));  // ensure errors logged even in subscribe
        }
    }

    private Mono<Void> refreshIfEmpty() {
        if (servicesById.isEmpty()) {
            return refreshCache();
        }
        return Mono.empty();
    }

    private Mono<Void> validatePolicy(RateLimitPolicy policy) {
        if (policy.getRouteId() == null || policy.getRouteId().isBlank()) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "routeId is required for policy"));
        }
        if (policy.getReplenishRate() <= 0 || policy.getBurstCapacity() <= 0) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "replenishRate and burstCapacity must be positive"));
        }
        return store.findAllServices()
                .filter(s -> s.getId() != null && s.getId().equals(policy.getRouteId()))
                .hasElements()
                .flatMap(hasService -> {
                    if (hasService) {
                        return Mono.empty();
                    }
                    return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                            "No registered service found for routeId: " + policy.getRouteId() +
                            ". Register the service first via /platform/admin/services"));
                });
    }

    public Mono<Void> refreshCache() {
        return Mono.zip(
                store.findAllServices().collectList(),
                store.findAllTenants().collectList(),
                store.findAllPolicies().collectList()
        ).doOnNext(tuple -> {
            servicesById = tuple.getT1().stream()
                    .collect(Collectors.toMap(ServiceDefinition::getId, s -> s, (a, b) -> a, ConcurrentHashMap::new));
            tenantsById = tuple.getT2().stream()
                    .collect(Collectors.toMap(Tenant::getId, t -> t, (a, b) -> a, ConcurrentHashMap::new));
            policies = tuple.getT3();
        })
        .doOnError(e -> log.warn("Failed to refresh policy/service/tenant cache from Redis, keeping previous snapshot", e))
        .onErrorResume(e -> Mono.empty())
        .then();
    }

    private ResolvedPolicy resolveFromCache(String routeId, String tenantId) {
        RateLimitPolicy tenantOverride = policies.stream()
                .filter(p -> p.isEnabled() && routeId.equals(p.getRouteId()))
                .filter(p -> tenantId != null && tenantId.equals(p.getTenantId()))
                .findFirst()
                .orElse(null);

        if (tenantOverride != null) {
            return new ResolvedPolicy(
                    tenantOverride.getReplenishRate(),
                    tenantOverride.getBurstCapacity(),
                    "tenant-policy:" + tenantOverride.getId());
        }

        ServiceDefinition service = servicesById.get(routeId);
        if (service != null && service.isEnabled()) {
            return new ResolvedPolicy(
                    service.getReplenishRate(),
                    service.getBurstCapacity(),
                    "service:" + service.getId());
        }

        Tenant tenant = tenantsById.get(tenantId);
        if (tenant != null && tenant.isEnabled()) {
            return new ResolvedPolicy(
                    tenant.getDefaultReplenishRate(),
                    tenant.getDefaultBurstCapacity(),
                    "tenant-default:" + tenant.getId());
        }

        return new ResolvedPolicy(10, 20, "platform-default");
    }
}