package com.platform.platform.service;

import com.platform.platform.dto.ServiceRequest;
import com.platform.platform.event.PlatformConfigChangedEvent;
import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ServiceRegistryService {

    private final PlatformRedisStore store;
    private final ApplicationEventPublisher eventPublisher;

    public Mono<ServiceDefinition> register(ServiceRequest request) {
        ServiceDefinition service = ServiceDefinition.builder()
                .id(request.getId())
                .name(request.getName())
                .uri(request.getUri())
                .pathPrefix(request.getPathPrefix())
                .replenishRate(request.getReplenishRate())
                .burstCapacity(request.getBurstCapacity())
                .circuitBreakerEnabled(request.isCircuitBreakerEnabled())
                .enabled(request.isEnabled())
                .build();
        return store.saveService(service);
    }

    public Mono<ServiceDefinition> update(String id, ServiceRequest request) {
        request.setId(id);
        return register(request);
    }

    public Mono<ServiceDefinition> get(String id) {
        return store.findService(id);
    }

    public Mono<List<ServiceDefinition>> list() {
        return store.findAllServices().collectList();
    }

    public Mono<Boolean> delete(String id) {
        return store.deleteService(id);
    }

    @EventListener
    public void onServiceChanged(PlatformConfigChangedEvent event) {
        if ("service".equals(event.entityType())) {
            eventPublisher.publishEvent(new RefreshRoutesEvent(this));
        }
    }
}