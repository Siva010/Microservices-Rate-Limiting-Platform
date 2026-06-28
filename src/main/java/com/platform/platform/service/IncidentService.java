package com.platform.platform.service;

import com.platform.platform.model.PlatformAlert;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IncidentService {

    private final PlatformRedisStore store;

    public Mono<PlatformAlert> recordIncident(String routeId,
                                              String tenantId,
                                              String clientId,
                                              String message,
                                              String severity) {
        PlatformAlert alert = PlatformAlert.builder()
                .id(UUID.randomUUID().toString())
                .routeId(routeId)
                .tenantId(tenantId)
                .clientId(clientId)
                .message(message)
                .severity(severity)
                .timestamp(System.currentTimeMillis())
                .build();
        return store.saveAlert(alert);
    }

    public Mono<List<PlatformAlert>> recentAlerts(int limit) {
        return store.findRecentAlerts(limit).collectList();
    }
}