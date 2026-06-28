package com.platform.platform.bootstrap;

import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.model.Tenant;
import com.platform.platform.service.PolicyService;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.cloud.gateway.event.RefreshRoutesEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class PlatformDataSeeder implements ApplicationRunner {

    private final PlatformRedisStore store;
    private final PolicyService policyService;
    private final ApplicationEventPublisher eventPublisher;

    @Override
    public void run(ApplicationArguments args) {
        seedIfEmpty().subscribe();
    }

    private Mono<Void> seedIfEmpty() {
        return store.findAllServices().collectList()
                .flatMap(services -> {
                    if (!services.isEmpty()) {
                        return policyService.refreshCache().then(refreshRoutes());
                    }
                    log.info("Seeding default platform data");
                    Tenant defaultTenant = Tenant.builder()
                            .id("default")
                            .name("Default Tenant")
                            .plan("FREE")
                            .defaultReplenishRate(10)
                            .defaultBurstCapacity(20)
                            .enabled(true)
                            .build();

                    ServiceDefinition httpbin = ServiceDefinition.builder()
                            .id("dummy-service")
                            .name("API Demo Service")
                            .uri("http://mock-api:80")
                            .pathPrefix("/api/**")
                            .replenishRate(10)
                            .burstCapacity(20)
                            .circuitBreakerEnabled(true)
                            .enabled(true)
                            .build();

                    ServiceDefinition payments = ServiceDefinition.builder()
                            .id("payments-service")
                            .name("Payments API")
                            .uri("http://mock-api:80")
                            .pathPrefix("/payments/**")
                            .replenishRate(5)
                            .burstCapacity(10)
                            .circuitBreakerEnabled(true)
                            .enabled(true)
                            .build();

                    return store.saveTenant(defaultTenant)
                            .then(store.saveService(httpbin))
                            .then(store.saveService(payments))
                            .then(policyService.refreshCache())
                            .then(refreshRoutes());
                });
    }

    private Mono<Void> refreshRoutes() {
        return Mono.fromRunnable(() -> eventPublisher.publishEvent(new RefreshRoutesEvent(this)));
    }
}