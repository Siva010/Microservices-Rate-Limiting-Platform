package com.platform.platform.service;

import com.platform.platform.model.RateLimitPolicy;
import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.model.Tenant;
import com.platform.platform.store.PlatformRedisStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PolicyServiceTest {

    @Mock
    private PlatformRedisStore store;

    private PolicyService policyService;

    @BeforeEach
    void setUp() {
        policyService = new PolicyService(store);
        lenient().when(store.findAllServices()).thenReturn(Flux.just(
                ServiceDefinition.builder()
                        .id("orders-service")
                        .replenishRate(20)
                        .burstCapacity(40)
                        .enabled(true)
                        .build()
        ));
        lenient().when(store.findAllTenants()).thenReturn(Flux.just(
                Tenant.builder()
                        .id("acme")
                        .defaultReplenishRate(5)
                        .defaultBurstCapacity(10)
                        .enabled(true)
                        .build()
        ));
        lenient().when(store.findAllPolicies()).thenReturn(Flux.just(
                RateLimitPolicy.builder()
                        .id("acme-orders")
                        .routeId("orders-service")
                        .tenantId("acme")
                        .replenishRate(100)
                        .burstCapacity(200)
                        .enabled(true)
                        .build()
        ));
    }

    @Test
    void resolvePolicy_ShouldPreferTenantOverride() {
        policyService.refreshCache().block();
        var policy = policyService.resolvePolicy("orders-service", "acme").block();
        assertThat(policy.replenishRate()).isEqualTo(100);
        assertThat(policy.burstCapacity()).isEqualTo(200);
        assertThat(policy.source()).contains("tenant-policy");
    }

    @Test
    void resolvePolicy_ShouldFallbackToServiceDefault() {
        policyService.refreshCache().block();
        var policy = policyService.resolvePolicy("orders-service", "other").block();
        assertThat(policy.replenishRate()).isEqualTo(20);
        assertThat(policy.source()).contains("service:");
    }

    @Test
    void createPolicy_ShouldRejectUnknownRouteId() {
        // Override to simulate no matching service for this test
        when(store.findAllServices()).thenReturn(Flux.empty());

        RateLimitPolicy bad = RateLimitPolicy.builder()
                .id("bad-policy")
                .routeId("nonexistent-service")
                .tenantId("acme")
                .replenishRate(10)
                .burstCapacity(20)
                .enabled(true)
                .build();

        StepVerifier.create(policyService.createPolicy(bad))
                .expectErrorSatisfies(err -> assertThat(err)
                        .isInstanceOf(org.springframework.web.server.ResponseStatusException.class)
                        .hasMessageContaining("No registered service"))
                .verify();
    }
}