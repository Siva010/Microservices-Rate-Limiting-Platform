package com.platform.filter;

import com.platform.model.RateLimitResult;
import com.platform.platform.model.ClientIdentity;
import com.platform.platform.service.ClientIdentityResolver;
import com.platform.platform.service.PolicyService;
import com.platform.platform.service.RateLimitMetricsService;
import com.platform.platform.service.UsageTrackingService;
import com.platform.service.RateLimiterService;
import lombok.Data;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@Component
public class CustomRateLimiterGatewayFilterFactory
        extends AbstractGatewayFilterFactory<CustomRateLimiterGatewayFilterFactory.Config> {

    private final RateLimiterService rateLimiterService;
    private final ClientIdentityResolver clientIdentityResolver;
    private final PolicyService policyService;
    private final RateLimitMetricsService metricsService;
    private final UsageTrackingService usageTrackingService;

    public CustomRateLimiterGatewayFilterFactory(RateLimiterService rateLimiterService,
                                                 ClientIdentityResolver clientIdentityResolver,
                                                 PolicyService policyService,
                                                 RateLimitMetricsService metricsService,
                                                 UsageTrackingService usageTrackingService) {
        super(Config.class);
        this.rateLimiterService = rateLimiterService;
        this.clientIdentityResolver = clientIdentityResolver;
        this.policyService = policyService;
        this.metricsService = metricsService;
        this.usageTrackingService = usageTrackingService;
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return Collections.emptyList();
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
            String routeId = route != null ? route.getId() : "unknown_route";
            final String resolvedRouteId = routeId;

            return clientIdentityResolver.resolve(exchange.getRequest())
                    .flatMap(identity -> policyService.resolvePolicy(resolvedRouteId, identity.tenantId())
                            .flatMap(policy -> rateLimiterService.checkRateLimit(
                                    identity.rateLimitKey(resolvedRouteId),
                                    policy.replenishRate(),
                                    policy.burstCapacity(),
                                    resolvedRouteId,
                                    identity.tenantId(),
                                    identity.clientId())
                                    .doOnNext(result -> recordOutcome(resolvedRouteId, identity, result))
                                    .flatMap(result -> handleResult(exchange, chain, result, policy.source()))));
        };
    }

    private void recordOutcome(String routeId, ClientIdentity identity, RateLimitResult result) {
        if (result.allowed()) {
            metricsService.recordAllowed(routeId, identity);
            usageTrackingService.recordAllowed(identity.tenantId(), identity.clientId()).subscribe();
        } else {
            metricsService.recordDenied(routeId, identity);
            usageTrackingService.recordDenied(identity.tenantId(), identity.clientId()).subscribe();
        }
    }

    private Mono<Void> handleResult(org.springframework.web.server.ServerWebExchange exchange,
                                    org.springframework.cloud.gateway.filter.GatewayFilterChain chain,
                                    RateLimitResult result,
                                    String policySource) {
        applyRateLimitHeaders(exchange.getResponse().getHeaders(), result, policySource);
        if (result.allowed()) {
            return chain.filter(exchange);
        }
        return writeTooManyRequestsResponse(exchange, result);
    }

    private void applyRateLimitHeaders(org.springframework.http.HttpHeaders headers,
                                       RateLimitResult result,
                                       String policySource) {
        headers.set("X-RateLimit-Limit", String.valueOf(result.burstCapacity()));
        headers.set("X-RateLimit-Remaining", String.valueOf(result.remainingTokens()));
        headers.set("X-RateLimit-Policy", policySource);
        if (!result.allowed()) {
            headers.set("Retry-After", String.valueOf(result.retryAfterSeconds()));
        }
    }

    private Mono<Void> writeTooManyRequestsResponse(
            org.springframework.web.server.ServerWebExchange exchange, RateLimitResult result) {
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        String body = String.format(
                "{\"error\":\"Too Many Requests\",\"message\":\"Rate limit exceeded. Retry after %d seconds.\"}",
                result.retryAfterSeconds());
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    @Data
    public static class Config {
    }
}