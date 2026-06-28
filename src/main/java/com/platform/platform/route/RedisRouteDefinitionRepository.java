package com.platform.platform.route;

import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.FilterDefinition;
import org.springframework.cloud.gateway.handler.predicate.PredicateDefinition;
import org.springframework.cloud.gateway.route.RouteDefinition;
import org.springframework.cloud.gateway.route.RouteDefinitionRepository;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisRouteDefinitionRepository implements RouteDefinitionRepository {

    private final PlatformRedisStore store;

    @Override
    public Flux<RouteDefinition> getRouteDefinitions() {
        return store.findAllServices()
                .filter(ServiceDefinition::isEnabled)
                .map(this::toRouteDefinition);
    }

    @Override
    public Mono<Void> save(Mono<RouteDefinition> route) {
        return Mono.error(new UnsupportedOperationException("Use /platform/admin/services API"));
    }

    @Override
    public Mono<Void> delete(Mono<String> routeId) {
        return Mono.error(new UnsupportedOperationException("Use /platform/admin/services API"));
    }

    private RouteDefinition toRouteDefinition(ServiceDefinition service) {
        RouteDefinition route = new RouteDefinition();
        route.setId(service.getId());
        route.setUri(URI.create(service.getUri()));

        List<PredicateDefinition> predicates = new ArrayList<>();
        PredicateDefinition pathPredicate = new PredicateDefinition();
        pathPredicate.setName("Path");
        pathPredicate.addArg("pattern", service.getPathPrefix());
        predicates.add(pathPredicate);
        route.setPredicates(predicates);

        List<FilterDefinition> filters = new ArrayList<>();
        FilterDefinition stripPrefix = new FilterDefinition();
        stripPrefix.setName("StripPrefix");
        stripPrefix.addArg("parts", "1");
        filters.add(stripPrefix);

        FilterDefinition rateLimiter = new FilterDefinition();
        rateLimiter.setName("CustomRateLimiter");
        filters.add(rateLimiter);

        if (service.isCircuitBreakerEnabled()) {
            FilterDefinition circuitBreaker = new FilterDefinition();
            circuitBreaker.setName("CircuitBreaker");
            circuitBreaker.addArg("name", "gatewayCircuitBreaker");
            circuitBreaker.addArg("fallbackUri", "forward:/fallback");
            filters.add(circuitBreaker);
        }

        route.setFilters(filters);
        return route;
    }
}