package com.platform.platform.config;

import com.platform.config.RateLimiterProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(-100)
@RequiredArgsConstructor
public class AdminSecurityWebFilter implements WebFilter {

    private final RateLimiterProperties properties;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!path.startsWith("/platform/admin")) {
            return chain.filter(exchange);
        }

        String providedKey = exchange.getRequest().getHeaders().getFirst("X-Admin-Key");
        if (properties.getAdminApiKey() != null
                && properties.getAdminApiKey().equals(providedKey)) {
            return chain.filter(exchange);
        }

        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        byte[] body = "{\"error\":\"Unauthorized\",\"message\":\"Valid X-Admin-Key header required\"}"
                .getBytes();
        return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(body)));
    }
}