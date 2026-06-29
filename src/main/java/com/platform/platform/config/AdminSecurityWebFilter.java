package com.platform.platform.config;

import com.platform.config.RateLimiterProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Order(-100)
@RequiredArgsConstructor
@Slf4j
public class AdminSecurityWebFilter implements WebFilter {

    private final RateLimiterProperties properties;

    private static final AtomicBoolean defaultAdminKeyWarned = new AtomicBoolean(false);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!path.startsWith("/platform/admin")) {
            return chain.filter(exchange);
        }

        warnIfUsingDefaultAdminKey();

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

    private void warnIfUsingDefaultAdminKey() {
        String key = properties.getAdminApiKey();
        boolean isWeak = key == null || "changeme".equals(key) || key.length() < 16 || key.contains("demo");
        if (isWeak && defaultAdminKeyWarned.compareAndSet(false, true)) {
            String masked = (key == null) ? "null" : (key.length() > 4 ? key.substring(0, 4) + "****" : "****");
            log.warn("SECURITY WARNING: Weak or default admin API key in use (length={}, looksLikeDefault={}). " +
                    "Set a strong, unique ADMIN_API_KEY environment variable (min 16 chars recommended) before production use.",
                    key != null ? key.length() : 0, "changeme".equals(key));
        }
    }
}