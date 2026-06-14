package com.platform.filter;

import com.platform.service.RateLimiterService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class CustomRateLimiterGatewayFilterFactory extends AbstractGatewayFilterFactory<CustomRateLimiterGatewayFilterFactory.Config> {

    private final RateLimiterService rateLimiterService;

    public CustomRateLimiterGatewayFilterFactory(RateLimiterService rateLimiterService) {
        super(Config.class);
        this.rateLimiterService = rateLimiterService;
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return Arrays.asList("replenishRate", "burstCapacity");
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String routeId = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
            if (routeId == null) {
                routeId = "unknown_route";
            }
            
            // In a real application, you might use IP address or user ID. For demo, we use IP.
            String key = exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();

            return rateLimiterService.isAllowed(routeId, key, config.getReplenishRate(), config.getBurstCapacity())
                    .flatMap(allowed -> {
                        if (allowed) {
                            return chain.filter(exchange);
                        } else {
                            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                            return exchange.getResponse().setComplete();
                        }
                    });
        };
    }

    @Data
    public static class Config {
        private int replenishRate = 1;
        private int burstCapacity = 1;
    }
}
