package com.platform.service;

import com.platform.config.RateLimiterProperties;
import com.platform.model.RateLimitResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RateLimiterService {

    private final ReactiveStringRedisTemplate redisTemplate;
    private final RedisScript<List<Long>> script;
    private final AlertService alertService;
    private final RateLimiterProperties properties;

    public Mono<RateLimitResult> checkRateLimit(String rateLimitKey,
                                                int replenishRate,
                                                int burstCapacity,
                                                String routeId,
                                                String tenantId,
                                                String clientId) {
        if (replenishRate <= 0 || burstCapacity <= 0) {
            log.warn("Invalid rate limit config for route {}: replenishRate={}, burstCapacity={}",
                    routeId, replenishRate, burstCapacity);
            return Mono.just(RateLimitResult.denied(burstCapacity, replenishRate));
        }

        List<String> keys = getKeys(rateLimitKey);
        int requestedTokens = 1;

        return redisTemplate.execute(this.script, keys, Arrays.asList(
                        String.valueOf(replenishRate),
                        String.valueOf(burstCapacity),
                        String.valueOf(Instant.now().getEpochSecond()),
                        String.valueOf(requestedTokens)
                )).next()
                .map(response -> toRateLimitResult(response, burstCapacity, replenishRate))
                .doOnNext(result -> {
                    if (!result.allowed()) {
                        alertService.publishRateLimitExceededAlert(routeId, tenantId, clientId);
                    }
                })
                .onErrorResume(e -> {
                    log.error("Error determining if request allowed", e);
                    return Mono.just(RateLimitResult.redisUnavailable(
                            properties.isFailOpen(), burstCapacity, replenishRate));
                });
    }

    private RateLimitResult toRateLimitResult(List<Long> response, int burstCapacity, int replenishRate) {
        boolean allowed = toLong(response.get(0)) == 1L;
        long remaining = response.size() > 1 ? toLong(response.get(1)) : 0L;
        return new RateLimitResult(allowed, remaining, burstCapacity, replenishRate);
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException("Unexpected lua response value: " + value);
    }

    static List<String> getKeys(String rateLimitKey) {
        String prefix = "request_rate_limiter.{" + rateLimitKey;
        String tokenKey = prefix + "}.tokens";
        String timestampKey = prefix + "}.timestamp";
        return Arrays.asList(tokenKey, timestampKey);
    }
}