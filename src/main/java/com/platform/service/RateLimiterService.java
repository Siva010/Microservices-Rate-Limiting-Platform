package com.platform.service;

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
    private final RedisScript<List> script;
    private final AlertService alertService;

    public Mono<Boolean> isAllowed(String routeId, String id, int replenishRate, int burstCapacity) {
        List<String> keys = getKeys(id);
        int requestedTokens = 1;
        
        return redisTemplate.execute(this.script, keys, Arrays.asList(
                String.valueOf(replenishRate),
                String.valueOf(burstCapacity),
                String.valueOf(Instant.now().getEpochSecond()),
                String.valueOf(requestedTokens)
        )).next().map(response -> {
            boolean allowed = (Long) response.get(0) == 1L;
            if (!allowed) {
                alertService.publishRateLimitExceededAlert(routeId, id);
            }
            return allowed;
        }).onErrorResume(e -> {
            log.error("Error determining if request allowed", e);
            return Mono.just(true); // Fail-open on Redis failure
        });
    }

    private static List<String> getKeys(String id) {
        String prefix = "request_rate_limiter.{" + id;
        String tokenKey = prefix + "}.tokens";
        String timestampKey = prefix + "}.timestamp";
        return Arrays.asList(tokenKey, timestampKey);
    }
}
