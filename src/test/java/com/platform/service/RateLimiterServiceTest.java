package com.platform.service;

import com.platform.config.RateLimiterProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RateLimiterServiceTest {

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    private RedisScript<List<Long>> script;

    @Mock
    private AlertService alertService;

    @Mock
    private RateLimiterProperties properties;

    @InjectMocks
    private RateLimiterService rateLimiterService;

    @Test
    void checkRateLimit_WhenRedisReturnsAllowed_ShouldReturnAllowedAndNotAlert() {
        List<Long> mockResponse = Arrays.asList(1L, 10L);
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.just(mockResponse));

        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-a:tenant-a:client-a", 10, 20, "route-a", "tenant-a", "client-a"))
                .assertNext(result -> {
                    assertThat(result.allowed()).isTrue();
                    assertThat(result.remainingTokens()).isEqualTo(10L);
                })
                .verifyComplete();

        verify(alertService, never()).publishRateLimitExceededAlert(any(), any(), any());
    }

    @Test
    void checkRateLimit_WhenRedisReturnsNotAllowed_ShouldReturnDeniedAndAlert() {
        List<Long> mockResponse = Arrays.asList(0L, 0L);
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.just(mockResponse));

        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-a:tenant-a:client-a", 10, 20, "route-a", "tenant-a", "client-a"))
                .assertNext(result -> assertThat(result.allowed()).isFalse())
                .verifyComplete();

        verify(alertService).publishRateLimitExceededAlert("route-a", "tenant-a", "client-a");
    }

    @Test
    void checkRateLimit_WhenRedisThrowsException_ShouldFailClosedByDefault() {
        when(properties.isFailOpen()).thenReturn(false);
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.error(new RuntimeException("Redis timeout")));

        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-a:tenant-a:client-a", 10, 20, "route-a", "tenant-a", "client-a"))
                .assertNext(result -> assertThat(result.allowed()).isFalse())
                .verifyComplete();
    }

    @Test
    void checkRateLimit_WhenRedisThrowsExceptionAndFailOpenEnabled_ShouldAllow() {
        when(properties.isFailOpen()).thenReturn(true);
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.error(new RuntimeException("Redis timeout")));

        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-a:tenant-a:client-a", 10, 20, "route-a", "tenant-a", "client-a"))
                .assertNext(result -> assertThat(result.allowed()).isTrue())
                .verifyComplete();
    }

    @Test
    void checkRateLimit_WithInvalidConfig_ShouldDenyWithoutCallingRedis() {
        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-a:tenant-a:client-a", 0, 20, "route-a", "tenant-a", "client-a"))
                .assertNext(result -> assertThat(result.allowed()).isFalse())
                .verifyComplete();

        verify(redisTemplate, never()).execute(any(), anyList(), anyList());
    }

    @Test
    void getKeys_ShouldUseCompositeRateLimitKey() {
        List<String> keys = RateLimiterService.getKeys("dummy-service:acme:10.0.0.1");

        assertThat(keys).containsExactly(
                "request_rate_limiter.{dummy-service:acme:10.0.0.1}.tokens",
                "request_rate_limiter.{dummy-service:acme:10.0.0.1}.timestamp"
        );
    }
}