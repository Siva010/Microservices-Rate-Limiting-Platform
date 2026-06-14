package com.platform.service;

import org.junit.jupiter.api.BeforeEach;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RateLimiterServiceTest {

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    private RedisScript<List> script;

    @Mock
    private AlertService alertService;

    @InjectMocks
    private RateLimiterService rateLimiterService;

    @BeforeEach
    void setUp() {
    }

    @Test
    void isAllowed_WhenRedisReturnsAllowed_ShouldReturnTrueAndNotAlert() {
        // Arrange
        List<Long> mockResponse = Arrays.asList(1L, 10L); // 1 = allowed, 10 = remaining tokens
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.just(mockResponse));

        // Act
        var result = rateLimiterService.isAllowed("test-route", "127.0.0.1", 10, 20);

        // Assert
        StepVerifier.create(result)
                .expectNext(true)
                .verifyComplete();

        verify(alertService, never()).publishRateLimitExceededAlert(any(), any());
    }

    @Test
    void isAllowed_WhenRedisReturnsNotAllowed_ShouldReturnFalseAndAlert() {
        // Arrange
        List<Long> mockResponse = Arrays.asList(0L, 0L); // 0 = not allowed, 0 = remaining tokens
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.just(mockResponse));

        // Act
        var result = rateLimiterService.isAllowed("test-route", "127.0.0.1", 10, 20);

        // Assert
        StepVerifier.create(result)
                .expectNext(false)
                .verifyComplete();

        verify(alertService).publishRateLimitExceededAlert("test-route", "127.0.0.1");
    }

    @Test
    void isAllowed_WhenRedisThrowsException_ShouldReturnTrueFailOpen() {
        // Arrange
        when(redisTemplate.execute(eq(script), anyList(), anyList()))
                .thenReturn(Flux.error(new RuntimeException("Redis timeout")));

        // Act
        var result = rateLimiterService.isAllowed("test-route", "127.0.0.1", 10, 20);

        // Assert
        StepVerifier.create(result)
                .expectNext(true) // fail-open
                .verifyComplete();
    }
}
