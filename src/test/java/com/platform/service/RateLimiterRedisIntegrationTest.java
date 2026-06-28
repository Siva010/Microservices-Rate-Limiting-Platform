package com.platform.service;

import com.platform.config.RateLimiterProperties;
import com.platform.config.RedisConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Testcontainers(disabledWithoutDocker = true)
class RateLimiterRedisIntegrationTest {

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.0-alpine"))
            .withExposedPorts(6379);

    private RateLimiterService rateLimiterService;

    @BeforeEach
    void setUp() {
        ReactiveRedisConnectionFactory connectionFactory = new LettuceConnectionFactory(
                redis.getHost(), redis.getMappedPort(6379));
        ((LettuceConnectionFactory) connectionFactory).afterPropertiesSet();

        RedisConfig redisConfig = new RedisConfig();
        RedisScript<List<Long>> script = redisConfig.rateLimiterScript();

        RateLimiterProperties properties = new RateLimiterProperties();
        properties.setFailOpen(false);

        AlertService alertService = mock(AlertService.class);
        rateLimiterService = new RateLimiterService(
                new ReactiveStringRedisTemplate(connectionFactory), script, alertService, properties);
    }

    @Test
    void checkRateLimit_WithRealRedis_ShouldAllowThenDenyWithinBurst() {
        String key = "integration-route:tenant-a:203.0.113.42";

        StepVerifier.create(rateLimiterService.checkRateLimit(key, 10, 2, "integration-route", "tenant-a", "203.0.113.42"))
                .assertNext(result -> assertThat(result.allowed()).isTrue())
                .verifyComplete();

        StepVerifier.create(rateLimiterService.checkRateLimit(key, 10, 2, "integration-route", "tenant-a", "203.0.113.42"))
                .assertNext(result -> assertThat(result.allowed()).isTrue())
                .verifyComplete();

        StepVerifier.create(rateLimiterService.checkRateLimit(key, 10, 2, "integration-route", "tenant-a", "203.0.113.42"))
                .assertNext(result -> assertThat(result.allowed()).isFalse())
                .verifyComplete();
    }

    @Test
    void checkRateLimit_WithRealRedis_ShouldIsolateBucketsPerRoute() {
        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-a:tenant-a:10.0.0.1", 10, 1, "route-a", "tenant-a", "10.0.0.1"))
                .assertNext(result -> assertThat(result.allowed()).isTrue())
                .verifyComplete();

        StepVerifier.create(rateLimiterService.checkRateLimit(
                        "route-b:tenant-a:10.0.0.1", 10, 1, "route-b", "tenant-a", "10.0.0.1"))
                .assertNext(result -> assertThat(result.allowed()).isTrue())
                .verifyComplete();
    }
}