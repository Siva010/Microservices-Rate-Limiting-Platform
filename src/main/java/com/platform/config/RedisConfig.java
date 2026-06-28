package com.platform.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveStringRedisTemplate(factory);
    }

    @Bean
    @SuppressWarnings("unchecked")
    public RedisScript<List<Long>> rateLimiterScript() {
        DefaultRedisScript<List<Long>> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("scripts/request_rate_limiter.lua"));
        script.setResultType((Class<List<Long>>) (Class<?>) List.class);
        return script;
    }
}