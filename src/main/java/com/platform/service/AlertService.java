package com.platform.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.config.RateLimiterProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
@Slf4j
public class AlertService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RateLimiterProperties properties;
    private final ObjectMapper objectMapper;
    private final Scheduler alertScheduler;
    private final ReactiveStringRedisTemplate redisTemplate;

    @Autowired
    public AlertService(KafkaTemplate<String, String> kafkaTemplate,
                        RateLimiterProperties properties,
                        ObjectMapper objectMapper,
                        ReactiveStringRedisTemplate redisTemplate) {
        this(kafkaTemplate, properties, objectMapper, Schedulers.boundedElastic(), redisTemplate);
    }

    AlertService(KafkaTemplate<String, String> kafkaTemplate,
                 RateLimiterProperties properties,
                 ObjectMapper objectMapper,
                 Scheduler alertScheduler,
                 ReactiveStringRedisTemplate redisTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.alertScheduler = alertScheduler;
        this.redisTemplate = redisTemplate;
    }

    public void publishRateLimitExceededAlert(String routeId, String tenantId, String clientId) {
        long debounceSeconds = properties.getKafka().getAlertDebounceSeconds();
        if (debounceSeconds <= 0) {
            doPublishAlert(routeId, tenantId, clientId);
            return;
        }

        String debounceKey = "rate-limit-alert-debounce:" + routeId + ":" + tenantId + ":" + clientId;
        redisTemplate.opsForValue()
                .setIfAbsent(debounceKey, "1", Duration.ofSeconds(debounceSeconds))
                .subscribeOn(alertScheduler)
                .subscribe(
                        set -> {
                            if (Boolean.TRUE.equals(set)) {
                                doPublishAlert(routeId, tenantId, clientId);
                            }
                        },
                        error -> {
                            log.error("Failed to check alert debounce in Redis, publishing anyway (best-effort)", error);
                            doPublishAlert(routeId, tenantId, clientId);  // fallback to avoid dropping critical alerts
                        }
                );
    }

    private void doPublishAlert(String routeId, String tenantId, String clientId) {
        String message = buildAlertMessage(routeId, tenantId, clientId);
        log.warn("Publishing alert to Kafka: {}", message);
        Mono.fromRunnable(() -> kafkaTemplate.send(properties.getKafka().getTopic(), clientId, message))
                .subscribeOn(alertScheduler)
                .subscribe(
                        null,
                        error -> log.error("Failed to publish rate limit alert to Kafka", error)
                );
    }

    private String buildAlertMessage(String routeId, String tenantId, String clientId) {
        Map<String, String> payload = new LinkedHashMap<>();
        payload.put("routeId", routeId);
        payload.put("tenantId", tenantId);
        payload.put("clientId", clientId);
        payload.put("severity", "WARN");
        payload.put("message", String.format(
                "Rate limit exceeded for route=%s tenant=%s client=%s", routeId, tenantId, clientId));
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return payload.get("message");
        }
    }
}