package com.platform.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.config.RateLimiterProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class AlertService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RateLimiterProperties properties;
    private final ObjectMapper objectMapper;
    private final Scheduler alertScheduler;
    private final ConcurrentHashMap<String, Long> lastAlertTimestamps = new ConcurrentHashMap<>();

    @Autowired
    public AlertService(KafkaTemplate<String, String> kafkaTemplate,
                        RateLimiterProperties properties,
                        ObjectMapper objectMapper) {
        this(kafkaTemplate, properties, objectMapper, Schedulers.boundedElastic());
    }

    AlertService(KafkaTemplate<String, String> kafkaTemplate,
                 RateLimiterProperties properties,
                 ObjectMapper objectMapper,
                 Scheduler alertScheduler) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.alertScheduler = alertScheduler;
    }

    public void publishRateLimitExceededAlert(String routeId, String tenantId, String clientId) {
        if (!shouldPublishAlert(routeId, tenantId, clientId)) {
            return;
        }

        String message = buildAlertMessage(routeId, tenantId, clientId);
        log.warn("Publishing alert to Kafka: {}", message);
        Mono.fromRunnable(() -> kafkaTemplate.send(properties.getKafka().getTopic(), clientId, message))
                .subscribeOn(alertScheduler)
                .subscribe(
                        null,
                        error -> log.error("Failed to publish rate limit alert to Kafka", error)
                );
    }

    boolean shouldPublishAlert(String routeId, String tenantId, String clientId) {
        long debounceSeconds = properties.getKafka().getAlertDebounceSeconds();
        if (debounceSeconds <= 0) {
            return true;
        }

        String alertKey = routeId + ":" + tenantId + ":" + clientId;
        long now = System.currentTimeMillis();
        Long lastAlert = lastAlertTimestamps.get(alertKey);

        if (lastAlert != null && now - lastAlert < Duration.ofSeconds(debounceSeconds).toMillis()) {
            return false;
        }

        lastAlertTimestamps.put(alertKey, now);
        return true;
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