package com.platform.platform.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.config.RateLimiterProperties;
import com.platform.platform.service.IncidentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RateLimitAlertConsumer {

    private final IncidentService incidentService;
    private final RateLimiterProperties properties;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${rate-limiter.kafka.topic}", groupId = "rate-limit-platform")
    public void consume(String message) {
        log.info("Received rate-limit alert: {}", message);
        try {
            JsonNode node = objectMapper.readTree(message);
            String routeId = textOrDefault(node, "routeId", "unknown");
            String tenantId = textOrDefault(node, "tenantId", "unknown");
            String clientId = textOrDefault(node, "clientId", "unknown");
            String severity = textOrDefault(node, "severity", "WARN");
            incidentService.recordIncident(routeId, tenantId, clientId, message, severity).subscribe();
        } catch (Exception ex) {
            incidentService.recordIncident("unknown", "unknown", "unknown", message, "WARN").subscribe();
        }
    }

    private String textOrDefault(JsonNode node, String field, String defaultValue) {
        JsonNode value = node.get(field);
        return value != null && !value.isNull() ? value.asText() : defaultValue;
    }
}