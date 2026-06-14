package com.platform.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AlertService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${rate-limiter.kafka.topic}")
    private String alertTopic;

    public void publishRateLimitExceededAlert(String routeId, String key) {
        String message = String.format("Rate limit exceeded for route: %s, key: %s", routeId, key);
        log.warn("Publishing alert to Kafka: {}", message);
        reactor.core.publisher.Mono.fromRunnable(() -> kafkaTemplate.send(alertTopic, key, message))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .subscribe();
    }
}
