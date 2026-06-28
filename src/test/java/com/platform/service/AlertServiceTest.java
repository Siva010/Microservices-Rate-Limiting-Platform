package com.platform.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.config.RateLimiterProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AlertServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private RateLimiterProperties properties;
    private AlertService alertService;

    @BeforeEach
    void setUp() {
        properties = new RateLimiterProperties();
        properties.getKafka().setTopic("rate-limit-alerts");
        properties.getKafka().setAlertDebounceSeconds(60);
        alertService = new AlertService(kafkaTemplate, properties, new ObjectMapper(), Schedulers.immediate());
    }

    @Test
    void publishRateLimitExceededAlert_ShouldSendJsonToKafka() {
        StepVerifier.create(reactor.core.publisher.Mono.fromRunnable(
                        () -> alertService.publishRateLimitExceededAlert("dummy-service", "acme", "192.168.1.1")))
                .verifyComplete();

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("rate-limit-alerts"), eq("192.168.1.1"), payloadCaptor.capture());
        assertThat(payloadCaptor.getValue()).contains("dummy-service");
        assertThat(payloadCaptor.getValue()).contains("acme");
    }

    @Test
    void shouldPublishAlert_ShouldDebounceRepeatedAlerts() {
        assertThat(alertService.shouldPublishAlert("route-a", "tenant-a", "10.0.0.1")).isTrue();
        assertThat(alertService.shouldPublishAlert("route-a", "tenant-a", "10.0.0.1")).isFalse();
        assertThat(alertService.shouldPublishAlert("route-b", "tenant-a", "10.0.0.1")).isTrue();
    }

    @Test
    void publishRateLimitExceededAlert_ShouldNotFloodKafkaWhenDebounced() {
        alertService.publishRateLimitExceededAlert("dummy-service", "acme", "192.168.1.1");
        alertService.publishRateLimitExceededAlert("dummy-service", "acme", "192.168.1.1");

        verify(kafkaTemplate, times(1)).send(eq("rate-limit-alerts"), eq("192.168.1.1"), org.mockito.ArgumentMatchers.anyString());
    }
}