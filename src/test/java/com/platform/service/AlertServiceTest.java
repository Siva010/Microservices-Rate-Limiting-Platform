package com.platform.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AlertServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @InjectMocks
    private AlertService alertService;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(alertService, "alertTopic", "rate-limit-alerts");
    }

    @Test
    void publishRateLimitExceededAlert_ShouldSendToKafka() {
        // Arrange
        String routeId = "dummy-service";
        String key = "192.168.1.1";
        String expectedMessage = "Rate limit exceeded for route: dummy-service, key: 192.168.1.1";

        // Act
        alertService.publishRateLimitExceededAlert(routeId, key);

        // Assert
        verify(kafkaTemplate).send(eq("rate-limit-alerts"), eq(key), eq(expectedMessage));
    }
}
