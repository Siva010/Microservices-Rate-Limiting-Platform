package com.platform.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "rate-limiter")
public class RateLimiterProperties {

    private boolean failOpen = false;
    private boolean trustForwardedHeaders = false;
    private String adminApiKey = "changeme";
    private Kafka kafka = new Kafka();
    private Jwt jwt = new Jwt();

    @Data
    public static class Kafka {
        private String topic = "rate-limit-alerts";
        private long alertDebounceSeconds = 60;
    }

    @Data
    public static class Jwt {
        private boolean enabled = true;
        private String secret = "demo-secret-key-change-in-production-min-32-chars";
    }
}