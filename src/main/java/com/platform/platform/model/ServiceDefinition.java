package com.platform.platform.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ServiceDefinition {
    private String id;
    private String name;
    private String uri;
    private String pathPrefix;
    private int replenishRate;
    private int burstCapacity;
    private boolean circuitBreakerEnabled;
    private boolean enabled;
}