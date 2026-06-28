package com.platform.platform.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitPolicy {
    private String id;
    private String routeId;
    private String tenantId;
    private int replenishRate;
    private int burstCapacity;
    private boolean enabled;
}