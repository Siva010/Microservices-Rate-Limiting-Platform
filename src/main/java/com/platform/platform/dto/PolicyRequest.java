package com.platform.platform.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class PolicyRequest {
    @NotBlank
    private String id;
    @NotBlank
    private String routeId;
    private String tenantId;
    @Min(1)
    private int replenishRate;
    @Min(1)
    private int burstCapacity;
    private boolean enabled = true;
}