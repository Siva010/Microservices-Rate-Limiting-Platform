package com.platform.platform.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class ServiceRequest {
    @NotBlank
    private String id;
    @NotBlank
    private String name;
    @NotBlank
    private String uri;
    @NotBlank
    private String pathPrefix;
    @Min(1)
    private int replenishRate = 10;
    @Min(1)
    private int burstCapacity = 20;
    private boolean circuitBreakerEnabled = true;
    private boolean enabled = true;
}