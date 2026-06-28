package com.platform.platform.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class TenantRequest {
    @NotBlank
    private String id;
    @NotBlank
    private String name;
    private String plan = "FREE";
    @Min(1)
    private int defaultReplenishRate = 10;
    @Min(1)
    private int defaultBurstCapacity = 20;
    private boolean enabled = true;
}