package com.platform.platform.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Tenant {
    private String id;
    private String name;
    private String plan;
    private int defaultReplenishRate;
    private int defaultBurstCapacity;
    private boolean enabled;
}