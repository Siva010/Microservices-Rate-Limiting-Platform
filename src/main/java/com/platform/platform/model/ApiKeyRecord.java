package com.platform.platform.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiKeyRecord {
    private String key;
    private String tenantId;
    private String name;
    private boolean enabled;
}