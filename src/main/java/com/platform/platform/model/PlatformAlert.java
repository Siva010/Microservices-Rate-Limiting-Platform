package com.platform.platform.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PlatformAlert {
    private String id;
    private String routeId;
    private String tenantId;
    private String clientId;
    private String message;
    private long timestamp;
    private String severity;
}