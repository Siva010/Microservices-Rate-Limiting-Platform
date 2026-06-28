package com.platform.platform.model;

public record ClientIdentity(
        String tenantId,
        String clientId,
        IdentityType identityType
) {
    public String rateLimitKey(String routeId) {
        return routeId + ":" + tenantId + ":" + clientId;
    }

    public enum IdentityType {
        API_KEY,
        JWT,
        TENANT_HEADER,
        IP
    }
}