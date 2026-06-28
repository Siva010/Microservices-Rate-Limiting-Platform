package com.platform.platform.event;

public record PlatformConfigChangedEvent(String entityType, String entityId) {
}