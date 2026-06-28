package com.platform.platform.service;

import com.platform.platform.dto.ApiKeyRequest;
import com.platform.platform.model.ApiKeyRecord;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ApiKeyAdminService {

    private final PlatformRedisStore store;

    public Mono<ApiKeyRecord> create(String tenantId, ApiKeyRequest request) {
        String key = request.getKey() != null && !request.getKey().isBlank()
                ? request.getKey()
                : PlatformRedisStore.generateApiKey();

        ApiKeyRecord record = ApiKeyRecord.builder()
                .key(key)
                .tenantId(tenantId)
                .name(request.getName())
                .enabled(true)
                .build();
        return store.saveApiKey(record);
    }

    public Mono<List<ApiKeyRecord>> listByTenant(String tenantId) {
        return store.findApiKeysByTenant(tenantId).collectList();
    }

    public Mono<Boolean> revoke(String tenantId, String key) {
        return store.deleteApiKey(key, tenantId);
    }
}