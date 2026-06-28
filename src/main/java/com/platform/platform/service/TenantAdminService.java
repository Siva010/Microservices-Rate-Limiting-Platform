package com.platform.platform.service;

import com.platform.platform.dto.TenantRequest;
import com.platform.platform.model.Tenant;
import com.platform.platform.store.PlatformRedisStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TenantAdminService {

    private final PlatformRedisStore store;

    public Mono<Tenant> create(TenantRequest request) {
        Tenant tenant = Tenant.builder()
                .id(request.getId())
                .name(request.getName())
                .plan(request.getPlan())
                .defaultReplenishRate(request.getDefaultReplenishRate())
                .defaultBurstCapacity(request.getDefaultBurstCapacity())
                .enabled(request.isEnabled())
                .build();
        return store.saveTenant(tenant);
    }

    public Mono<Tenant> update(String id, TenantRequest request) {
        request.setId(id);
        return create(request);
    }

    public Mono<Tenant> get(String id) {
        return store.findTenant(id);
    }

    public Mono<List<Tenant>> list() {
        return store.findAllTenants().collectList();
    }

    public Mono<Boolean> delete(String id) {
        return store.deleteTenant(id);
    }
}