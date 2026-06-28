package com.platform.platform.controller.admin;

import com.platform.platform.dto.ApiKeyRequest;
import com.platform.platform.model.ApiKeyRecord;
import com.platform.platform.service.ApiKeyAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/platform/admin/tenants/{tenantId}/api-keys")
@RequiredArgsConstructor
public class ApiKeyAdminController {

    private final ApiKeyAdminService apiKeyAdminService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ApiKeyRecord> create(@PathVariable String tenantId,
                                     @Valid @RequestBody ApiKeyRequest request) {
        return apiKeyAdminService.create(tenantId, request);
    }

    @GetMapping
    public Mono<List<ApiKeyRecord>> list(@PathVariable String tenantId) {
        return apiKeyAdminService.listByTenant(tenantId);
    }

    @DeleteMapping("/{key}")
    public Mono<Boolean> revoke(@PathVariable String tenantId, @PathVariable String key) {
        return apiKeyAdminService.revoke(tenantId, key);
    }
}