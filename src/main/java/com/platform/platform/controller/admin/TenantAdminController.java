package com.platform.platform.controller.admin;

import com.platform.platform.dto.TenantRequest;
import com.platform.platform.model.Tenant;
import com.platform.platform.service.TenantAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/platform/admin/tenants")
@RequiredArgsConstructor
public class TenantAdminController {

    private final TenantAdminService tenantAdminService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Tenant> create(@Valid @RequestBody TenantRequest request) {
        return tenantAdminService.create(request);
    }

    @GetMapping
    public Mono<List<Tenant>> list() {
        return tenantAdminService.list();
    }

    @GetMapping("/{id}")
    public Mono<Tenant> get(@PathVariable String id) {
        return tenantAdminService.get(id);
    }

    @PutMapping("/{id}")
    public Mono<Tenant> update(@PathVariable String id, @Valid @RequestBody TenantRequest request) {
        return tenantAdminService.update(id, request);
    }

    @DeleteMapping("/{id}")
    public Mono<Boolean> delete(@PathVariable String id) {
        return tenantAdminService.delete(id);
    }
}