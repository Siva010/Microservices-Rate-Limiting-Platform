package com.platform.platform.controller.admin;

import com.platform.platform.dto.PolicyRequest;
import com.platform.platform.model.RateLimitPolicy;
import com.platform.platform.service.PolicyService;
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
@RequestMapping("/platform/admin/policies")
@RequiredArgsConstructor
public class PolicyAdminController {

    private final PolicyService policyService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<RateLimitPolicy> create(@Valid @RequestBody PolicyRequest request) {
        RateLimitPolicy policy = RateLimitPolicy.builder()
                .id(request.getId())
                .routeId(request.getRouteId())
                .tenantId(request.getTenantId())
                .replenishRate(request.getReplenishRate())
                .burstCapacity(request.getBurstCapacity())
                .enabled(request.isEnabled())
                .build();
        return policyService.createPolicy(policy);
    }

    @GetMapping
    public Mono<List<RateLimitPolicy>> list() {
        return policyService.listPolicies();
    }

    @GetMapping("/{id}")
    public Mono<RateLimitPolicy> get(@PathVariable String id) {
        return policyService.getPolicy(id);
    }

    @PutMapping("/{id}")
    public Mono<RateLimitPolicy> update(@PathVariable String id, @Valid @RequestBody PolicyRequest request) {
        RateLimitPolicy policy = RateLimitPolicy.builder()
                .id(id)
                .routeId(request.getRouteId())
                .tenantId(request.getTenantId())
                .replenishRate(request.getReplenishRate())
                .burstCapacity(request.getBurstCapacity())
                .enabled(request.isEnabled())
                .build();
        return policyService.updatePolicy(id, policy);
    }

    @DeleteMapping("/{id}")
    public Mono<Boolean> delete(@PathVariable String id) {
        return policyService.deletePolicy(id);
    }
}