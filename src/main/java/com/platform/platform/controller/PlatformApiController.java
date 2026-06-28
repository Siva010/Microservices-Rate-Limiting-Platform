package com.platform.platform.controller;

import com.platform.platform.model.PlatformAlert;
import com.platform.platform.service.IncidentService;
import com.platform.platform.service.PolicyService;
import com.platform.platform.service.ServiceRegistryService;
import com.platform.platform.service.TenantAdminService;
import com.platform.platform.service.UsageTrackingService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/platform/api")
@RequiredArgsConstructor
public class PlatformApiController {

    private final IncidentService incidentService;
    private final UsageTrackingService usageTrackingService;
    private final TenantAdminService tenantAdminService;
    private final ServiceRegistryService serviceRegistryService;
    private final PolicyService policyService;
    private final MeterRegistry meterRegistry;

    @GetMapping("/alerts/recent")
    public Mono<List<PlatformAlert>> recentAlerts(@RequestParam(defaultValue = "20") int limit) {
        return incidentService.recentAlerts(limit);
    }

    @GetMapping("/usage/{tenantId}")
    public Mono<Map<String, Long>> usage(@PathVariable String tenantId,
                                         @RequestParam(defaultValue = "*") String clientId) {
        return usageTrackingService.getUsage(tenantId, clientId);
    }

    @GetMapping("/metrics/summary")
    public Mono<Map<String, Object>> metricsSummary() {
        return Mono.zip(
                tenantAdminService.list(),
                serviceRegistryService.list(),
                policyService.listPolicies(),
                incidentService.recentAlerts(10)
        ).map(tuple -> {
            Map<String, Object> summary = new HashMap<>();
            summary.put("tenantCount", tuple.getT1().size());
            summary.put("serviceCount", tuple.getT2().size());
            summary.put("policyCount", tuple.getT3().size());
            summary.put("recentAlerts", tuple.getT4());
            summary.put("allowedTotal", meterRegistry.find("rate_limit_allowed_total").counters().stream()
                    .mapToDouble(c -> c.count()).sum());
            summary.put("deniedTotal", meterRegistry.find("rate_limit_denied_total").counters().stream()
                    .mapToDouble(c -> c.count()).sum());
            return summary;
        });
    }
}