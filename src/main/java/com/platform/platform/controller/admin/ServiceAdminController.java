package com.platform.platform.controller.admin;

import com.platform.platform.dto.ServiceRequest;
import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.service.ServiceRegistryService;
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
@RequestMapping("/platform/admin/services")
@RequiredArgsConstructor
public class ServiceAdminController {

    private final ServiceRegistryService serviceRegistryService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ServiceDefinition> register(@Valid @RequestBody ServiceRequest request) {
        return serviceRegistryService.register(request);
    }

    @GetMapping
    public Mono<List<ServiceDefinition>> list() {
        return serviceRegistryService.list();
    }

    @GetMapping("/{id}")
    public Mono<ServiceDefinition> get(@PathVariable String id) {
        return serviceRegistryService.get(id);
    }

    @PutMapping("/{id}")
    public Mono<ServiceDefinition> update(@PathVariable String id, @Valid @RequestBody ServiceRequest request) {
        return serviceRegistryService.update(id, request);
    }

    @DeleteMapping("/{id}")
    public Mono<Boolean> delete(@PathVariable String id) {
        return serviceRegistryService.delete(id);
    }
}