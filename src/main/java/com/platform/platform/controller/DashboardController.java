package com.platform.platform.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class DashboardController {

    @GetMapping(value = "/platform/dashboard", produces = MediaType.TEXT_HTML_VALUE)
    public Mono<String> dashboard() {
        return Mono.fromCallable(() -> {
            ClassPathResource resource = new ClassPathResource("static/platform/dashboard.html");
            return new String(resource.getInputStream().readAllBytes());
        });
    }
}