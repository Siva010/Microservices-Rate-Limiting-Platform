package com.platform.util;

import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

class ClientIpResolverTest {

    private final ClientIpResolver resolver = new ClientIpResolver();

    @Test
    void resolve_WhenForwardedHeadersNotTrusted_ShouldUseRemoteAddress() {
        MockServerHttpRequest request = MockServerHttpRequest
                .get("/api/test")
                .header("X-Forwarded-For", "203.0.113.1, 198.51.100.2")
                .remoteAddress(new java.net.InetSocketAddress("10.0.0.5", 8080))
                .build();

        assertThat(resolver.resolve(request, false)).isEqualTo("10.0.0.5");
    }

    @Test
    void resolve_WhenForwardedHeadersTrusted_ShouldUseFirstForwardedIp() {
        MockServerHttpRequest request = MockServerHttpRequest
                .get("/api/test")
                .header("X-Forwarded-For", "203.0.113.1, 198.51.100.2")
                .remoteAddress(new java.net.InetSocketAddress("10.0.0.5", 8080))
                .build();

        assertThat(resolver.resolve(request, true)).isEqualTo("203.0.113.1");
    }

    @Test
    void resolve_WhenForwardedHeadersTrustedAndRealIpPresent_ShouldPreferForwardedFor() {
        MockServerHttpRequest request = MockServerHttpRequest
                .get("/api/test")
                .header("X-Real-IP", "203.0.113.9")
                .remoteAddress(new java.net.InetSocketAddress("10.0.0.5", 8080))
                .build();

        assertThat(resolver.resolve(request, true)).isEqualTo("203.0.113.9");
    }
}