package com.platform.platform.service;

import com.platform.config.RateLimiterProperties;
import com.platform.platform.model.ApiKeyRecord;
import com.platform.platform.model.ClientIdentity;
import com.platform.platform.store.PlatformRedisStore;
import com.platform.util.ClientIpResolver;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;

@Component
@RequiredArgsConstructor
public class ClientIdentityResolver {

    private final PlatformRedisStore store;
    private final ClientIpResolver clientIpResolver;
    private final RateLimiterProperties properties;

    public Mono<ClientIdentity> resolve(ServerHttpRequest request) {
        String apiKey = request.getHeaders().getFirst("X-API-Key");
        if (apiKey != null && !apiKey.isBlank()) {
            return resolveApiKey(apiKey.trim());
        }

        String authHeader = request.getHeaders().getFirst("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            return Mono.justOrEmpty(resolveJwt(authHeader.substring(7).trim()))
                    .switchIfEmpty(resolveIpFallback(request));
        }

        String tenantHeader = request.getHeaders().getFirst("X-Tenant-Id");
        if (tenantHeader != null && !tenantHeader.isBlank()) {
            String ip = clientIpResolver.resolve(request, properties.isTrustForwardedHeaders());
            return Mono.just(new ClientIdentity(
                    tenantHeader.trim(),
                    ip,
                    ClientIdentity.IdentityType.TENANT_HEADER));
        }

        return resolveIpFallback(request);
    }

    private Mono<ClientIdentity> resolveApiKey(String rawApiKey) {
        return store.findApiKey(rawApiKey)
                .filter(ApiKeyRecord::isEnabled)
                .map(record -> new ClientIdentity(
                        record.getTenantId(),
                        record.getKey(),  // use the stored hash (safe identifier), never the raw secret
                        ClientIdentity.IdentityType.API_KEY))
                .switchIfEmpty(Mono.error(new ResponseStatusException(
                        HttpStatus.UNAUTHORIZED, "Invalid or disabled API key")));
    }

    private ClientIdentity resolveJwt(String token) {
        if (!properties.getJwt().isEnabled()) {
            return null;
        }

        try {
            SecretKey key = Keys.hmacShaKeyFor(properties.getJwt().getSecret().getBytes(StandardCharsets.UTF_8));
            Claims claims = Jwts.parser()
                    .verifyWith(key)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();

            String tenantId = claims.get("tenant_id", String.class);
            if (tenantId == null) {
                tenantId = claims.get("tenantId", String.class);
            }
            if (tenantId == null) {
                tenantId = "default";
            }

            String subject = claims.getSubject() != null ? claims.getSubject() : "jwt-user";
            return new ClientIdentity(tenantId, subject, ClientIdentity.IdentityType.JWT);
        } catch (Exception e) {
            return null;
        }
    }

    private Mono<ClientIdentity> resolveIpFallback(ServerHttpRequest request) {
        String ip = clientIpResolver.resolve(request, properties.isTrustForwardedHeaders());
        return Mono.just(new ClientIdentity("anonymous", ip, ClientIdentity.IdentityType.IP));
    }
}