package com.platform.platform.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.platform.event.PlatformConfigChangedEvent;
import com.platform.platform.model.ApiKeyRecord;
import com.platform.platform.model.PlatformAlert;
import com.platform.platform.model.RateLimitPolicy;
import com.platform.platform.model.ServiceDefinition;
import com.platform.platform.model.Tenant;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class PlatformRedisStore {

    static final String TENANT_KEY = "platform:tenants:";
    static final String TENANT_IDS = "platform:tenants:ids";
    static final String API_KEY_KEY = "platform:apikeys:";
    static final String API_KEYS_BY_TENANT = "platform:apikeys:by-tenant:";
    static final String SERVICE_KEY = "platform:services:";
    static final String SERVICE_IDS = "platform:services:ids";
    static final String POLICY_KEY = "platform:policies:";
    static final String POLICY_IDS = "platform:policies:ids";
    static final String ALERTS_RECENT = "platform:alerts:recent";
    static final String USAGE_ALLOWED = "platform:usage:allowed:";
    static final String USAGE_DENIED = "platform:usage:denied:";

    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    public Mono<Tenant> saveTenant(Tenant tenant) {
        return write(TENANT_KEY + tenant.getId(), tenant)
                .then(redisTemplate.opsForSet().add(TENANT_IDS, tenant.getId()).then())
                .then(Mono.fromRunnable(() -> eventPublisher.publishEvent(
                        new PlatformConfigChangedEvent("tenant", tenant.getId()))))
                .thenReturn(tenant);
    }

    public Mono<Tenant> findTenant(String id) {
        return read(TENANT_KEY + id, Tenant.class);
    }

    public Flux<Tenant> findAllTenants() {
        return redisTemplate.opsForSet().members(TENANT_IDS)
                .flatMap(id -> findTenant(id).onErrorResume(e -> Mono.empty()));
    }

    public Mono<Boolean> deleteTenant(String id) {
        return redisTemplate.opsForSet().remove(TENANT_IDS, id)
                .then(redisTemplate.delete(TENANT_KEY + id))
                .map(count -> count > 0)
                .doOnNext(deleted -> {
                    if (Boolean.TRUE.equals(deleted)) {
                        eventPublisher.publishEvent(new PlatformConfigChangedEvent("tenant", id));
                    }
                });
    }

    public Mono<ApiKeyRecord> saveApiKey(ApiKeyRecord record) {
        String raw = record.getKey();
        if (raw == null || raw.isBlank()) {
            return Mono.error(new IllegalArgumentException("API key value is required"));
        }
        String hash = hashApiKey(raw);

        // Persist with opaque (hashed) value in the record to avoid storing plaintext secret
        ApiKeyRecord toStore = ApiKeyRecord.builder()
                .key(hash)
                .tenantId(record.getTenantId())
                .name(record.getName())
                .enabled(record.isEnabled())
                .build();

        String storageRef = API_KEY_KEY + hash;
        return write(storageRef, toStore)
                .then(redisTemplate.opsForSet()
                        .add(API_KEYS_BY_TENANT + record.getTenantId(), hash).then())
                .then(Mono.fromRunnable(() -> eventPublisher.publishEvent(
                        new PlatformConfigChangedEvent("apikey", hash))))
                .thenReturn(record);  // return original (with raw) to caller for create response
    }

    public Mono<ApiKeyRecord> findApiKey(String providedKey) {
        if (providedKey == null || providedKey.isBlank()) {
            return Mono.empty();
        }
        // Always compute hash; support legacy (keys stored under their raw value) + modern (stored under hash)
        String asHash = hashApiKey(providedKey);
        // Try legacy path first (providedKey may be an old raw value used as storage suffix)
        return read(API_KEY_KEY + providedKey, ApiKeyRecord.class)
                .onErrorResume(e -> Mono.empty())
                .switchIfEmpty(read(API_KEY_KEY + asHash, ApiKeyRecord.class));
    }

    /**
     * Find by the *stored* identifier (hash or legacy raw). Used internally by tenant listing.
     */
    private Mono<ApiKeyRecord> findByStorageId(String storageId) {
        if (storageId == null || storageId.isBlank()) return Mono.empty();
        return read(API_KEY_KEY + storageId, ApiKeyRecord.class).onErrorResume(e -> Mono.empty());
    }

    public Flux<ApiKeyRecord> findApiKeysByTenant(String tenantId) {
        return redisTemplate.opsForSet().members(API_KEYS_BY_TENANT + tenantId)
                .flatMap(this::findByStorageId)
                .map(this::maskKeyForAdminList);  // never return raw; lists show opaque id
    }

    private ApiKeyRecord maskKeyForAdminList(ApiKeyRecord rec) {
        if (rec == null || rec.getKey() == null) return rec;
        String k = rec.getKey();
        if (k.length() <= 8) return rec;
        // For admin list responses, show only the stored identifier (hash) or a masked view
        String display = k.substring(0, 4) + "****" + k.substring(k.length() - 4);
        return ApiKeyRecord.builder()
                .key(display)
                .tenantId(rec.getTenantId())
                .name(rec.getName())
                .enabled(rec.isEnabled())
                .build();
    }

    public Mono<Boolean> deleteApiKey(String keyOrId, String tenantId) {
        String hash = hashApiKey(keyOrId);
        // Try treating param as already-hashed id (from lists), fallback to hashed raw
        return tryDeleteApiKeyEntry(tenantId, keyOrId)
                .flatMap(deleted -> {
                    if (Boolean.TRUE.equals(deleted)) {
                        return Mono.just(true);
                    }
                    return tryDeleteApiKeyEntry(tenantId, hash);
                })
                .doOnNext(deleted -> {
                    if (Boolean.TRUE.equals(deleted)) {
                        // Always publish the safe hash identifier, never raw secret
                        eventPublisher.publishEvent(new PlatformConfigChangedEvent("apikey", hash));
                    }
                });
    }

    private Mono<Boolean> tryDeleteApiKeyEntry(String tenantId, String storageId) {
        if (storageId == null) return Mono.just(false);
        return redisTemplate.opsForSet().remove(API_KEYS_BY_TENANT + tenantId, storageId)
                .then(redisTemplate.delete(API_KEY_KEY + storageId))
                .map(count -> count > 0);
    }

    public Mono<ServiceDefinition> saveService(ServiceDefinition service) {
        return write(SERVICE_KEY + service.getId(), service)
                .then(redisTemplate.opsForSet().add(SERVICE_IDS, service.getId()).then())
                .then(Mono.fromRunnable(() -> eventPublisher.publishEvent(
                        new PlatformConfigChangedEvent("service", service.getId()))))
                .thenReturn(service);
    }

    public Mono<ServiceDefinition> findService(String id) {
        return read(SERVICE_KEY + id, ServiceDefinition.class);
    }

    public Flux<ServiceDefinition> findAllServices() {
        return redisTemplate.opsForSet().members(SERVICE_IDS)
                .flatMap(id -> findService(id).onErrorResume(e -> Mono.empty()));
    }

    public Mono<Boolean> deleteService(String id) {
        return redisTemplate.opsForSet().remove(SERVICE_IDS, id)
                .then(redisTemplate.delete(SERVICE_KEY + id))
                .map(count -> count > 0)
                .doOnNext(deleted -> {
                    if (Boolean.TRUE.equals(deleted)) {
                        eventPublisher.publishEvent(new PlatformConfigChangedEvent("service", id));
                    }
                });
    }

    public Mono<RateLimitPolicy> savePolicy(RateLimitPolicy policy) {
        return write(POLICY_KEY + policy.getId(), policy)
                .then(redisTemplate.opsForSet().add(POLICY_IDS, policy.getId()).then())
                .then(Mono.fromRunnable(() -> eventPublisher.publishEvent(
                        new PlatformConfigChangedEvent("policy", policy.getId()))))
                .thenReturn(policy);
    }

    public Mono<RateLimitPolicy> findPolicy(String id) {
        return read(POLICY_KEY + id, RateLimitPolicy.class);
    }

    public Flux<RateLimitPolicy> findAllPolicies() {
        return redisTemplate.opsForSet().members(POLICY_IDS)
                .flatMap(id -> findPolicy(id).onErrorResume(e -> Mono.empty()));
    }

    public Mono<Boolean> deletePolicy(String id) {
        return redisTemplate.opsForSet().remove(POLICY_IDS, id)
                .then(redisTemplate.delete(POLICY_KEY + id))
                .map(count -> count > 0)
                .doOnNext(deleted -> {
                    if (Boolean.TRUE.equals(deleted)) {
                        eventPublisher.publishEvent(new PlatformConfigChangedEvent("policy", id));
                    }
                });
    }

    public Mono<PlatformAlert> saveAlert(PlatformAlert alert) {
        return redisTemplate.opsForList().leftPush(ALERTS_RECENT, serialize(alert))
                .then(redisTemplate.opsForList().trim(ALERTS_RECENT, 0, 99))
                .thenReturn(alert);
    }

    public Flux<PlatformAlert> findRecentAlerts(int limit) {
        return redisTemplate.opsForList().range(ALERTS_RECENT, 0, limit - 1)
                .flatMap(json -> Mono.fromCallable(() -> objectMapper.readValue(json, PlatformAlert.class)));
    }

    public Mono<Long> incrementAllowed(String tenantId, String clientId) {
        String key = USAGE_ALLOWED + tenantId + ":" + clientId;
        return redisTemplate.opsForValue().increment(key)
                .flatMap(count -> redisTemplate.expire(key, Duration.ofDays(1)).thenReturn(count));
    }

    public Mono<Long> incrementDenied(String tenantId, String clientId) {
        String key = USAGE_DENIED + tenantId + ":" + clientId;
        return redisTemplate.opsForValue().increment(key)
                .flatMap(count -> redisTemplate.expire(key, Duration.ofDays(1)).thenReturn(count));
    }

    public Mono<Long> getAllowedCount(String tenantId, String clientId) {
        return redisTemplate.opsForValue().get(USAGE_ALLOWED + tenantId + ":" + clientId)
                .flatMap(val -> {
                    try {
                        return Mono.just(Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        return Mono.just(0L);
                    }
                })
                .defaultIfEmpty(0L);
    }

    public Mono<Long> getDeniedCount(String tenantId, String clientId) {
        return redisTemplate.opsForValue().get(USAGE_DENIED + tenantId + ":" + clientId)
                .flatMap(val -> {
                    try {
                        return Mono.just(Long.parseLong(val));
                    } catch (NumberFormatException e) {
                        return Mono.just(0L);
                    }
                })
                .defaultIfEmpty(0L);
    }

    private <T> Mono<Void> write(String key, T value) {
        return redisTemplate.opsForValue().set(key, serialize(value)).then();
    }

    private <T> Mono<T> read(String key, Class<T> type) {
        return redisTemplate.opsForValue().get(key)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Not found: " + key)))
                .flatMap(json -> Mono.fromCallable(() -> objectMapper.readValue(json, type)));
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + value.getClass().getSimpleName(), e);
        }
    }

    public static String generateApiKey() {
        return "rlk_" + UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * One-way hash for secure storage/lookup of API keys. Raw key never persisted.
     * Uses unsalted SHA-256 (sufficient here because keys have high entropy from UUID;
     * a Redis leak would still require brute force against high-entropy values).
     */
    public static String hashApiKey(String rawKey) {
        if (rawKey == null) {
            return null;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(rawKey.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hashBytes.length * 2);
            for (byte b : hashBytes) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available for API key hashing", e);
        }
    }
}