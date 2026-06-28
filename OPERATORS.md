# Operator Guide

This guide explains how to onboard services, manage tenants, and operate the rate limiting platform.

## Quick Start

```bash
docker-compose up --build -d
```

Open:
- Dashboard: http://localhost:8080/platform/dashboard
- Prometheus metrics: http://localhost:8080/actuator/prometheus
- Health: http://localhost:8080/actuator/health

Default admin key: `changeme` (override with `ADMIN_API_KEY`).

## Authentication

Admin mutations require header:

```
X-Admin-Key: changeme
```

## 1. Onboard a Tenant

```bash
curl -X POST http://localhost:8080/platform/admin/tenants \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: changeme" \
  -d '{
    "id": "acme-corp",
    "name": "Acme Corp",
    "plan": "PRO",
    "defaultReplenishRate": 50,
    "defaultBurstCapacity": 100,
    "enabled": true
  }'
```

## 2. Issue API Keys

```bash
curl -X POST http://localhost:8080/platform/admin/tenants/acme-corp/api-keys \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: changeme" \
  -d '{"name": "production-app"}'
```

Clients send `X-API-Key: <returned-key>` on requests.

## 3. Register a Service (Dynamic Route)

```bash
curl -X POST http://localhost:8080/platform/admin/services \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: changeme" \
  -d '{
    "id": "orders-service",
    "name": "Orders API",
    "uri": "http://httpbin.org:80",
    "pathPrefix": "/orders/**",
    "replenishRate": 20,
    "burstCapacity": 40,
    "circuitBreakerEnabled": true,
    "enabled": true
  }'
```

Routes are stored in Redis and hot-reloaded. No gateway restart required.

## 4. Tenant-Specific Policy Override

```bash
curl -X POST http://localhost:8080/platform/admin/policies \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: changeme" \
  -d '{
    "id": "acme-orders-boost",
    "routeId": "orders-service",
    "tenantId": "acme-corp",
    "replenishRate": 100,
    "burstCapacity": 200,
    "enabled": true
  }'
```

Policy resolution order:
1. Tenant-specific policy for route
2. Service default limits
3. Tenant default limits
4. Platform default (10 rps / burst 20)

## 5. Client Identity Priority

The gateway resolves identity in this order:
1. `X-API-Key` header
2. `Authorization: Bearer <JWT>` (`sub` + `tenant_id` claim)
3. `X-Tenant-Id` header + client IP
4. Client IP only (`anonymous` tenant)

## 6. Observe Incidents

Recent alerts:
```bash
curl http://localhost:8080/platform/api/alerts/recent
```

Per-tenant usage:
```bash
curl http://localhost:8080/platform/api/usage/acme-corp?clientId=rlk_abc123
```

## 7. Production Checklist

- Set `ADMIN_API_KEY` to a strong secret
- Set `JWT_SECRET` to at least 32 characters
- Set `RATE_LIMITER_TRUST_FORWARDED_HEADERS=true` only behind trusted proxies
- Keep `rate-limiter.fail-open=false` unless you explicitly want outage bypass
- Monitor `rate_limit_denied_total` via Prometheus
- Scale Redis for HA (Sentinel/Cluster) for production control-plane + enforcement data