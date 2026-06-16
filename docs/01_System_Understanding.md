# System Understanding — Retail Media Streaming Platform

> **Context:** This document captures what we built, maps it back to the interview assignment requirements, and outlines the design decisions behind each choice.

---

## 1. What We Built

A real-time retail media platform with **3 microservices**, **3 infrastructure dependencies**, and **6 .NET projects** following Clean Architecture.

### Services Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ EventCollector│────▶│    Kafka     │────▶│StreamProcessor│
│  POST /events │     │ raw-events   │     │  Background   │
│  (ASP.NET)    │     │              │     │  Worker (.NET)│
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                 │
                                   ┌─────────────┼─────────────┐
                                   ▼             ▼             ▼
                              ┌─────────┐  ┌──────────┐  ┌──────────┐
                              │  Redis  │  │PostgreSQL│  │   API    │
                              │Counters │  │ Campaign │  │ Insights │
                              │+Session │  │ Metrics  │  │  .NET    │
                              └─────────┘  └──────────┘  └──────────┘
```

| Service | Role | Port |
|---------|------|------|
| **EventCollector** | Ingestion gateway — validates events, publishes to Kafka | 5001 |
| **StreamProcessor** | Background consumer — routes events to handlers, updates Redis + PostgreSQL | — |
| **RetailMedia.Api** | Insights API — serves campaign metrics combining Redis + PostgreSQL | 5000 |

### Project Structure (Clean Architecture)

```
src/
├── RetailMedia.Domain/           # Layer 0: Entities, Value Objects, Interfaces
├── RetailMedia.Application/      # Layer 1: Use cases, DTOs, Service interfaces
├── RetailMedia.Infrastructure/   # Layer 2: EF Core, Kafka, Redis implementations
├── RetailMedia.Api/              # Layer 3a: Insights API (ASP.NET Web API)
├── RetailMedia.EventCollector/   # Layer 3b: Event ingestion (ASP.NET Web API)
└── RetailMedia.StreamProcessor/  # Layer 3c: Kafka consumer worker (.NET Worker)
```

---

## 2. Requirements Mapping

### Event Ingestion

**Requirement:** Handle high-velocity event streams with minimal latency.

**Implementation:** `EventCollector` exposes `POST /events` which validates the payload, maps to a domain `Event`, and publishes to Kafka topic `raw-events`. Returns **202 Accepted** immediately — the client does not wait for processing.

```csharp
// EventIngestionService.cs — validates, creates domain event, publishes to Kafka
await _kafkaProducer.PublishAsync(@event, ct);
```

**Why this works:** Kafka acts as a durable buffer. The ingestion path is a single Kafka producer call (~1-5ms), well under the 100ms target. The collector can be horizontally scaled behind a load balancer.

### Data Processing

**Requirement:** Real-time event processing with stateful operations (user session tracking).

**Implementation:** `StreamProcessor` runs a `BackgroundService` that consumes from Kafka and routes events by type:

| Event Type | Handler(s) | Stateful? | What Happens |
|-----------|-----------|-----------|-------------|
| `AdImpression` | `ImpressionHandler` | No | `INCR` Redis counter |
| `AdClick` | `ClickHandler` + `AttributionHandler.HandleClickAsync` | Yes | `INCR` Redis counter + store session with 30-min TTL |
| `AddToCart` | `AttributionHandler.HandleAddToCartAsync` | Yes | Check session → if within window, `INCR` clickToBasket counter |
| `ProductView` | Default (no-op) | No | Logged and skipped |
| `Purchase` | Default (no-op) | No | Logged and skipped |

**Attribution window:** 30-minute session stored in Redis with TTL:
```
Key:   session:{tenantId}:{userId}
Value: {"campaignId":"...", "timestamp":"..."}
TTL:   1800 seconds (30 min)
```

**Why this works:** Redis `INCR` is atomic and sub-millisecond. The session-based attribution is lightweight — no complex windowing logic needed at this scope.

### Data Storage

**Requirement:** Support both real-time querying and batch analytics.

**Implementation:**

| Store | Purpose | Data |
|-------|---------|------|
| **Redis** | Real-time counters + session state | `campaign:{id}:clicks`, `campaign:{id}:impressions`, `campaign:{id}:clickToBasket`, `session:{tenant}:{user}` |
| **PostgreSQL** | Persisted aggregates + entity data | `Campaigns`, `Events`, `CampaignMetrics` tables |

**Read strategy (Cache-Aside):** The Insights API reads Redis first, then falls back to PostgreSQL SUM, and returns the combined total:
```csharp
// InsightsService.cs — for each metric
var redisCount = await _cache.GetCounterAsync(redisKey);
var dbCount = await _metricsRepo.GetCountAsync(campaignId, tenantId, ct);
return new ClickResponse(campaignId.ToString(), redisCount + dbCount, timestamp);
```

**Why this works:** Redis gives sub-millisecond reads. PostgreSQL provides durable storage with strong consistency. The cache-aside pattern means we never serve stale data — we always combine both sources.

### Insights API

**Requirement:** APIs for clicks, impressions, click-to-basket with real-time + historical support.

**Implementation:**

| Endpoint | Redis? | PostgreSQL? | Real-time? |
|----------|--------|------------|-----------|
| `GET /ad/{id}/clicks` | ✅ Counter read | ✅ SUM aggregation | Yes |
| `GET /ad/{id}/impressions` | ✅ Counter read | ✅ SUM aggregation | Yes |
| `GET /ad/{id}/clickToBasket` | ✅ Counter read | ✅ SUM aggregation | Yes |
| `GET /ad/{id}/metrics?metric=&startDate=&endDate=` | ❌ | ✅ EF Core filtered query | No (DB only) |

All responses wrapped in a consistent envelope:
```json
{
  "data": { ... },
  "meta": { "timestamp": "2026-06-16T12:00:00Z" }
}
```

### Multi-Tenancy

**Requirement:** Support multiple retailers with data isolation.

**Implementation:**
- `TenantContextMiddleware` extracts tenant from JWT claim `tenantId` or `X-Tenant-Id` header
- `TenantContext` is scoped per request (`ITenantContext.CurrentTenantId`)
- All repository queries filter by `TenantId`
- `TenantId` is a typed Value Object (not a raw string)

```csharp
// TenantContextMiddleware.cs
var claim = context.User?.FindFirst("tenantId")?.Value;
var header = context.Request.Headers["X-Tenant-Id"].FirstOrDefault();
```

### Scalability

**Requirement:** Horizontal scalability during traffic spikes.

**Implementation:**
- **Kafka partitioning:** Events keyed by `tenantId:campaignId` — ordering guarantee per campaign, even distribution
- **Kubernetes HPA:** 3-20 replicas, triggers at 70% CPU / 80% memory
- **Stateless services:** EventCollector and API can scale horizontally; StreamProcessor scales with Kafka partitions
- **K8s manifests** provided for all services (`k8s/`)

### Monitoring

**Requirement:** Key metrics + tools for logging and diagnostics.

**Implementation:**
- `/healthz` health check endpoint on all services
- Structured logging via `ILogger<T>` with event IDs
- Docker health checks configured in compose file
- K8s liveness/readiness probes configured

---

## 3. Key Design Decisions

| Decision | Choice | Why (Not the Alternative) |
|----------|--------|--------------------------|
| **Stream processing** | .NET BackgroundService (not Flink) | Interview scope — demonstrates same concepts (consume → process → state) without Flink infrastructure. The BackgroundService still shows stateful windowing, event routing, and horizontal consumption. |
| **OLTP database** | PostgreSQL (not Cassandra) | Strong consistency, relational model for campaign data, EF Core migrations. Cassandra would be better at 100K+ writes/sec but adds operational complexity. |
| **Cache** | Redis (not Memcached) | Rich data structures (hashes for sessions), TTL, atomic INCR. Memcached is simpler but lacks these features. |
| **API style** | Minimal API (not MVC) | Less ceremony, same DI/testability. Controllers add no value for 5 endpoints. |
| **ORM** | EF Core + Dapper hybrid | EF Core for CRUD + migrations; Dapper for raw SUM aggregates (avoids EF query overhead). |
| **Multi-tenancy** | Shared DB with tenantId column | Lower ops cost. Dedicated DB per tenant for enterprise retailers if needed. |
| **Event schema** | JSON over Kafka | Simple, debuggable, schema-less. Avro/Protobuf for production (schema registry, compression). |

---

## 4. Current Limitations (Acknowledged)

These are intentional scope boundaries — not design flaws:

| Gap | Why It's Missing | Production Fix |
|-----|-----------------|----------------|
| **Apache Flink** | .NET BackgroundService sufficient for scope | Replace with Flink for exactly-once, event-time processing, backpressure handling |
| **Data Lake (S3)** | Not required for API demo | Sink Kafka → S3 via Kafka Connect or Flink for raw event archival |
| **BigQuery** | No analytics requirement in MVP | Stream aggregates → BigQuery for historical/BI queries |
| **RedisFlushService** | Removed — replaced by write-along | Handlers now write to both Redis and PostgreSQL at event time. No periodic flush needed. |
| **Purchase/ProductView handlers** | Not in API requirements | Add handlers if attribution models need them |
| **Exactly-once processing** | Kafka with at-least-once + manual commit | Idempotent writes + transactional Kafka or Flink checkpoints |
| **Authentication/Authorization** | Not in scope | Add JWT bearer auth + per-tenant API keys |
| **Rate limiting** | Not implemented | Add middleware with sliding window (Redis-based) |
| **CI/CD actual deployment** | Workflows defined but not connected to live infra | Configure GKE/GCR integration in CD workflow |
| **OpenTelemetry tracing** | Referenced but not wired | Add OTel SDK + exporter to all services |

---

## 5. How the Code Demonstrates Interview Criteria

| Criterion | Evidence |
|-----------|----------|
| **Clean Architecture** | 4-layer separation: Domain (zero deps) → Application → Infrastructure → Presentation |
| **SOLID Principles** | Single Responsibility (handler per event type), Dependency Inversion (Domain defines interfaces, Infrastructure implements), Open/Closed (new event types = new handler, no switch modification in domain) |
| **Dependency Injection** | All services registered in `DependencyInjection.cs` — Application layer adds scoped services, Infrastructure adds singletons (Kafka, Redis) and scoped (DbContext, repositories) |
| **Testing** | xUnit + Moq test projects for API, Application, and StreamProcessor |
| **Logging** | Structured `ILogger<T>` throughout — event IDs, key-value pairs |
| **Containerization** | Multi-stage Dockerfile with distroless runtime image |
| **Orchestration** | K8s manifests with HPA, liveness/readiness probes, configmaps |
| **CI/CD** | GitHub Actions: `dotnet build → test → docker build → push` |
