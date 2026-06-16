# Scope for Extension — Retail Media Streaming Platform

> **Context:** This document outlines the production-grade extensions we would add beyond the interview case study. It directly addresses fan-in/fan-out patterns, infrastructure gaps, and future enhancements — demonstrating architectural thinking for the Technical Leadership Round.

---

## 1. Fan-In Patterns

### Current State

Only one ingestion path: `POST /events` on the EventCollector.

### Production Extension

Multiple event sources all converging (fanning-in) to the same Kafka topic:

```
┌─────────────────┐
│  Web SDK        │────┐
│  (Browser)      │    │
└─────────────────┘    │
┌─────────────────┐    │    ┌──────────────┐    ┌──────────────────────┐
│  Mobile SDK     │────┼───▶│   Kafka      │───▶│   Event Collector    │
│  (iOS/Android)  │    │    │ raw-events   │    │   (Server-side)      │
└─────────────────┘    │    └──────────────┘    └──────────────────────┘
┌─────────────────┐    │
│  Server-Side    │────┘
│  (Webhook/Batch)│
└─────────────────┘
```

**Why this pattern matters:**
- SDKs send events directly to a lightweight proxy or load balancer, not to the EventCollector
- The EventCollector becomes a **server-side ingestion service** that also handles webhook integrations and batch uploads
- All sources converge into the same Kafka topic regardless of origin

**Implementation considerations:**
- **Event deduplication** at ingress: check `eventId` against a Redis Bloom filter or short-lived cache before acknowledging
- **Schema validation per source**: SDK events may have different fields than server-side events — validate at the edge before publishing to Kafka
- **Client-side buffering**: SDKs should buffer events locally and retry on failure. Use a compact serialization format (Avro/Protobuf) over the wire
- **Protocol adaptation**: Accept gRPC for mobile, HTTP for web, and file upload for batch — all normalize to the same Kafka message format

### Multi-Region Fan-In

```
┌──────────────┐     ┌──────────────┐
│  US Region   │────▶│   Kafka      │
│  (us-east1)  │     │   Aggregator │
└──────────────┘     └──────┬───────┘
                            │
┌──────────────┐            │    Mirrored via
│  EU Region   │────────────┘    Kafka MirrorMaker
│  (europe-w1) │                 or Confluent Cluster
└──────────────┘                 Linking
                            │
┌──────────────┐            │
│  APAC Region │────────────┘
│  (asia-s1)   │
└──────────────┘
```

**Why:** A global retailer needs events from all regions processed centrally for unified campaign metrics.

**Trade-off:** Cross-region replication adds latency (100-300ms). Acceptable for metrics that are not time-critical. Regional sub-aggregates can serve local dashboards with lower latency.

---

## 2. Fan-Out Patterns

### Current State

Single consumer group (`retail-media-processor`) processes all events in one linear pipeline.

### Production Extension — Multi-Sink Fan-Out

One event in Kafka → multiple independent consumers, each doing something different:

```
                           ┌──────────────────┐
                           │  Consumer Group A │
                           │  Real-Time        │
                           │  Counters → Redis │
                           └──────────────────┘
                           │
                           ├──────────────────┐
                           │  Consumer Group B │
Kafka ────────────────────▶│  Raw Event Sink   │
(raw-events)               │  → S3 (Parquet)   │
                           │  → Data Lake      │
                           ├──────────────────┘
                           │
                           ├──────────────────┐
                           │  Consumer Group C │
                           │  Flink Processor  │
                           │  → Aggregations   │
                           │  → BigQuery       │
                           └──────────────────┘
```

**Why this matters (interview talking points):**
- **Independent scaling**: Each consumer group can scale independently. If the S3 sink is slow, it doesn't block the real-time counters
- **Independent failures**: A bug in the analytics pipeline doesn't affect ad serving metrics
- **Independent schemas**: Each sink can evolve its schema independently

**Implementation:**
- Each consumer group has its own `group.id` and offset commits
- Kafka retains messages for 7 days (configurable), so a late-joining consumer can replay from any point
- Use Kafka Connect for the S3 sink (Confluent S3 Sink Connector with Parquet format) — no custom code needed

### Processing Fan-Out — Flink Side-Outputs

```
                        ┌──────────────────────────────┐
                        │      Flink Pipeline           │
                        │                              │
Kafka ───▶ Source ───▶ KeyBy(tenantId:campaignId)      │
                        │                    │          │
                        │                    ▼          │
                        │          ProcessFunction      │
                        │           (Stateful)          │
                        │         ┌──────────┐          │
                        │         │ Main     │─────────▶│───▶ Click Count → Redis
                        │         │ Output   │          │
                        │         └──────────┘          │
                        │              │                │
                        │      Side Outputs             │
                        │         ┌──────────┐          │
                        │         │ Late     │─────────▶│───▶ Dead Letter Queue
                        │         │ Events   │          │
                        │         └──────────┘          │
                        │         ┌──────────┐          │
                        │         │ Audit    │─────────▶│───▶ Audit Log → S3
                        │         │ Trail    │          │
                        │         └──────────┘          │
                        └──────────────────────────────┘
```

**Why replace the BackgroundService with Flink:**
| Feature | BackgroundService | Flink |
|---------|------------------|-------|
| **State management** | Manual Redis sessions | KeyedState with RocksDB backend, automatic recovery |
| **Event-time processing** | Not supported | Native event-time semantics with watermarks |
| **Exactly-once** | At-least-once (manual commit) | Exactly-once via checkpoints + two-phase commit |
| **Backpressure** | Not handled | Automatic backpressure propagation |
| **Failure recovery** | Manual rebalance | Stateful snapshot/restore |
| **Windowing** | Manual implementation | Tumbling, sliding, session windows built-in |

---

## 3. Data Architecture Extensions

### Hot → Warm → Cold Tiered Storage

```
┌─────────────┐    ┌──────────────┐    ┌───────────────┐
│   Hot       │    │   Warm       │    │   Cold        │
│   Redis     │    │  PostgreSQL  │    │  S3 / GCS     │
│   +         │───▶│  +           │───▶│  +            │
│   PostgreSQL│    │  BigQuery    │    │  Glacier      │
│   30 days   │    │  12 months   │    │  Multi-year   │
└─────────────┘    └──────────────┘    └───────────────┘
```

**Data flow:**
1. Events land in Kafka → Redis (real-time counters)
2. Every 30 seconds → flush Redis to PostgreSQL (counter checkpoints)
3. Every hour → batch upsert PostgreSQL aggregates to BigQuery
4. Raw events archived to S3 as Parquet files (partitioned by `tenant/year/month/day/hour`)
5. After 12 months in BigQuery → move cold data to S3 Glacier

### Data Lake Schema

```
s3://retail-media-data-lake/
  raw-events/
    tenant=tenant-retail-01/
      year=2026/
        month=06/
          day=16/
            hour=12/
              events-0001.parquet
              events-0002.parquet
  aggregated/
    tenant=tenant-retail-01/
      year=2026/
        month=06/
          day=16/
            campaign-metrics.parquet
```

**Parquet schema for raw events:**
| Column | Type | Description |
|--------|------|-------------|
| `eventId` | STRING | Unique event ID (used for dedup) |
| `tenantId` | STRING | Tenant/retailer identifier |
| `userId` | STRING | Anonymous/pseudonymous user ID |
| `campaignId` | STRING | Campaign identifier |
| `eventType` | STRING | ProductView, AddToCart, Purchase, AdImpression, AdClick |
| `timestamp` | TIMESTAMP | Event timestamp (source time) |
| `metadata` | STRUCT<...> | Flexible metadata payload |
| `ingestionTime` | TIMESTAMP | When Kafka received the event |
| `source` | STRING | SDK/Webhook/Batch |

---

## 4. Production-Grade Extensions

### 4.1 RedisFlushService (Implement the Stub)

**Current:** `RedisFlushService` is a stub — it runs every 30 seconds but does nothing.

**Production implementation:**
```
1. SCAN Redis for keys matching "campaign:*:{metric}" pattern
2. For each key:
   a. GET + GETSET(campaign:{id}:{metric}, 0) — atomic read-and-reset
   b. Upsert Count into PostgreSQL CampaignMetrics table
   c. Log flushed count
3. Track flush timestamp for monitoring
```

**Why GETSET instead of GET+DEL:** Atomicity — no lost counts between the read and reset.

### 4.2 Event Handlers for All Types

**Current gap:** `ProductView` and `Purchase` events hit the `default` case and are silently dropped.

**Production addition:**

| Event Type | Handler | Why |
|-----------|---------|-----|
| `ProductView` | `ProductViewHandler` | Track product-level interest, build recommendation signals |
| `Purchase` | `PurchaseHandler` | Measure ROAS (Return on Ad Spend) — the ultimate conversion metric |
| `AddToCart` (already done) | — | Attribution to prior click |
| `AdClick` (already done) | — | Counter + session |
| `AdImpression` (already done) | — | Counter |

Purchase handling would require a more sophisticated attribution model:
- **Last-click attribution** (current): 100% credit to last clicked campaign within 30 min
- **Multi-touch attribution**: Distribute credit across all campaigns in the user journey (linear, time-decay, position-based)
- **View-through attribution**: Count conversions even if user only saw an impression (not clicked)

### 4.3 Exactly-Once Semantics

**Current:** At-least-once delivery (Kafka with manual commit after processing).

**Risks:**
- If processor crashes between Redis INCR and Kafka commit → on restart, the same event is re-processed → **over-counting**
- Redis INCR is not idempotent (INCR 1 → INCR 1 → value is 2, not 1)

**Production fixes:**
1. **Idempotent writes:** Use `eventId` as a idempotency key. Before counting, check if `eventId` has been processed (Bloom filter in Redis, or a dedup table in PostgreSQL)
2. **Flink exactly-once:** Flink's Kafka connector supports exactly-once via two-phase commit (checkpoint barriers → transactional Kafka producers)
3. **Transactional outbox:** EventCollector writes events to PostgreSQL first, then a separate process publishes to Kafka. If Kafka fails, the event is still in the outbox for retry

### 4.4 Enhanced Metrics Endpoint

**Current gap:** `GET /ad/{id}/metrics` only queries PostgreSQL — it misses real-time Redis counters.

**Fix:**
```csharp
public async Task<MetricsResponse> GetMetricsAsync(...)
{
    // Same as today — query PostgreSQL with filters
    var dbMetrics = await _metricsRepo.GetMetricsAsync(...);

    // NEW: Add Redis counters for the same period
    // (Redis doesn't have date filtering, so we return the raw counter
    //  and let the consumer decide)
    var redisClicks = await _cache.GetCounterAsync("campaign:{id}:clicks");
    var redisImpressions = await _cache.GetCounterAsync("campaign:{id}:impressions");
    var redisClickToBasket = await _cache.GetCounterAsync("campaign:{id}:clickToBasket");

    return new MetricsResponse(
        campaignId.ToString(),
        (dbClicks ?? 0) + redisClicks,       // combined
        (dbImpressions ?? 0) + redisImpressions,
        (dbClickToBasket ?? 0) + redisClickToBasket,
        startDate, endDate);
}
```

### 4.5 Authentication & Authorization

**Current:** No auth — tenant is identified via header but not validated.

**Production implementation:**
- **JWT Bearer auth** on all endpoints (ASP.NET Core `AddAuthentication().AddJwtBearer()`)
- JWT contains `tenantId` claim + `role` claim
- `TenantContextMiddleware` validates the token and extracts claims
- Per-tenant API keys for server-to-server integration
- Rate limiting by API key / tenant (sliding window in Redis)

### 4.6 Observability Stack

**Current:** Structured logging + `/healthz`.

**Production additions:**

| Tool | Purpose |
|------|---------|
| **Prometheus** | Metrics collection (request latency, error rate, Kafka consumer lag, Redis ops/sec) |
| **Grafana** | Dashboards per tenant — real-time campaign performance |
| **OpenTelemetry** | Distributed tracing across EventCollector → Kafka → StreamProcessor → Redis/PostgreSQL |
| **ELK / Grafana Loki** | Centralized log aggregation |
| **PagerDuty / OpsGenie** | Alerting on consumer lag > threshold, error rate spikes |

**Key metrics to monitor (from the assignment):**

| Metric | Where | Alert Threshold |
|--------|-------|----------------|
| Kafka consumer lag | StreamProcessor | > 10,000 messages |
| P95 API latency | RetailMedia.Api | > 200ms |
| Event ingestion rate | EventCollector | Drop > 20% from baseline |
| Redis memory usage | Redis | > 80% of maxmemory |
| PostgreSQL connection pool | PostgreSQL | > 80% of max connections |
| Error rate (5xx) | All services | > 1% of requests |

### 4.7 Cost Optimization

**Strategy:**

| Layer | Cost Driver | Optimization |
|-------|-------------|-------------|
| **Kafka** | Storage (retention) | 7-day retention for raw events, then move to S3 |
| **Redis** | Memory (counters + sessions) | Smaller TTLs for sessions, eviction policy: allkeys-lru |
| **PostgreSQL** | Storage + connections | Read replicas for API, connection pooling with PgBouncer |
| **K8s** | Compute (node hours) | Spot/preemptible VMs for StreamProcessor; HPA to scale down at night |
| **S3** | Storage | Lifecycle policy: Standard → Infrequent Access → Glacier |
| **BigQuery** | Query costs | Materialized views for dashboards, avoid SELECT * |

---

## 5. Fan-In vs Fan-Out Summary

| Pattern | Our Implementation | Production Extension |
|---------|-------------------|---------------------|
| **Fan-in (sources)** | Single `POST /events` endpoint | Web SDK + Mobile SDK + Server-side + Batch → Kafka |
| **Fan-in (regions)** | Single region | Multi-region Kafka → MirrorMaker → Central |
| **Fan-out (sinks)** | Single consumer group | 3+ consumer groups: Counters, S3 Archive, BigQuery Analytics |
| **Fan-out (processing)** | Switch statement | Flink side-outputs for main + late events + audit trail |
| **Fan-out (scale)** | One StreamProcessor | Kafka partition scaling + HPA on all services |

---

## 6. Future Enhancements (Interview Slide)

Beyond the production gaps above, the platform would benefit from:

1. **Real-time dashboards** — WebSocket/SSE push to marketer dashboards
2. **ML-based attribution** — Multi-touch attribution models (linear, time-decay, algorithmic)
3. **Budget pacing** — Real-time campaign budget checks: if budget exhausted, stop serving ads
4. **Fraud detection** — Anomaly detection on click patterns (bot detection, click farms)
5. **A/B testing** — Campaign experiment framework with statistical significance checks
6. **Audience segmentation** — Build user segments based on event history (look-alike modeling)
7. **Self-serve reporting** — Allow retailers to define custom metrics and dashboards

---

## 7. Interview Talking Points

### Why Not Flink in the MVP?

"The assignment asked for stream processing. I chose a .NET BackgroundService over Flink because:
- **Same conceptual model**: consume → process → state → sink
- **No infrastructure burden**: No Flink cluster, no job manager, no checkpoint storage
- **Team alignment**: The team is .NET-focused — a BackgroundService is maintainable by them
- **Scope-appropriate**: For 50M events/day, a single BackgroundService handles the load; Flink becomes necessary at 500M+ events/day with complex windowing
- **Migration path**: The domain logic (handlers, Redis state, PostgreSQL persistence) is already separated. Migrating to Flink means rewriting only the orchestration layer — the business logic stays the same"

### Why PostgreSQL Over Cassandra?

"PostgreSQL gives us:
- Strong consistency for campaign metadata
- EF Core migrations for schema evolution
- Dapper for high-performance read queries
- Team familiarity (the entire .NET ecosystem)

We'd move to Cassandra when:
- Write throughput exceeds 100K writes/second (PostgreSQL starts struggling)
- We need multi-region writes with conflict resolution
- The data model becomes simple key-value (no joins needed)

Until then, PostgreSQL with read replicas is cheaper and more maintainable."

### How Would You Handle 10x Black Friday Traffic?

"Three strategies working together:
1. **Kafka absorbs the spike** — producers publish at full speed, consumers catch up when traffic subsides. Kafka's retention (7 days) gives us a buffer.
2. **Auto-scaling** — K8s HPA scales API and collector pods based on CPU/request count. StreamProcessor scales with Kafka partitions.
3. **Degraded modes** — If PostgreSQL write capacity is saturated, we fall back to Redis-only mode (counters only, no DB flush). Reads still work. PostgreSQL catches up when traffic normalizes.

The key insight: don't try to handle peak traffic with peak infrastructure. Use Kafka as a shock absorber and plan for eventual consistency."
