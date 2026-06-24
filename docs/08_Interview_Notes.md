# Interview Notes & Gotchas — Retail Media Streaming Platform

> A cheat sheet for the interview: how to frame the architecture, the sharp questions an interviewer is likely to drill into, and honest answers grounded in this codebase.

---

## 1. One-line framing

> "It's **Clean Architecture** (Ports & Adapters). Inside it I use **tactical DDD** (entities + value objects), the **Repository pattern** as ports, **Dependency Inversion** via DI, and an **event-driven pipeline** (Kafka pub/sub) with **cache-aside** reads."

Layers (dependency rule points inward): `Domain` (zero deps) → `Application` → `Infrastructure` → `Presentation` (`Api` / `EventCollector` / `StreamProcessor`).

**Say "tactical DDD", not "full DDD".** Present: Entities with behavior (`Campaign.Deactivate/Rename`, private setters), Value Objects (`CampaignId`, `TenantId`). Absent: explicit Aggregates/Aggregate Roots, Domain Events, an explicit Unit of Work (EF's `DbContext` already is one).

---

## 2. Pattern stack (name them precisely)

| Pattern | Where |
|---------|-------|
| Clean Architecture / Hexagonal (Ports & Adapters) | Project/layer split; `Domain.Interfaces` = ports |
| Tactical DDD — Entities & Value Objects | `Domain/Entities`, `Domain/ValueObjects` |
| Repository | `I*Repository` (Domain) → `*Repository` (Infrastructure) |
| Dependency Inversion / DI | `DependencyInjection.cs` |
| Event-driven / Pub-Sub | Kafka `raw-events` topic |
| Strategy / handler-per-type | `ClickHandler`, `ImpressionHandler`, `AttributionHandler` |
| Cache-aside (read) + Write-along (write) | `InsightsService` reads; handlers write both stores |
| Factory | `CampaignId.From` / `TenantId.From` |
| Middleware | `TenantContextMiddleware`, `ErrorHandlingMiddleware` |

---

## 3. Gotcha #1 — Double counting (most likely deep-dive)

**The bug.** Every event is written to **both** stores, and the API **sums both**:

- `ClickHandler.HandleAsync` → `INCR campaign:{id}:clicks` (Redis, 24h TTL) **and** upsert `CampaignMetrics` (`ClickHandler.cs:22-26`).
- `InsightsService.GetClicksAsync` → `redisCount + dbCount` (`InsightsService.cs:23-25`).

So **one** `AdClick` makes `/ad/{id}/clicks` return **2**, not 1. Same for impressions and clickToBasket. It self-heals after the Redis key's 24h TTL expires (then Redis=0, API returns DB-only).

**Why it can look like it's NOT happening:**
- `/ad/{id}/metrics` reads **DB only** (no Redis) → returns the correct `1`. Testing this endpoint hides the bug.
- The README E2E walkthrough states expected `clicks → 1` — that doc is wrong vs. the code (it would be `2`).

**Deterministic proof (run locally):**
```bash
# clean slate
docker exec retail-redis redis-cli FLUSHDB
docker exec retail-postgres psql -U retail -d retail_media -c 'DELETE FROM "CampaignMetrics"; DELETE FROM "Events";'

# send exactly ONE click
curl -X POST http://localhost:5001/events -H "Content-Type: application/json" \
  -d '{"eventId":"dc_1","tenantId":"tesco","userId":"alice","campaignId":"cmp_dc","eventType":"AdClick","timestamp":"2026-06-16T10:00:00Z"}'

sleep 2   # let the processor consume

docker exec retail-redis redis-cli GET "campaign:cmp_dc:clicks"     # → 1   (Redis truth)
docker exec retail-postgres psql -U retail -d retail_media -c \
  "SELECT COALESCE(SUM(\"Count\"),0) FROM \"CampaignMetrics\" WHERE \"Metric\"='Clicks';"   # → 1   (DB truth)

curl -H "X-Tenant-Id: tesco" http://localhost:5000/ad/cmp_dc/clicks      # → 2   ⬅ DOUBLE COUNT
curl -H "X-Tenant-Id: tesco" http://localhost:5000/ad/cmp_dc/metrics     # → clicks: 1  (DB-only, correct)
```
If `/clicks` shows `2` while Redis=1 and DB=1, the double-count is proven. (EventCollector port: `5001` in docker-compose, `5229` in the README's local-run section — use whichever your run prints.)

**How to answer it well:**
1. Acknowledge the root cause: Redis and Postgres each hold the **full** count, and the read **adds** them.
2. Name the intended model that would make summing valid: **Redis = un-flushed hot delta, Postgres = flushed history** (sum only when they're disjoint). The current write-along writes the full count to both, breaking that assumption.
3. Fix options:
   - **Single source of truth on read** — read Redis (fast) and fall back to Postgres only on a cache miss; never add them.
   - Or **Postgres = source of truth**, Redis = pure cache populated from it.
   - Or keep write-along but make Redis store only the **delta since last flush** (then sum is correct) — i.e., a real flush/reconciliation job.
4. Bonus: call out the **endpoint inconsistency** — `/clicks` (Redis+DB) vs `/metrics` (DB-only) can disagree; they should share one read path.

---

## 4. Other sharp questions to rehearse

| Topic | Honest answer |
|-------|---------------|
| **Delivery semantics** | At-least-once. `EnableAutoCommit=false`, `Commit()` after processing (`KafkaEventConsumer.cs`). Redelivery → counters double-increment. |
| **Idempotency** | Not idempotent. `EventId` is the `Events` PK, but the insert runs **after** the counters and a duplicate is just logged/swallowed. Fix: idempotent upsert keyed by `EventId`, or transactional outbox. |
| **Dual-write consistency** | Redis + Postgres writes aren't in one transaction; a mid-event failure diverges them. Fix: outbox pattern or single store + async projection. |
| **Poison messages / DLQ** | None — a bad message is logged and skipped (lost). Add a dead-letter topic + retry/backoff. |
| **Ordering** | Kafka key = `tenantId:campaignId` → per-campaign ordering; partitions cap consumer parallelism. |
| **Multi-tenancy isolation** | Shared DB + `TenantId` column; middleware resolves tenant; every index leads with `TenantId`. Risk: no row-level enforcement — a missing filter leaks data. |
| **Why no FKs** | Write throughput; tables linked logically by tenant/campaign. Trade-off: no referential integrity. |
| **EF + Dapper hybrid** | EF for CRUD/migrations; Dapper for raw `SUM` aggregates to skip EF overhead. |
| **Why BackgroundService not Flink** | Same conceptual model (consume→process→state→sink) without cluster overhead; Flink needed for exactly-once, event-time/watermarks, backpressure at high throughput. |
| **Black Friday 10x** | Kafka absorbs spike; HPA scales stateless API/Collector; processor scales with partitions; degrade to Redis-only if Postgres saturates. |

---

## 5. "What would you do differently in production?" (have 5 ready)

1. **Kill the double-count** — one read path / single source of truth.
2. **Exactly-once / idempotency** — outbox or transactional Kafka (or Flink checkpoints).
3. **DLQ + retries** for poison messages.
4. **Schema registry** (Avro/Protobuf) instead of raw JSON over Kafka.
5. **AuthN/Z + rate limiting**; wire **OpenTelemetry** tracing (referenced, not implemented).

> The team docs already scope these as intentional gaps — point to `04_Scope_For_Extension.md` and `01_System_Understanding.md` §4 to show it was deliberate, not missed.

---

## 6. Fast facts to have on the tip of your tongue

- Ports: API `5000`, EventCollector `5001` (docker) / `5229` (local), Postgres `5433→5432`, Redis `6380→6379`, Kafka `9092`.
- Attribution window: **30 min** session (`session:{tenant}:{user}`); counter keys have **24h** TTL.
- Handled events: `AdClick`, `AdImpression`, `AddToCart`. No-op (still persisted to `Events`): `ProductView`, `Purchase`.
- 3 tables: `Campaigns`, `Events`, `CampaignMetrics`. No FKs. Value-object IDs stored as `text`. Metadata is `jsonb`.
