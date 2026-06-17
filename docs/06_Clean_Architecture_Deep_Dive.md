# Clean Architecture (Ports & Adapters) — Deep Dive

> **Context:** This project follows Clean Architecture with 4 layers. Dependencies point **inward** — the Domain layer has zero external dependencies. This document traces concrete code examples from every layer to show exactly how the pattern works.

---

## 1. The Dependency Rule

```
Outer layers reference inner layers. Inner layers NEVER reference outer layers.
```

```
┌──────────────────────────────────────────────────────────────────┐
│  Presentation (API / Collector / Processor)                      │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Application (Use Cases / Services)                       │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │  Infrastructure (Adapters — Redis, Kafka, EF Core)   │  │  │
│  │  │  ┌────────────────────────────────────────────────┐  │  │  │
│  │  │  │  Domain (Entities, Value Objects, Interfaces)  │  │  │  │
│  │  │  │  • Zero using statements to external packages   │  │  │  │
│  │  │  │  • No JSON attributes, no DB attributes         │  │  │  │
│  │  │  │  • No framework references                      │  │  │  │
│  │  │  └────────────────────────────────────────────────┘  │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

The arrows are **not** data flow — they are **source-code dependency** direction. Each layer only knows about the layer directly inside it.

---

## 2. Layer 1: Domain — The Innermost Core

**Location:** `src/RetailMedia.Domain/`

### What lives here
- **Entities** — Business objects (`Event`, `Campaign`, `CampaignMetric`)
- **Value Objects** — Typed identifiers (`TenantId`, `CampaignId`, `EventType`)
- **Interfaces (Ports)** — Contracts that outer layers implement (`IRedisCache`, `IMetricsRepository`, `IKafkaProducer`)

### Zero external dependencies

```csharp
// src/RetailMedia.Domain/Entities/Event.cs
using RetailMedia.Domain.ValueObjects;  // Only other Domain types

namespace RetailMedia.Domain.Entities;

public class Event
{
    public TenantId TenantId { get; private set; }   // Value Object, not a raw string
    public CampaignId CampaignId { get; private set; }
    public EventType Type { get; private set; }

    private Event() { }  // For EF Core materialization

    public Event(string eventId, TenantId tenantId, string userId,
        CampaignId campaignId, EventType type, DateTime timestamp,
        Dictionary<string, string>? metadata = null)
    {
        EventId = eventId ?? throw new ArgumentNullException(nameof(eventId));
        TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
        // ...
    }
}
```

**What's NOT here:**
- `[JsonProperty]`, `[Key]`, `[Table]` attributes
- `using Newtonsoft.Json`, `using System.Text.Json`
- `using StackExchange.Redis`, `using Confluent.Kafka`
- `using Microsoft.EntityFrameworkCore`

Nothing in Domain knows Infrastructure exists. The business logic is pure C#.

### Value Objects — encode domain rules at the type level

```csharp
// src/RetailMedia.Domain/ValueObjects/TenantId.cs
public record TenantId(string Value)
{
    public static TenantId From(string value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new ArgumentException("TenantId cannot be empty", nameof(value))
            : new TenantId(value);

    public override string ToString() => Value;
}
```

Why a `TenantId` type instead of a plain `string`? Because `TenantId` and `CampaignId` are semantically different — you can't accidentally pass a campaign ID where a tenant ID is expected. The compiler enforces this.

### Interfaces (Ports) — what the domain needs

```csharp
// src/RetailMedia.Domain/Interfaces/IRedisCache.cs
public interface IRedisCache
{
    Task<long> IncrementCounterAsync(string key, long value = 1, TimeSpan? expiry = null);
    Task<long> GetCounterAsync(string key);
    Task SetAsync<T>(string key, T value, TimeSpan? expiry = null);
    Task<T?> GetAsync<T>(string key);
    Task<bool> KeyExistsAsync(string key);
    Task<long> GetAndResetCounterAsync(string key);
}
```

```csharp
// src/RetailMedia.Domain/Interfaces/IEventRepository.cs
public interface IEventRepository
{
    Task AddAsync(Event @event, CancellationToken ct = default);
    Task<IReadOnlyList<Event>> GetByCampaignAsync(
        CampaignId campaignId, TenantId tenantId,
        DateTime? from = null, DateTime? to = null, CancellationToken ct = default);
}
```

These are **ports** — the Domain says "I need a cache" without knowing which cache. It says "I need event storage" without knowing which database.

---

## 3. Layer 2: Application — Use Cases

**Location:** `src/RetailMedia.Application/`

### What lives here
- **Services** — Orchestrate business logic (`InsightsService`, `EventIngestionService`)
- **DTOs** — Data transfer objects for API contracts
- **Application Interfaces** — Service interfaces (`IInsightsService`, `IEventIngestionService`)

### Only references Domain

```csharp
// src/RetailMedia.Application/Services/InsightsService.cs
using RetailMedia.Domain.Entities;         // Domain entities
using RetailMedia.Domain.Interfaces;       // Domain interfaces (ports)
using RetailMedia.Domain.ValueObjects;     // Domain value objects
using RetailMedia.Application.DTOs;       // Application DTOs

public class InsightsService : IInsightsService
{
    private readonly IRedisCache _cache;          // Domain interface
    private readonly IMetricsRepository _metricsRepo; // Domain interface

    public InsightsService(IRedisCache cache, IMetricsRepository metricsRepo)
    {
        _cache = cache;
        _metricsRepo = metricsRepo;
    }

    public async Task<ClickResponse> GetClicksAsync(
        CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        // Uses Domain interfaces — no idea if Redis, Memcached, or in-memory
        var redisCount = await _cache.GetCounterAsync($"campaign:{campaignId}:clicks");

        // Uses Domain interfaces — no idea if PostgreSQL, SQL Server, or CosmosDB
        var dbCount = await _metricsRepo.GetClickCountAsync(campaignId, tenantId, ct);

        return new ClickResponse(campaignId.ToString(), redisCount + dbCount, ...);
    }
}
```

```csharp
// src/RetailMedia.Application/Services/EventIngestionService.cs
public class EventIngestionService : IEventIngestionService
{
    private readonly IKafkaProducer _kafkaProducer;  // Domain interface
    private readonly IEventRepository _eventRepo;      // Domain interface

    public async Task IngestAsync(IngestEventRequest request, CancellationToken ct = default)
    {
        // Validates and constructs domain objects
        var @event = new Event(
            request.EventId,
            TenantId.From(request.TenantId),
            request.UserId,
            CampaignId.From(request.CampaignId),
            eventType,
            request.Timestamp,
            request.Metadata);

        // Publishes via a Domain interface — no Kafka dependency
        await _kafkaProducer.PublishAsync(@event, ct);
    }
}
```

**Key observation:** `InsightsService` does not know that `IRedisCache` is backed by Redis or that `IMetricsRepository` is backed by PostgreSQL. If we swap Redis for Google Cloud Memorystore or PostgreSQL for CosmosDB, **Application layer changes nothing**.

---

## 4. Layer 3: Infrastructure — Adapters

**Location:** `src/RetailMedia.Infrastructure/`

### What lives here
- **Caching** — `RedisCache : IRedisCache`
- **Messaging** — `KafkaProducer : IKafkaProducer`
- **Persistence** — `MetricsRepository : IMetricsRepository`, `EventRepository : IEventRepository`, `AppDbContext`
- **Dependency Injection wiring** — `AddInfrastructure()` extension method

### Adapters implement Domain ports

```csharp
// src/RetailMedia.Infrastructure/Caching/RedisCache.cs
using StackExchange.Redis;                    // External dependency (allowed in Infrastructure)
using RetailMedia.Domain.Interfaces;          // References Domain

public class RedisCache : IRedisCache         // Implements Domain port
{
    private readonly ConnectionMultiplexer _redis;

    public async Task<long> IncrementCounterAsync(string key, long value = 1, TimeSpan? expiry = null)
    {
        var count = await _db.StringIncrementAsync(key, value);  // Redis-specific
        if (expiry.HasValue)
            await _db.KeyExpireAsync(key, expiry.Value);
        return count;
    }
}
```

The concrete implementation uses `StackExchange.Redis` types — this is the **adapter** converting Domain's `IRedisCache` port into a real Redis call.

```csharp
// src/RetailMedia.Infrastructure/Persistence/Repositories/MetricsRepository.cs
using Dapper;
using Npgsql;                                    // PostgreSQL-specific
using RetailMedia.Domain.Interfaces;

public class MetricsRepository : IMetricsRepository
{
    public async Task<long> GetClickCountAsync(
        CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<long>(           // Raw SQL
            "SELECT COALESCE(SUM(\"Count\"), 0) FROM \"CampaignMetrics\" WHERE ...",
            new { t = tenantId.Value, c = campaignId.Value });
    }
}
```

### Wiring — Composition Root

```csharp
// src/RetailMedia.Infrastructure/DependencyInjection.cs
public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration config)
{
    // Port → Adapter mapping
    services.AddSingleton<IRedisCache>(_ => new RedisCache(redisConnection));
    services.AddScoped<IMetricsRepository>(sp => new MetricsRepository(db, connectionString));
    services.AddScoped<IEventRepository, EventRepository>();

    return services;
}
```

This is the **only place** that knows `IRedisCache` maps to `RedisCache`. If you swap infrastructure, you only change this file.

---

## 5. Layer 4: Presentation — Entry Points

**Location:** `src/RetailMedia.Api/`, `src/RetailMedia.EventCollector/`, `src/RetailMedia.StreamProcessor/`

These are thin layers that:
1. Accept HTTP requests or Kafka messages
2. Resolve Application services from DI
3. Map results to HTTP responses

```csharp
// src/RetailMedia.Api/Endpoints/CampaignEndpoints.cs
public static class CampaignEndpoints
{
    public static void MapCampaignEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/ad/{campaignId}");
        group.MapGet("/clicks", GetClicks);
        group.MapGet("/impressions", GetImpressions);
        group.MapGet("/clickToBasket", GetClickToBasket);
        group.MapGet("/metrics", GetMetrics);
    }

    static async Task<IResult> GetClicks(
        string campaignId,
        IInsightsService service,             // App service (from DI)
        ITenantContext tenant,
        CancellationToken ct)
    {
        var result = await service.GetClicksAsync(
            CampaignId.From(campaignId), tenant.CurrentTenantId, ct);
        return Results.Ok(new { data = result, meta = new { timestamp = DateTime.UtcNow } });
    }
}
```

`CampaignEndpoints` has **zero lines of business logic** — it's pure HTTP mapping. It calls `IInsightsService` (Application interface) which calls `IRedisCache` + `IMetricsRepository` (Domain interfaces) backed by Infrastructure adapters.

---

## 6. What Happens When You Swap Infrastructure

### Scenario: Replace Redis with Google Cloud Memorystore

**Change 1** — Write a new adapter:
```csharp
// New file — 0 existing files modified
public class MemorystoreCache : IRedisCache
{
    // Implementation using Google.Cloud.Memorystore API
}
```

**Change 2** — Update the composition root:
```csharp
// DependencyInjection.cs — 1 line changes
// Before:
services.AddSingleton<IRedisCache>(_ => new RedisCache(redisConnection));
// After:
services.AddSingleton<IRedisCache>(_ => new MemorystoreCache(connectionString));
```

**Files touched:** 2 (new adapter + DI wiring)
**Files NOT touched:** `InsightsService`, `ClickHandler`, `CampaignEndpoints`, `EventIngestionService`, any Domain entity.

### Scenario: Replace PostgreSQL with CosmosDB

**Change 1** — New repository:
```csharp
public class CosmosMetricsRepository : IMetricsRepository { ... }
```

**Change 2** — Update DI:
```csharp
// services.AddScoped<IMetricsRepository, MetricsRepository>();
services.AddScoped<IMetricsRepository, CosmosMetricsRepository>();
```

**Files touched:** 2. Application and Domain don't change.

---

## 7. How Testing Benefits

Because layers depend on interfaces (ports), tests can mock everything:

```csharp
// test creates a mock IRedisCache — no Redis server needed
var cacheMock = new Mock<IRedisCache>();
cacheMock.Setup(c => c.GetCounterAsync("campaign:cmp_789:clicks")).ReturnsAsync(42);

var service = new InsightsService(cacheMock.Object, metricsRepoMock.Object);
var result = await service.GetClicksAsync(CampaignId.From("cmp_789"), TenantId.From("tesco"));

Assert.Equal(42, result.Clicks);
```

Same pattern applies to every Domain interface:
- `Mock<IMetricsRepository>` — no PostgreSQL
- `Mock<IKafkaProducer>` — no Kafka broker
- `Mock<IEventRepository>` — no database

This is why our **60 unit tests** run in ~50ms total with zero infrastructure.

---

## 8. Summary

| Layer | What it contains | References | Can be swapped |
|-------|-----------------|-----------|----------------|
| **Domain** | Entities, Value Objects, Interfaces | Nothing external | Never (the business) |
| **Application** | Services, Use Cases, DTOs | Domain only | Rarely (rules change) |
| **Infrastructure** | Redis, Kafka, EF Core adapters | Domain + NuGet packages | Often (cloud vendors) |
| **Presentation** | API endpoints, Middleware | Application + Infrastructure | Regularly (API style) |

**The litmus test:** If a file in Domain has `using Newtonsoft.Json` or `using StackExchange.Redis`, the architecture is violated. In this project, no Domain file references any external package.
