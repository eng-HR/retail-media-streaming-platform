# End-to-End Use Case Walkthroughs

Step-by-step traces through the code with dummy data for every event type and query path.

---

## Common Dummy Data

| Field | Value |
|-------|-------|
| TenantId | `tenant-retail-01` |
| CampaignId | `camp-summer-sale-2026` |
| UserId | `user-alice-99` |

---

## Use Case 1: AdImpression Event

> **Goal:** Record that a user saw an ad, then query the impression count.

### Dummy Payload

```json
{
  "eventId": "evt-001-impression",
  "tenantId": "tenant-retail-01",
  "userId": "user-alice-99",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "AdImpression",
  "timestamp": "2026-06-16T12:00:00Z"
}
```

### Walkthrough

#### 1a. POST /events — EventCollector

**File:** `src/RetailMedia.EventCollector/Program.cs:10`

```csharp
Line 12: app.MapEventEndpoints();
```

Maps the `POST /events` endpoint.

**File:** `src/RetailMedia.EventCollector/Endpoints/EventEndpoints.cs:10`

```csharp
Line 10: app.MapPost("/events", async (IngestEventRequest request,
Line 11:     IEventIngestionService service, CancellationToken ct) =>
```

ASP.NET binds the JSON body to `IngestEventRequest`:

```csharp
// IngestEventRequest is a record:
public record IngestEventRequest(
    string EventId,       // "evt-001-impression"
    string TenantId,      // "tenant-retail-01"
    string UserId,        // "user-alice-99"
    string CampaignId,    // "camp-summer-sale-2026"
    string EventType,     // "AdImpression"
    DateTime Timestamp,   // 2026-06-16T12:00:00Z
    Dictionary<string, string>? Metadata = null);
```

```csharp
Line 13: await service.IngestAsync(request, ct);
Line 14: return Results.Accepted($"/events/{request.EventId}", new
Line 15: {
Line 16:     data = new { eventId = request.EventId, status = "accepted" },
Line 17:     meta = new { timestamp = DateTime.UtcNow }
Line 18: });
```

Returns **202 Accepted** immediately (async fire-and-forget pattern):
```json
{
  "data": { "eventId": "evt-001-impression", "status": "accepted" },
  "meta": { "timestamp": "2026-06-16T12:00:01Z" }
}
```

---

#### 1b. EventIngestionService — Validate + Publish to Kafka

**File:** `src/RetailMedia.Application/Services/EventIngestionService.cs:20`

```csharp
Line 22: if (!Enum.TryParse<EventType>(request.EventType, ignoreCase: true, out var eventType))
Line 23:     throw new ArgumentException($"Invalid event type: {request.EventType}");
```

`"AdImpression"` matches `EventType.AdImpression` → passes. If we sent `"FooBar"`, this throws `ArgumentException`, caught by `ErrorHandlingMiddleware`, returns **400 Bad Request**.

```csharp
Line 25: var @event = new Event(
Line 26:     request.EventId,                          // "evt-001-impression"
Line 27:     TenantId.From(request.TenantId),           // validates non-null/whitespace → TenantId("tenant-retail-01")
Line 28:     request.UserId,                            // "user-alice-99"
Line 29:     CampaignId.From(request.CampaignId),       // validates → CampaignId("camp-summer-sale-2026")
Line 30:     eventType,                                 // EventType.AdImpression
Line 31:     request.Timestamp,                         // DateTime(2026-06-16 12:00:00 UTC)
Line 32:     request.Metadata);                         // null
```

Domain `Event` entity created with private setters (immutable after construction).

```csharp
Line 34: await _kafkaProducer.PublishAsync(@event, ct);
```

Note: `_eventRepo` (line 12) is injected but **never used** here — events are not saved to PostgreSQL at ingestion time.

---

#### 1c. KafkaProducer — Serialize + Produce to `raw-events`

**File:** `src/RetailMedia.Infrastructure/Messaging/KafkaProducer.cs:22`

```csharp
Line 24: var key = $"{@event.TenantId}:{@event.CampaignId}";
// → "tenant-retail-01:camp-summer-sale-2026"
```

Kafka key determines partition. All events for this campaign go to the same partition, preserving order.

```csharp
Line 25: var value = JsonSerializer.Serialize(new
Line 26: {
Line 27:     @event.EventId,                              // "evt-001-impression"
Line 28:     tenantId = @event.TenantId.ToString(),        // "tenant-retail-01"
Line 29:     @event.UserId,                               // "user-alice-99"
Line 30:     campaignId = @event.CampaignId.ToString(),    // "camp-summer-sale-2026"
Line 31:     eventType = @event.Type.ToString(),           // "AdImpression"
Line 32:     @event.Timestamp,                            // 2026-06-16T12:00:00Z
Line 33:     @event.Metadata                              // null
Line 34: });
```

```csharp
Line 36: var result = await _producer.ProduceAsync("raw-events",
Line 37:     new Message<string, string> { Key = key, Value = value }, ct);
```

```csharp
Line 42: _logger.LogInformation("Published event {EventId} to topic {Topic} partition {Partition} offset {Offset}",
Line 43:     @event.EventId, Topic, result.Partition, result.Offset);
```

Logs: `Published event evt-001-impression to topic raw-events partition 2 offset 15`

**DI registration** (`src/RetailMedia.Infrastructure/DependencyInjection.cs:34`):
```csharp
services.AddSingleton<IKafkaProducer>(sp => {
    var logger = sp.GetRequiredService<ILogger<KafkaProducer>>();
    return new KafkaProducer(kafkaBootstrapServers, logger);
});
```
Producer is **singleton** — one Kafka connection shared across all requests.

---

#### 1d. StreamProcessor — Consume from Kafka

**File:** `src/RetailMedia.StreamProcessor/Program.cs:5`

```csharp
Line 10: builder.Services.AddSingleton(sp => { ... return new KafkaEventConsumer(config, sp, logger); });
Line 17: builder.Services.AddHostedService(sp => sp.GetRequiredService<KafkaEventConsumer>());
```

`KafkaEventConsumer` registered as singleton + hosted service (runs in background thread).

**File:** `src/RetailMedia.StreamProcessor/KafkaEventConsumer.cs:31`

```csharp
Line 24: GroupId = "retail-media-processor"
Line 25: AutoOffsetReset = AutoOffsetReset.Earliest   // start from earliest on first run
Line 26: EnableAutoCommit = false                      // manual commit for at-least-once
```

```csharp
Line 33: _consumer.Subscribe("raw-events");
Line 36: while (!stoppingToken.IsCancellationRequested)
Line 37: {
Line 38:     try
Line 39:     {
Line 40:         var result = _consumer.Consume(stoppingToken);  // blocks until message arrives
Line 41:         await ProcessMessageAsync(result.Message, stoppingToken);
Line 42:         _consumer.Commit(result);  // only commits after successful processing
```

At-least-once delivery: commit happens **after** `ProcessMessageAsync` succeeds.

```csharp
Line 44:     catch (ConsumeException ex)
Line 45:     {
Line 46:         _logger.LogError(ex, "Kafka consume error");  // log and continue
Line 47:     }
Line 48:     catch (OperationCanceledException) { break; }
```

`ProcessMessageAsync` (line 55):

```csharp
Line 57: using var scope = _services.CreateScope();
Line 58: var cache = scope.ServiceProvider.GetRequiredService<IRedisCache>();
Line 59: var metricsRepo = scope.ServiceProvider.GetRequiredService<IMetricsRepository>();
```

Creates a new DI scope per message (needed for scoped `IMetricsRepository` which depends on scoped `AppDbContext`).

```csharp
Line 64: var doc = JsonDocument.Parse(message.Value);
Line 67: var eventType = root.GetProperty("eventType").GetString() ?? "";  // "AdImpression"
Line 68: var @event = new Event(
Line 69:     root.GetProperty("eventId").GetString()!,        // "evt-001-impression"
Line 70:     TenantId.From(root.GetProperty("tenantId").GetString()!),  // "tenant-retail-01"
Line 71:     root.GetProperty("userId").GetString()!,          // "user-alice-99"
Line 72:     CampaignId.From(root.GetProperty("campaignId").GetString()!),  // "camp-summer-sale-2026"
Line 73:     Enum.Parse<EventType>(eventType, ignoreCase: true),  // EventType.AdImpression
Line 74:     root.GetProperty("timestamp").GetDateTime(),      // 2026-06-16T12:00:00Z
Line 75:     null);  // Metadata is null
```

Reconstructs the domain `Event` from the JSON.

```csharp
Line 77: switch (@event.Type)
Line 78: {
Line 79:     case EventType.AdClick: ...
Line 84:     case EventType.AdImpression:
Line 85:         await new ImpressionHandler(cache, loggerFactory.CreateLogger<ImpressionHandler>())
                      .HandleAsync(@event);
Line 86:         break;
Line 88:     case EventType.AddToCart: ...
Line 92:     default:
Line 93:         _logger.LogDebug("Unhandled event type: {Type}", @event.Type);
Line 94:         break;
```

For `AdImpression`, routes to `ImpressionHandler`.

---

#### 1e. ImpressionHandler — Increment Redis Counter

**File:** `src/RetailMedia.StreamProcessor/Handlers/ImpressionHandler.cs:17`

```csharp
Line 19: var redisKey = $"campaign:{@event.CampaignId}:impressions";
// → "campaign:camp-summer-sale-2026:impressions"
Line 20: var count = await _cache.IncrementCounterAsync(redisKey);
```

**File:** `src/RetailMedia.Infrastructure/Caching/RedisCache.cs:18`

```csharp
return await _db.StringIncrementAsync(key, value);
// Redis INCR campaign:camp-summer-sale-2026:impressions → returns 1
```

Atomic increment. If key doesn't exist, Redis initializes it at 0 then increments to 1.

```csharp
Line 21: _logger.LogInformation("Impression event {EventId} for campaign {CampaignId}: total {Count}",
Line 22:     @event.EventId, @event.CampaignId, count);
```

Logs: `Impression event evt-001-impression for campaign camp-summer-sale-2026: total 1`

---

#### 1f. Query Impressions — API endpoint

**File:** `src/RetailMedia.Api/Program.cs:36`

```csharp
Line 36: app.MapCampaignEndpoints();
```

**File:** `src/RetailMedia.Api/Endpoints/CampaignEndpoints.cs:15`

```csharp
Line 11: var group = app.MapGroup("/ad/{campaignId}");
Line 15: group.MapGet("/impressions", GetImpressions);
```

Route: `GET /ad/camp-summer-sale-2026/impressions`

`TenantContextMiddleware` runs before handler:

**File:** `src/RetailMedia.Api/Middleware/TenantContextMiddleware.cs:13`

```csharp
Line 22: var tenantId = ExtractTenantId(context);
Line 30: if (tenantContext is TenantContext tc)
Line 31:     tc.SetTenantId(TenantId.From(tenantId));
```

`ExtractTenantId` (line 36):
```csharp
Line 38: var claim = context.User?.FindFirst("tenantId")?.Value;      // check JWT
Line 43: var header = context.Request.Headers["X-Tenant-Id"].FirstOrDefault();  // check header
```

Returns `"tenant-retail-01"`.

Back in handler:
```csharp
Line 31: static async Task<IResult> GetImpressions(
Line 32:     string campaignId,              // "camp-summer-sale-2026" (from route)
Line 33:     IInsightsService service,
Line 34:     ITenantContext tenant,           // populated by middleware — CurrentTenantId = "tenant-retail-01"
Line 35:     CancellationToken ct)
Line 36: {
Line 37:     var result = await service.GetImpressionsAsync(
Line 38:         CampaignId.From(campaignId), tenant.CurrentTenantId, ct);
Line 39:     return Results.Ok(new { data = result, meta = new { timestamp = DateTime.UtcNow } });
```

---

#### 1g. InsightsService — Combine Redis + PostgreSQL

**File:** `src/RetailMedia.Application/Services/InsightsService.cs:28`

```csharp
Line 30: var redisKey = $"campaign:{campaignId}:impressions";
// → "campaign:camp-summer-sale-2026:impressions"
Line 31: var redisCount = await _cache.GetCounterAsync(redisKey);
```

**File:** `src/RetailMedia.Infrastructure/Caching/RedisCache.cs:21`

```csharp
Line 22: var val = await _db.StringGetAsync(key);
Line 23: return val.HasValue ? (long)val : 0;
// Redis GET campaign:camp-summer-sale-2026:impressions → "1" → returns 1
```

```csharp
// Back in InsightsService:
Line 32: var dbCount = await _metricsRepo.GetImpressionCountAsync(campaignId, tenantId, ct);
```

**File:** `src/RetailMedia.Infrastructure/Persistence/Repositories/MetricsRepository.cs:29`

```csharp
Line 31: await using var conn = new NpgsqlConnection(_connectionString);
Line 32: return await conn.ExecuteScalarAsync<long>(
Line 33:     "SELECT COALESCE(SUM(\"Count\"), 0) FROM \"CampaignMetrics\"
          WHERE \"TenantId\" = @t AND \"CampaignId\" = @c AND \"Metric\" = 'Impressions'",
Line 34:     new { t = "tenant-retail-01", c = "camp-summer-sale-2026" });
```

Dapper raw SQL → `SUM(Count)` across all rows. Returns `0` (no flushes yet — `RedisFlushService` is a stub).

**Back in InsightsService (line 33):**
```csharp
return new ImpressionResponse(campaignId.ToString(), redisCount + dbCount, timestamp);
// → ImpressionResponse("camp-summer-sale-2026", 1 + 0, 1718544000)
```

**Final API response:**
```json
{
  "data": { "campaignId": "camp-summer-sale-2026", "impressions": 1, "timestamp": 1718544000 },
  "meta": { "timestamp": "2026-06-16T12:00:05Z" }
}
```

### Data Flow Diagram

```
CLIENT                         EVENTCOLLECTOR                    KAFKA
  │                                │                              │
  │  POST /events                  │                              │
  │  {eventType:"AdImpression"}    │                              │
  │────────────────────────────>   │                              │
  │                                │  Validate EventType enum     │
  │                                │  Create domain Event         │
  │                                │  PublishAsync(event)         │
  │                                │────────────────────────────> │
  │  202 Accepted                  │                              │
  │<────────────────────────────   │                              │
  │                                │                              │
  │                                │                              │
  │                    STREAMPROCESSOR                            │
  │                              │                                │
  │                              │  Consume ←────────────────── │
  │                              │                                │
  │                              │  Deserialize JSON → Event      │
  │                              │  EventType.AdImpression        │
  │                              │  └─► ImpressionHandler         │
  │                              │       └─► Redis INCR           │
  │                              │            campaign:...        │
  │                              │            :impressions = 1    │
  │                              │                                │
  │                    API                                        │
  │                              │                                │
  │  GET /ad/.../impressions     │                                │
  │────────────────────────────> │                                │
  │                              │  TenantContextMiddleware        │
  │                              │  └─► X-Tenant-Id → tenantId    │
  │                              │                                │
  │                              │  InsightsService               │
  │                              │  ├─► Redis GET → 1             │
  │                              │  └─► PostgreSQL SUM → 0        │
  │                              │       (Dapper raw SQL)         │
  │                              │                                │
  │  { data: { impressions: 1 }} │                                │
  │<──────────────────────────── │                                │
```

---

## Use Case 2: AdClick Event

> **Goal:** Record a click event and create an attribution session for later conversion tracking.

### Dummy Payload

```json
{
  "eventId": "evt-002-click",
  "tenantId": "tenant-retail-01",
  "userId": "user-alice-99",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "AdClick",
  "timestamp": "2026-06-16T12:05:00Z"
}
```

### Walkthrough

#### Steps 2a–2d are identical to 1a–1d (EventCollector → Kafka → StreamProcessor)

The switch statement routes differently:

**File:** `src/RetailMedia.StreamProcessor/KafkaEventConsumer.cs:79`

```csharp
Line 79: case EventType.AdClick:
Line 80:     await new ClickHandler(cache, loggerFactory.CreateLogger<ClickHandler>()).HandleAsync(@event);
Line 81:     await new AttributionHandler(cache, loggerFactory.CreateLogger<AttributionHandler>())
                .HandleClickAsync(@event);
Line 82:     break;
```

Two handlers fire for a single click event.

---

#### 2e. ClickHandler — Increment Click Counter

**File:** `src/RetailMedia.StreamProcessor/Handlers/ClickHandler.cs:18`

```csharp
Line 20: var redisKey = $"campaign:{@event.CampaignId}:clicks";
// → "campaign:camp-summer-sale-2026:clicks"
Line 21: var count = await _cache.IncrementCounterAsync(redisKey);
// Redis INCR campaign:camp-summer-sale-2026:clicks → 1
```

---

#### 2f. AttributionHandler — Store Click Session

**File:** `src/RetailMedia.StreamProcessor/Handlers/AttributionHandler.cs:18`

```csharp
Line 10: private static readonly TimeSpan AttributionWindow = TimeSpan.FromMinutes(30);
```

```csharp
Line 20: var sessionKey = $"session:{@event.TenantId}:{@event.UserId}";
// → "session:tenant-retail-01:user-alice-99"
Line 21: var session = new Dictionary<string, string>
Line 22: {
Line 23:     ["campaignId"] = @event.CampaignId.ToString(),  // "camp-summer-sale-2026"
Line 24:     ["timestamp"] = DateTime.UtcNow.ToString("O")   // "2026-06-16T12:05:01Z"
Line 25: };
Line 26: await _cache.SetAsync(sessionKey, session, AttributionWindow);
```

**File:** `src/RetailMedia.Infrastructure/Caching/RedisCache.cs:27`

```csharp
Line 29: var json = JsonSerializer.Serialize(value);
         // → "{\"campaignId\":\"camp-summer-sale-2026\",\"timestamp\":\"2026-06-16T12:05:01Z\"}"
Line 30: if (expiry.HasValue)
Line 31:     await _db.StringSetAsync(key, json, expiry.Value, When.Always);
         // Redis SETEX session:tenant-retail-01:user-alice-99 1800 "{...}"
```

Session expires in 1800 seconds (30 min). Used later for AddToCart attribution.

#### 2g. Query Clicks

Same pattern as impressions:

```
GET /ad/camp-summer-sale-2026/clicks
```

`InsightsService.GetClicksAsync`:
- Redis GET `campaign:camp-summer-sale-2026:clicks` → 1
- PostgreSQL SUM WHERE Metric='Clicks' → 0
- Total: 1

Response:
```json
{
  "data": { "campaignId": "camp-summer-sale-2026", "clicks": 1, "timestamp": 1718544000 },
  "meta": { "timestamp": "2026-06-16T12:05:05Z" }
}
```

---

## Use Case 3: AddToCart (Attributed)

> **Goal:** User adds an item to cart within 30 minutes of clicking the ad → this counts as a "click-to-basket" attribution.

### Precondition

The click attribution session exists in Redis (from Use Case 2):
- Key: `session:tenant-retail-01:user-alice-99`
- Value: `{"campaignId":"camp-summer-sale-2026","timestamp":"2026-06-16T12:05:01Z"}`
- TTL: ~25 minutes remaining (within the 30-min window)

### Dummy Payload

```json
{
  "eventId": "evt-003-addtocart",
  "tenantId": "tenant-retail-01",
  "userId": "user-alice-99",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "AddToCart",
  "timestamp": "2026-06-16T12:15:00Z",
  "metadata": { "productId": "prod-xyz-123", "price": "29.99" }
}
```

### Walkthrough

#### Steps 3a–3d are identical to ingestion pipeline.

#### 3e. KafkaEventConsumer routes to AttributionHandler

**File:** `src/RetailMedia.StreamProcessor/KafkaEventConsumer.cs:88`

```csharp
Line 88: case EventType.AddToCart:
Line 89:     await new AttributionHandler(cache, loggerFactory.CreateLogger<AttributionHandler>())
                .HandleAddToCartAsync(@event);
Line 90:     break;
```

---

#### 3f. AttributionHandler.HandleAddToCartAsync — Check Session

**File:** `src/RetailMedia.StreamProcessor/Handlers/AttributionHandler.cs:31`

```csharp
Line 33: var sessionKey = $"session:{@event.TenantId}:{@event.UserId}";
// → "session:tenant-retail-01:user-alice-99"
Line 34: var session = await _cache.GetAsync<Dictionary<string, string>>(sessionKey);
```

**File:** `src/RetailMedia.Infrastructure/Caching/RedisCache.cs:36`

```csharp
Line 38: var json = await _db.StringGetAsync(key);
         // Redis GET session:tenant-retail-01:user-alice-99
         // → "{\"campaignId\":\"camp-summer-sale-2026\",\"timestamp\":\"2026-06-16T12:05:01Z\"}"
Line 39: return json.HasValue ? JsonSerializer.Deserialize<T>(json!) : default;
         // → Dictionary<string, string> with campaignId + timestamp
```

**Back in AttributionHandler (line 36):**
```csharp
if (session == null) return;
```

Session exists → continues.

```csharp
Line 38: if (!DateTime.TryParse(session["timestamp"], out var clickTime)) return;
Line 39: if (DateTime.UtcNow - clickTime > AttributionWindow)
Line 40: {
Line 41:     _logger.LogInformation("Attribution window expired for user {UserId}", @event.UserId);
Line 42:     return;
Line 43: }
```

`clickTime = 2026-06-16T12:05:01Z`, `DateTime.UtcNow = 2026-06-16T12:15:02Z`, difference = ~10 minutes < 30 min → passes.

```csharp
Line 45: var redisKey = $"campaign:{@event.CampaignId}:clickToBasket";
// → "campaign:camp-summer-sale-2026:clickToBasket"
Line 46: await _cache.IncrementCounterAsync(redisKey);
// Redis INCR campaign:camp-summer-sale-2026:clickToBasket → 1
```

The AddToCart is attributed to the prior click.

```csharp
Line 47: _logger.LogInformation("Attributed add-to-cart for user {UserId} campaign {CampaignId}",
Line 48:     @event.UserId, @event.CampaignId);
```

---

#### 3g. Query ClickToBasket

```
GET /ad/camp-summer-sale-2026/clickToBasket
```

`InsightsService.GetClickToBasketAsync`:
- Redis GET `campaign:camp-summer-sale-2026:clickToBasket` → 1
- PostgreSQL SUM WHERE Metric='ClickToBasket' → 0
- Total: 1

Response:
```json
{
  "data": { "campaignId": "camp-summer-sale-2026", "clickToBasket": 1, "timestamp": 1718544000 },
  "meta": { "timestamp": "2026-06-16T12:15:10Z" }
}
```

### Attribution Flow Diagram

```
TIME   CLICK EVENT                          ADD-TO-CART EVENT
│                                      │
│  POST /events {eventType:"AdClick"}  │  POST /events {eventType:"AddToCart"}
│        │                             │        │
│        ▼                             │        ▼
│  Kafka → StreamProcessor             │  Kafka → StreamProcessor
│        │                             │        │
│        ├─► ClickHandler              │        └─► AttributionHandler
│        │     Redis INCR clicks=1     │              │
│        │                             │              ├─► Redis GET session:... → found!
│        └─► AttributionHandler        │              │    (created during click, still within 30min)
│              .HandleClickAsync()      │              │
│              └─► Redis SETEX          │              └─► Redis INCR clickToBasket=1
│                   session:...         │
│                   TTL=30min           │
│                                      │
└──────────────────────────────────────┴────────────────────────────►
```

---

## Use Case 4: AddToCart (Not Attributed — No Prior Click)

### Dummy Payload

```json
{
  "eventId": "evt-004-addtocart-noattrib",
  "tenantId": "tenant-retail-01",
  "userId": "user-bob-77",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "AddToCart",
  "timestamp": "2026-06-16T14:00:00Z"
}
```

### Walkthrough

Only the `AttributionHandler.HandleAddToCartAsync` step differs from Use Case 3.

**File:** `src/RetailMedia.StreamProcessor/Handlers/AttributionHandler.cs:31`

```csharp
Line 33: var sessionKey = $"session:tenant-retail-01:user-bob-77";
Line 34: var session = await _cache.GetAsync<Dictionary<string, string>>(sessionKey);
```

Redis `GET session:tenant-retail-01:user-bob-77` → **nil** (key doesn't exist — user-bob-77 never clicked).

```csharp
Line 36: if (session == null) return;
```

Method returns immediately. No counter is incremented. No attribution.

The event is effectively **silently dropped** — logged at Debug level in the `default` case would not even be hit since `AddToCart` is explicitly handled, but the handler returns early.

```csharp
Line 97: catch (Exception ex)
Line 98: {
Line 99:     _logger.LogError(ex, "Error processing message {Key}", message.Key);
```

If any exception occurs (e.g. malformed data), it's caught and logged without crashing the consumer loop.

---

## Use Case 5: ProductView Event (Unhandled)

### Dummy Payload

```json
{
  "eventId": "evt-005-productview",
  "tenantId": "tenant-retail-01",
  "userId": "user-alice-99",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "ProductView",
  "timestamp": "2026-06-16T12:30:00Z"
}
```

### Walkthrough

Ingestion (steps 1a–1d) is identical. The switch statement hits the default case:

**File:** `src/RetailMedia.StreamProcessor/KafkaEventConsumer.cs:92`

```csharp
Line 92: default:
Line 93:     _logger.LogDebug("Unhandled event type: {Type}", @event.Type);
Line 94:     break;
```

The event is consumed from Kafka and acknowledged (committed offset), but **no processing occurs**. The message is neither stored in Redis nor PostgreSQL. It disappears silently after logging.

This is a notable gap — the `EventRepository` is injected into `EventIngestionService` but never called. If you wanted to persist raw events for replay/audit, you'd add that here.

---

## Use Case 6: Purchase Event (Unhandled)

Same as ProductView — hits `default` case. No counter, no DB write.

```json
{
  "eventId": "evt-006-purchase",
  "tenantId": "tenant-retail-01",
  "userId": "user-alice-99",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "Purchase",
  "timestamp": "2026-06-16T12:20:00Z",
  "metadata": { "orderId": "ord-456", "total": "59.98" }
}
```

---

## Use Case 7: Aggregate Metrics Query (With Date Range)

> **Goal:** Query all metric types (clicks, impressions, clickToBasket) for a campaign, filtered by date range.

### Precondition

PostgreSQL `CampaignMetrics` table has persisted rows from a previous flush:

| TenantId | CampaignId | Metric | Count | Date |
|----------|-----------|--------|-------|------|
| tenant-retail-01 | camp-summer-sale-2026 | Clicks | 100 | 2026-06-15 |
| tenant-retail-01 | camp-summer-sale-2026 | Impressions | 500 | 2026-06-15 |
| tenant-retail-01 | camp-summer-sale-2026 | Clicks | 50 | 2026-06-16 |

### Request

```
GET /ad/camp-summer-sale-2026/metrics?metric=clicks&startDate=2026-06-15&endDate=2026-06-16
```

### Walkthrough

**File:** `src/RetailMedia.Api/Endpoints/CampaignEndpoints.cs:53`

```csharp
Line 17: group.MapGet("/metrics", GetMetrics);
Line 53: static async Task<IResult> GetMetrics(
Line 54:     string campaignId,       // "camp-summer-sale-2026"
Line 55:     string? metric,           // "clicks"
Line 56:     DateTime? startDate,      // 2026-06-15
Line 57:     DateTime? endDate,        // 2026-06-16
Line 58:     IInsightsService service,
Line 59:     ITenantContext tenant,
Line 60:     CancellationToken ct)
```

**File:** `src/RetailMedia.Application/Services/InsightsService.cs:44`

```csharp
Line 47: var metricType = metric?.ToLowerInvariant() switch
Line 48: {
Line 49:     "clicks" => MetricType.Clicks,       // matches → MetricType.Clicks
Line 50:     "impressions" => MetricType.Impressions,
Line 51:     "clicktobasket" => MetricType.ClickToBasket,
Line 52:     _ => (MetricType?)null
Line 53: };
```

```csharp
Line 55: var metrics = await _metricsRepo.GetMetricsAsync(campaignId, tenantId,
Line 56:     MetricType.Clicks,                    // metric filter
Line 57:     startDate?.Date,                       // 2026-06-15
Line 58:     endDate?.Date,                         // 2026-06-16
Line 59:     ct);
```

**File:** `src/RetailMedia.Infrastructure/Persistence/Repositories/MetricsRepository.cs:66`

```csharp
Line 70: var query = _db.CampaignMetrics
Line 71:     .Where(m => m.TenantId == tenantId && m.CampaignId == campaignId);
// → filters to campaign-summer-sale-2026 for tenant-retail-01

Line 73: if (metric.HasValue) query = query.Where(m => m.Metric == metric.Value);
// → MetricType.Clicks → further filters to Clicks only

Line 74: if (from.HasValue) query = query.Where(m => m.Date >= from.Value.ToUniversalTime());
// → Date >= 2026-06-15

Line 75: if (to.HasValue) query = query.Where(m => m.Date <= to.Value.ToUniversalTime());
// → Date <= 2026-06-16
```

Generated EF Core SQL (roughly):
```sql
SELECT * FROM "CampaignMetrics"
WHERE "TenantId" = 'tenant-retail-01'
  AND "CampaignId" = 'camp-summer-sale-2026'
  AND "Metric" = 'Clicks'
  AND "Date" >= '2026-06-15'
  AND "Date" <= '2026-06-16'
```

Returns 2 rows: (100 clicks on 06-15) + (50 clicks on 06-16)

**Back in InsightsService (line 60):**
```csharp
foreach (var m in metrics)
{
    switch (m.Metric)
    {
        case MetricType.Clicks:
            clicks = (clicks ?? 0) + m.Count;  // 0 → 100 → 150
            break;
        case MetricType.Impressions: ...
        case MetricType.ClickToBasket: ...
    }
}
return new MetricsResponse("camp-summer-sale-2026", clicks: 150, ...);
```

Response:
```json
{
  "data": {
    "campaignId": "camp-summer-sale-2026",
    "clicks": 150,
    "impressions": null,
    "clickToBasket": null,
    "startDate": "2026-06-15",
    "endDate": "2026-06-16"
  },
  "meta": { "timestamp": "2026-06-16T12:00:10Z" }
}
```

Note: The **`/metrics` endpoint only reads from PostgreSQL**, not Redis. It does not include real-time unflushed counters. The individual endpoints (`/clicks`, `/impressions`, `/clickToBasket`) combine Redis + DB, but `/metrics` is DB-only.

---

## Use Case 8: Error Scenario — Invalid EventType

### Dummy Payload

```json
{
  "eventId": "evt-007-invalid",
  "tenantId": "tenant-retail-01",
  "userId": "user-alice-99",
  "campaignId": "camp-summer-sale-2026",
  "eventType": "BogusType",
  "timestamp": "2026-06-16T12:00:00Z"
}
```

### Walkthrough

**File:** `src/RetailMedia.Application/Services/EventIngestionService.cs:22`

```csharp
Line 22: if (!Enum.TryParse<EventType>("BogusType", ignoreCase: true, out var eventType))
Line 23:     throw new ArgumentException($"Invalid event type: {request.EventType}");
```

`Enum.TryParse` with `"BogusType"` returns `false` → `ArgumentException` thrown.

**File:** `src/RetailMedia.Api (actually EventCollector)/Middleware/ErrorHandlingMiddleware.cs`

```csharp
// Not shown here, but the pattern in RetailMedia.Api:
// ArgumentException → 400 Bad Request
// All others → 500 Internal Server Error
```

Client receives:
```json
{
  "error": "Invalid event type: BogusType"
}
```

Status: **400 Bad Request**. No Kafka message is published. No processing occurs.

---

## Summary of All Event Types

| # | EventType | Handler(s) | Redis Effect | DB Effect |
|---|-----------|-----------|-------------|-----------|
| 1 | `AdImpression` | `ImpressionHandler` | `INCR campaign:{id}:impressions` | None (until flush) |
| 2 | `AdClick` | `ClickHandler` + `AttributionHandler.HandleClickAsync` | `INCR campaign:{id}:clicks` + `SETEX session:{t}:{u}` TTL=30min | None |
| 3 | `AddToCart` (attributed) | `AttributionHandler.HandleAddToCartAsync` | `INCR campaign:{id}:clickToBasket` (if session found) | None |
| 4 | `AddToCart` (not attributed) | `AttributionHandler.HandleAddToCartAsync` | None (session not found → early return) | None |
| 5 | `ProductView` | `default` (unhandled) | None | None |
| 6 | `Purchase` | `default` (unhandled) | None | None |

### API Endpoint Summary

| Endpoint | Data Source | Real-time? |
|----------|-----------|-----------|
| `GET /ad/{id}/clicks` | Redis GET + PostgreSQL SUM | Yes (combines both) |
| `GET /ad/{id}/impressions` | Redis GET + PostgreSQL SUM | Yes |
| `GET /ad/{id}/clickToBasket` | Redis GET + PostgreSQL SUM | Yes |
| `GET /ad/{id}/metrics` | PostgreSQL only (EF Core query) | No (DB only, no Redis) |
