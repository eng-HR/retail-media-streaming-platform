# Retail Media Streaming Platform – Interview Case Study Plan

## Repository Name
`retail-media-streaming-platform`

---

# 1. Objective

Design a real-time Retail Media platform that:

- Ingests customer and ad engagement events from retailer websites.
- Processes events in real-time.
- Generates campaign insights.
- Supports multiple retailers (multi-tenancy).
- Exposes APIs for real-time and historical reporting.
- Scales to Black Friday level traffic.

---

# 2. Assumptions

### Traffic

- 50M–500M events/day
- Peak traffic 10x during major retail events
- Events arrive from multiple retailer properties

### Latency Targets

- Event ingestion: < 100ms
- Stream processing: < 2 seconds
- API response: < 200ms (P95)

---

# 3. High-Level Architecture

```text
Retailer Websites / SDKs
            |
            V
     API Gateway
            |
            V
      Event Collector
            |
            V
         Kafka
            |
    +-------+-------+
    |               |
    V               V
 Apache Flink    Raw Event Sink
    |               |
    V               V
Aggregations     Data Lake (S3)
    |
+---+---------------+
|                   |
V                   V
Redis          BigQuery/Snowflake
|                   |
V                   |
PostgreSQL          |
|                   |
+--------+----------+
         |
         V
    Insights APIs
```

---

# 4. Technology Choices

| Layer | Technology |
|---------|---------|
| API Layer | ASP.NET Core 8 |
| Event Ingestion | Kafka |
| Stream Processing | Apache Flink |
| Cache | Redis |
| OLTP Storage | PostgreSQL |
| Analytics | BigQuery |
| Data Lake | S3 |
| Container Platform | Kubernetes |
| Monitoring | Prometheus + Grafana |
| Logging | ELK |
| Tracing | OpenTelemetry |
| CI/CD | GitHub Actions / GitLab |
| Cloud | GCP Preferred |

---

# 5. Event Ingestion Design

### Event Schema

```json
{
  "eventId":"123",
  "tenantId":"tesco",
  "userId":"u123",
  "campaignId":"c789",
  "eventType":"AD_CLICK",
  "timestamp":"2026-01-01T10:00:00Z"
}
```

### Event Types

- Product View
- Add To Cart
- Purchase
- Ad Impression
- Ad Click

### Kafka Partitioning

Partition Key:

```text
tenantId + campaignId
```

Benefits:

- Ordering guarantee per campaign
- Balanced partition distribution
- Horizontal scalability

---

# 6. Real-Time Processing

## Why Flink

- True streaming
- Low latency
- Stateful processing
- Exactly-once support
- Event-time processing

### Processing Pipelines

#### Impression Counter

Ad Impression -> Aggregation -> Impression Count

#### Click Counter

Ad Click -> Aggregation -> Click Count

#### Click-To-Basket Attribution

Ad Click -> Store State
Add To Cart -> Lookup State
If within attribution window -> Increment Metric

### Stateful Processing

Keyed State:

```text
Key = UserId
```

Store:

- Last clicked campaign
- Timestamp
- Attribution window

---

# 7. Data Storage Strategy

## Raw Events

### S3

Retention:

- 1–3 years
- Cheap storage
- Replay support

Partition Structure

```text
tenant/year/month/day/hour
```

---

## Real-Time Metrics

### PostgreSQL

Stores:

- Campaign metrics
- Aggregated counters
- Metadata

Reason:

- Familiar ecosystem
- Strong consistency
- Fits Dunnhumby stack

---

## Cache Layer

### Redis

Used for:

- Campaign counters
- Hot metrics
- API caching

TTL:

30–60 seconds

---

## Historical Analytics

### BigQuery

Stores:

- Daily aggregates
- Historical trends
- BI reporting

---

# 8. API Design

## Required Endpoints

```http
GET /campaigns/{id}/clicks

GET /campaigns/{id}/impressions

GET /campaigns/{id}/clickToBasket
```

## Enhanced Endpoint

```http
GET /campaigns/{id}/metrics
```

Example

```http
GET /campaigns/123/metrics
?metric=clicks
&startDate=2026-01-01
&endDate=2026-01-31
```

---

# 9. Code Walkthrough Plan

Implement:

```http
GET /campaigns/{campaignId}/clicks
```

Technology:

- ASP.NET Core 8
- PostgreSQL
- Redis

Architecture:

```text
Controller
    |
Service
    |
Cache
    |
Repository
    |
PostgreSQL
```

### Demonstrate

- Clean Architecture
- SOLID Principles
- Dependency Injection
- Unit Testing
- Logging
- Health Checks
- Docker

---

# 10. Multi-Tenancy Strategy

## Tenant Identification

Every request contains:

```text
tenantId
```

## Shared Infrastructure

Logical Isolation:

```text
tenantId column
```

Benefits:

- Lower cost
- Easier operations

## Enterprise Tenants

Dedicated resources possible for large retailers.

### Security

JWT Claims

```json
{
  "tenantId":"tesco"
}
```

---

# 11. Scalability Design

## Kafka

Scale by:

- Adding partitions
- Adding brokers

## Flink

Scale by:

- Additional task managers
- Increased parallelism

## APIs

Kubernetes HPA

Scale based on:

- CPU
- Memory
- Request volume

---

# 12. Monitoring & Observability

## Metrics

### Kafka

- Consumer lag
- Throughput
- Partition skew

### Flink

- Processing latency
- Backpressure
- Checkpoint duration

### APIs

- P95 latency
- Error rate
- Request volume

## Tooling

- Prometheus
- Grafana
- ELK
- OpenTelemetry

---

# 13. Trade-Off Discussion

## Real-Time vs Accuracy

Real-time:

- Faster insights
- Possible late-event inaccuracies

Accuracy:

- Batch reconciliation
- Slight delay

Approach:

Real-time counters + nightly reconciliation.

---

## PostgreSQL vs Cassandra

PostgreSQL

Pros:

- Team familiarity
- Strong consistency

Cons:

- Lower write scale

Cassandra

Pros:

- Massive write throughput

Cons:

- Operational complexity

Decision:

PostgreSQL initially, Cassandra if scale demands.

---

# 14. Cost Optimization

Hot Data:

- Redis
- PostgreSQL
- 30 days

Warm Data:

- BigQuery
- 12 months

Cold Data:

- S3
- Multi-year retention

---

# 15. Interview Presentation Flow

### Slide 1

Problem Statement

### Slide 2

Requirements & Assumptions

### Slide 3

High-Level Architecture

### Slide 4

Event Ingestion

### Slide 5

Stream Processing

### Slide 6

Storage Layer

### Slide 7

API Design

### Slide 8

Multi-Tenancy

### Slide 9

Scalability

### Slide 10

Monitoring

### Slide 11

Trade-Offs

### Slide 12

Code Walkthrough

### Slide 13

Future Enhancements

- Real-time dashboards
- ML-based attribution
- Budget pacing
- Fraud detection

---

# Final Recommendation

Architecture:
Kafka + Flink + Redis + PostgreSQL + BigQuery + Kubernetes

Code:
ASP.NET Core 8 using Clean Architecture

Positioning:
Present the solution as a production-grade Retail Media Platform capable of supporting multiple retailers, billions of events, and real-time campaign insights.
