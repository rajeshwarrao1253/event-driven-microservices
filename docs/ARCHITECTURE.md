# Architecture Documentation

## Event-Driven Microservices Platform

**Version:** 1.0.0  
**Last Updated:** 2024

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Event Flow Diagram](#event-flow-diagram)
3. [Saga Pattern Implementation](#saga-pattern-implementation)
4. [CQRS Read/Write Separation](#cqrs-readwrite-separation)
5. [Outbox Pattern](#outbox-pattern)
6. [Idempotent Consumers](#idempotent-consumers)
7. [Service Communication](#service-communication)
8. [Data Management](#data-management)
9. [Error Handling & Resilience](#error-handling--resilience)
10. [Deployment Strategy](#deployment-strategy)
11. [Security Considerations](#security-considerations)
12. [Scaling Strategy](#scaling-strategy)

---

## System Overview

The platform consists of **5 independent services** communicating exclusively through **Kafka events**. There are no synchronous service-to-service calls, ensuring loose coupling and high availability.

### Core Principles

- **Database Per Service**: Each service owns its data
- **Event-Driven Architecture**: All communication via Kafka
- **Eventual Consistency**: CQRS with read/write separation
- **Compensating Transactions**: Saga pattern for distributed transactions
- **Idempotency**: All consumers handle duplicate events safely

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           API Gateway (Go)                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Routing    │  │   CORS/Auth  │  │ Rate Limiter │  │   Logging    │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Kafka Event Bus (Redpanda)                        │
│                                                                          │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────────────────────┐│
│  │  orders.created │ │payments.processed│ │   inventory.reserved        ││
│  │  (6 partitions) │ │ (6 partitions)  │ │   (6 partitions)             ││
│  └────────────────┘ └────────────────┘ └────────────────────────────────┘│
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────────────────────┐│
│  │orders.cancelled │ │payments.refunded │ │   inventory.released        ││
│  └────────────────┘ └────────────────┘ └────────────────────────────────┘│
│  ┌────────────────┐ ┌────────────────┐                                  ││
│  │notifications.sent│ │   dead-letter   │                                  ││
│  │ (3 partitions)  │ │ (3 partitions)  │                                  ││
│  └────────────────┘ └────────────────┘                                  ││
└─────────────────────────────────────────────────────────────────────────┘
        │                        │                        │
        ▼                        ▼                        ▼
┌───────────────┐    ┌────────────────┐    ┌───────────────────────┐
│ Order Service │    │ Payment Service│    │  Inventory Service    │
│     (Go)      │    │     (Go)       │    │    (Node.js/TS)       │
│               │    │                │    │                       │
│ • Saga Orc.   │    │ • Payment Proc │    │ • Stock Mgmt          │
│ • Outbox Pat. │    │ • Fraud Detect │    │ • Redis Cache         │
│ • PostgreSQL  │    │ • Idempotency  │    │ • Lua Scripts         │
│               │    │ • PostgreSQL   │    │ • Reservation Logic   │
└───────┬───────┘    └───────┬────────┘    └───────────┬───────────┘
        │                    │                        │
        └────────────────────┼────────────────────────┘
                             │
                             ▼
               ┌───────────────────────┐
               │ Notification Service  │
               │    (Node.js/TS)       │
               │                       │
               │ • Event Listener      │
               │ • Multi-channel       │
               │ • Template Engine     │
               │ • DLQ Handler         │
               └───────────────────────┘
```

---

## Event Flow Diagram

### Happy Path: Order Creation Flow

```
┌────────┐          ┌──────────┐         ┌──────────┐          ┌──────────┐
│ Client │          │  Order   │         │  Payment │          │ Inventory│
└───┬────┘          │ Service  │         │ Service  │          │ Service  │
    │               └────┬─────┘         └────┬─────┘          └────┬─────┘
    │                    │                    │                     │
    │ POST /api/orders   │                    │                     │
    │───────────────────►│                    │                     │
    │                    │                    │                     │
    │                    │ 1. Save Order      │                     │
    │                    │ 2. Write Outbox    │                     │
    │                    │ (atomic tx)        │                     │
    │                    │                    │                     │
    │  202 Accepted      │                    │                     │
    │◄───────────────────│                    │                     │
    │                    │                    │                     │
    │                    │ 3. Publish         │                     │
    │                    │    orders.created  │                     │
    │                    │───────────────────►│                     │
    │                    │                    │                     │
    │                    │                    │ 4. Process Payment  │
    │                    │                    │ 5. Save + Outbox    │
    │                    │                    │                     │
    │                    │                    │ 6. Publish          │
    │                    │                    │    payments.processed
    │                    │                    │────────────────────►│
    │                    │                    │                     │
    │                    │                    │                     │ 7. Reserve Stock
    │                    │                    │                     │ 8. Save Reservation
    │                    │                    │                     │
    │                    │                    │                     │ 9. Publish
    │                    │                    │                     │    inventory.reserved
    │                    │                    │◄────────────────────│
    │                    │                    │                     │
    │                    │ 10. Confirm Order  │                     │
    │                    │     OrderConfirmed │                     │
    │                    │     (Saga Complete)│                     │
    │                    │                    │                     │
    │ 11. Notification   │                    │                     │
    │     (async)        │                    │                     │
    │◄──────────────────────────────────────────────────────────────│
```

### Failure Path: Payment Rejected

```
┌────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Client │    │  Order   │    │  Payment │    │ Inventory│
└───┬────┘    │ Service  │    │ Service  │    │ Service  │
    │         └────┬─────┘    └────┬─────┘    └────┬─────┘
    │              │               │               │
    │              │  1. orders.created             │
    │              │──────────────►│               │
    │              │               │               │
    │              │               │ 2. Process    │
    │              │               │    Payment    │
    │              │               │    FAILED     │
    │              │               │               │
    │              │               │ 3. Publish    │
    │              │               │    payments.processed
    │              │               │    status=failed    │
    │              │               │──────────────►│
    │              │               │               │
    │              │               │               │ 4. (No action needed
    │              │               │               │    inventory not touched
    │              │               │               │    due to payment failure)
    │              │               │               │
    │              │◄──────────────│               │
    │              │ 5. Cancel Order               │
    │              │    OrderCancelled              │
    │              │    (Compensate)                │
    │              │               │               │
    │              │               │◄──────────────│
    │              │               │ 6. (No comp.  │
    │              │               │    needed)    │
    │              │               │               │
    │  7. Notify   │               │               │
    │     User     │               │               │
    │◄─────────────────────────────────────────────────────────────│
```

### Failure Path: Inventory Unavailable (Saga Compensation)

```
┌────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ Client │    │  Order   │    │  Payment │    │ Inventory│
└───┬────┘    │ Service  │    │ Service  │    │ Service  │
    │         └────┬─────┘    └────┬─────┘    └────┬─────┘
    │              │               │               │
    │              │  1. orders.created             │
    │              │──────────────►│               │
    │              │               │               │
    │              │               │ 2. Process    │
    │              │               │    Payment OK │
    │              │               │               │
    │              │               │ 3. Publish    │
    │              │               │    payments.processed
    │              │               │    status=completed     │
    │              │               │──────────────►│
    │              │               │               │
    │              │               │               │ 4. Try Reserve Stock
    │              │               │               │    NOT ENOUGH STOCK
    │              │               │               │
    │              │               │               │ 5. Publish
    │              │               │               │    inventory.released
    │              │               │◄──────────────│
    │              │               │               │
    │              │               │ 6. Refund     │
    │              │               │    Payment    │
    │              │               │    (Compensate
    │              │◄──────────────│    Payment)   │
    │              │               │               │
    │              │ 7. Cancel     │               │
    │              │    Order      │               │
    │              │    (Compensate                │
    │              │    Order)     │               │
    │              │               │               │
    │  8. Notify   │               │               │
    │     User     │               │               │
    │◄─────────────────────────────────────────────────────────────│
```

---

## Saga Pattern Implementation

The Saga pattern coordinates a distributed transaction across multiple services using a sequence of local transactions, each followed by an event publication.

### Saga: Order Processing

| Step | Service | Local Action | Event Published | Compensation |
|------|---------|-------------|-----------------|-------------|
| 1 | Order | Create order (pending) | `orders.created` | Cancel order |
| 2 | Payment | Process payment | `payments.processed` | Refund payment |
| 3 | Inventory | Reserve stock | `inventory.reserved` | Release stock |

### Compensation Rules

```
IF Payment fails:
    → Order Service: Cancel order (orders.cancelled)

IF Inventory fails:
    → Payment Service: Refund payment (payments.refunded)
    → Order Service: Cancel order (orders.cancelled)

IF All succeed:
    → Order Service: Confirm order (orders.confirmed)
    → Inventory Service: Commit reservation
```

### Why Saga over 2PC?

| Aspect | 2PC | Saga |
|--------|-----|------|
| Availability | Blocked during prepare | Always available |
| Latency | High (coordination) | Low (async) |
| Complexity | Distributed locks | Simple event handlers |
| Failure Recovery | Complex blocking | Natural compensations |
| Scalability | Limited by coordinator | Horizontally scalable |

---

## CQRS Read/Write Separation

CQRS separates read and write operations to optimize each independently.

### Write Model (Commands)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Command    │────►│   PostgreSQL  │────►│   Outbox     │
│   Handler    │     │   (Source of  │     │   Table      │
│              │     │    Truth)     │     │              │
└──────────────┘     └──────────────┘     └──────────────┘
                                                  │
                                                  ▼
                                           ┌──────────────┐
                                           │ Kafka Events │
                                           └──────────────┘
```

**Command Examples:**
- `CreateOrder` → Insert order row
- `ProcessPayment` → Update payment status
- `ReserveStock` → Update Redis with Lua

### Read Model (Queries)

```
┌──────────────┐     ┌──────────────┐
│    Query     │────►│    Redis     │
│   Handler    │     │    Cache     │
└──────────────┘     └──────────────┘
```

**Query Examples:**
- `GetOrder` → Fetch from PostgreSQL
- `CheckStock` → Fetch from Redis (sub-millisecond)
- `GetPaymentStatus` → Fetch from PostgreSQL

### Event Synchronization

Kafka events keep read models updated:

```
Inventory Service (Write):
  Lua script: HSET stock:SKU reserved +1

↓ publishes inventory.reserved

Order Service (Read Model update):
  UPDATE orders SET status = 'confirmed' WHERE id = ?
```

---

## Outbox Pattern

The Outbox pattern ensures **atomicity** between database writes and event publishing.

### Problem

```
Without Outbox:
  1. BEGIN TX
  2. INSERT INTO orders (...)          ← DB write succeeds
  3. kafka.Publish("orders.created")   ← Publish FAILS
  4. COMMIT                             ← INCONSISTENT STATE!
```

### Solution

```
With Outbox:
  1. BEGIN TX
  2. INSERT INTO orders (...)          ← Order saved
  3. INSERT INTO outbox (topic, key, payload)  ← Event saved (same TX)
  4. COMMIT                             ← BOTH succeed or BOTH fail

  5. Background worker polls outbox:
     SELECT * FROM outbox WHERE processed = FALSE

  6. Worker publishes to Kafka

  7. Worker marks as processed:
     UPDATE outbox SET processed = TRUE WHERE id = ?
```

### Implementation

Each service has an `outbox` table:

```sql
CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(128) NOT NULL,
    key VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,
    headers TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_outbox_unprocessed ON outbox(processed, created_at)
    WHERE processed = FALSE;
```

### Why It Matters

- **Reliability**: Events never lost even if Kafka is temporarily down
- **Ordering**: Events published in creation order
- **Replay**: Unprocessed events can be manually reprocessed
- **Debugging**: Complete audit trail of all events

---

## Idempotent Consumers

All Kafka consumers are idempotent - processing the same event multiple times produces the same result.

### Problem

```
Kafka delivers message M twice:
  T1: Process M → Debit $100 (balance: $400)
  T2: Process M → Debit $100 (balance: $300) ← WRONG!
```

### Solution: Event ID Tracking

```typescript
class IdempotencyStore {
  private processedEvents: Set<string> = new Set();

  async processEvent(event: Event): Promise<void> {
    // Check if already processed
    if (this.processedEvents.has(event.meta.event_id)) {
      return; // Skip duplicate
    }

    // Process event
    await this.handleEvent(event);

    // Mark as processed
    this.processedEvents.add(event.meta.event_id);
  }
}
```

### Go Implementation

The Go services use an in-memory map with TTL:

```go
type IdempotencyStore struct {
    mu     sync.RWMutex
    keys   map[string]time.Time
    ttl    time.Duration
}

func (s *IdempotencyStore) IsProcessed(eventID string) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    _, ok := s.keys[eventID]
    return ok
}
```

### Production: Persistent Idempotency

In production, use Redis or PostgreSQL for distributed idempotency:

```sql
CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

---

## Service Communication

### Async-Only Communication

All services communicate **exclusively** via Kafka events. No REST calls between services.

```
┌──────────────┐         ┌──────────────┐
│ Order Service│ ─XREST─►│Payment Svc   │  ← NEVER DO THIS
└──────────────┘         └──────────────┘

┌──────────────┐         ┌──────────────┐
│ Order Service│ ─KAFKA─►│Payment Svc   │  ← CORRECT
└──────────────┘         └──────────────┘
```

### Why No REST Between Services?

1. **Temporal Decoupling**: Services don't need to be available simultaneously
2. **Rate Independence**: Consumer processes at its own pace
3. **Resilience**: Producer continues even if consumer is down
4. **Scalability**: Multiple consumer instances process events in parallel
5. **Observability**: Complete audit trail of all interactions

### REST Usage

REST is used only for:
- Client → API Gateway (external API)
- API Gateway → Services (routing)
- Service Health Checks

---

## Data Management

### Database Per Service

| Service | Database | Purpose |
|---------|----------|---------|
| Order Service | PostgreSQL | Order data, outbox |
| Payment Service | PostgreSQL | Payment records, outbox |
| Inventory Service | Redis | Stock levels, reservations |
| Notification Service | In-Memory | Notification logs (use DB in prod) |
| API Gateway | None | Stateless |

### Data Isolation

```
┌────────────────────────────────────────────────────────────┐
│                    Order Service                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  PostgreSQL: orders DB                               │  │
│  │  • orders table (order_id, user_id, items, status)   │  │
│  │  • outbox table (events to publish)                  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                  Payment Service                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  PostgreSQL: payments DB                             │  │
│  │  • payments table (payment_id, order_id, status)     │  │
│  │  • outbox table (events to publish)                  │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                 Inventory Service                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Redis: Cache + Stock Store                          │  │
│  │  • inventory:{sku} (quantity, reserved)              │  │
│  │  • reservation:{order_id} (reserved items)           │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

### Event-Driven Data Consistency

Services maintain consistency by reacting to events, not by direct data access:

```
When Payment is processed:
  Payment Service: UPDATE payments SET status = 'completed'
  ↓ publishes payments.processed

Order Service (consumer):
  UPDATE orders SET status = 'confirmed' WHERE id = ?

Notification Service (consumer):
  Send "Payment Successful" email
```

---

## Error Handling & Resilience

### Retry Strategy

| Scenario | Strategy | Max Retries | Backoff |
|----------|----------|-------------|---------|
| Kafka publish fail | Exponential backoff | 3 | 100ms → 400ms → 1600ms |
| DB connection fail | Immediate retry | 5 | 1s fixed |
| External API fail | Circuit breaker | 3 | 1s → 5s → 25s |

### Circuit Breaker

```
CLOSED ──3 failures──► OPEN (30s timeout)
  ▲                      │
  └───success──────── HALF-OPEN
```

### Dead Letter Queue

Failed messages after all retries go to the `dead-letter` topic:

```
Consumer fails 3 times:
  ┌──────────────────────────────────────────┐
  │  1. Receive orders.created              │
  │  2. Process FAIL                         │
  │  3. Retry 1... FAIL                      │
  │  4. Retry 2... FAIL                      │
  │  5. Retry 3... FAIL                      │
  │  6. Publish to dead-letter topic         │
  └──────────────────────────────────────────┘

Dead Letter Event:
  {
    "original_topic": "orders.created",
    "original_key": "ord-123",
    "original_payload": "...",
    "error_message": "insufficient_stock",
    "retry_count": 3,
    "failed_at": "2024-01-15T10:30:00Z"
  }
```

---

## Deployment Strategy

### Docker Compose (Development)

```yaml
# All services with dependencies
# docker-compose up -d
```

### Kubernetes (Production)

```yaml
# Each service as a Deployment with:
# - 3+ replicas for HA
# - HorizontalPodAutoscaler (CPU/memory)
# - Liveness/Readiness probes
# - Resource limits/requests
# - PodDisruptionBudget

apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: order-service
        image: order-service:v1.0.0
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8081
          initialDelaySeconds: 10
          periodSeconds: 15
        readinessProbe:
          httpGet:
            path: /health
            port: 8081
          initialDelaySeconds: 5
          periodSeconds: 5
```

### CI/CD Pipeline

```
Developer pushes code
         │
         ▼
┌─────────────────┐
│  GitHub Actions  │
│  - Lint          │
│  - Unit Tests    │
│  - Build Images  │
│  - Integration   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Staging Env    │
│  - E2E Tests    │
│  - Performance  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Production     │
│  - Canary 5%    │
│  - Rolling 25%  │
│  - Full deploy  │
└─────────────────┘
```

---

## Security Considerations

### Authentication & Authorization

- **API Gateway**: JWT validation, API key management
- **Service-to-Service**: mTLS (mutual TLS) via service mesh
- **Kafka**: SASL/SSL for broker authentication

### Data Protection

- **PII Encryption**: Sensitive fields encrypted at rest
- **TLS**: All external traffic over HTTPS
- **Secrets**: Managed via Vault / Kubernetes Secrets

### Network Security

```
Internet ──► API Gateway ──► Services (internal network)
                    │
                    └───► Kafka (internal network)
                    │
                    └───► PostgreSQL (internal network)
                    │
                    └───► Redis (internal network)
```

---

## Scaling Strategy

### Horizontal Scaling

| Service | Scaling Metric | Target |
|---------|---------------|--------|
| API Gateway | CPU > 70% | 3-10 pods |
| Order Service | CPU > 60% | 3-8 pods |
| Payment Service | Queue depth > 100 | 3-8 pods |
| Inventory Service | CPU > 70% | 2-6 pods |
| Notification Service | CPU > 60% | 2-6 pods |

### Kafka Partition Strategy

```
orders.created: 6 partitions
  → Up to 6 parallel order consumers

payments.processed: 6 partitions
  → Up to 6 parallel payment processors

inventory.reserved: 6 partitions
  → Up to 6 parallel inventory handlers
```

### Database Scaling

- **PostgreSQL**: Read replicas for query load
- **Redis**: Cluster mode for cache sharding

---

## Monitoring & Alerting

### Key Metrics

| Metric | Type | Threshold |
|--------|------|-----------|
| Order Creation Latency | Histogram | P99 < 500ms |
| Payment Processing Time | Histogram | P99 < 2s |
| Kafka Lag | Gauge | < 1000 messages |
| Error Rate | Counter | < 0.1% |
| Saga Completion Time | Timer | P99 < 30s |

### Health Checks

All services expose `/health` with:
- Service status
- Database connectivity
- Kafka connectivity (for producers/consumers)

---

*This architecture document is a living document. Update it as the system evolves.*
