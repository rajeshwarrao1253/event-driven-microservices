# Event-Driven Microservices Platform

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://golang.org)
[![Node.js Version](https://img.shields.io/badge/Node.js-20+-339933?logo=node.js)](https://nodejs.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.6-231F20?logo=apache-kafka)](https://kafka.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7.0-DC382D?logo=redis)](https://redis.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-24.0-2496ED?logo=docker)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-grade event-driven microservices platform demonstrating distributed systems patterns including **Event Sourcing**, **CQRS**, **Saga Pattern**, **Outbox Pattern**, and **Idempotent Consumers**. Built with Go and Node.js/TypeScript, communicating asynchronously via Apache Kafka.

---

## Architecture Overview

```
                    ┌─────────────────┐
                    │   API Gateway   │
                    │     (Go)        │
                    │  :8080          │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
    ┌─────────▼────────┐    │    ┌─────────▼────────┐
    │  Order Service   │    │    │ Payment Service  │
    │     (Go)         │    │    │     (Go)         │
    │  :8081           │    │    │  :8082           │
    └────────┬─────────┘    │    └────────┬─────────┘
             │              │              │
             │    ┌─────────▼──────────┐   │
             │    │   Event Bus        │   │
             └───►│   (Kafka/Redpanda) │◄──┘
                  │   orders.created   │
                  │   payments.processed│
                  │   inventory.reserved│
                  │   notifications.sent│
                  └──────────┬───────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
    ┌─────────▼─────────┐   │   ┌─────────▼─────────────┐
    │ Inventory Service │   │   │ Notification Service  │
    │   (Node.js/TS)    │   │   │   (Node.js/TS)        │
    │    :8083          │   │   │    :8084              │
    └───────────────────┘   │   └───────────────────────┘
                            │
    ┌───────────────────────┼───────────────────────┐
    │                       │                       │
    ▼                       ▼                       ▼
┌──────────┐         ┌──────────┐           ┌──────────┐
│PostgreSQL│         │  Redis   │           │PostgreSQL│
│ (Orders) │         │  (Cache) │           │(Payments)│
└──────────┘         └──────────┘           └──────────┘
```

### Data Flow (Saga Pattern)

```
┌─────────────┐     orders.created      ┌─────────────────┐
│   Client    │────────────────────────►│  Order Service  │
└─────────────┘                         │  (Saga Orchestrator)
                                        └────────┬────────┘
                                                 │
                    ┌────────────────────────────┘
                    │ payments.processed
                    ▼
┌─────────────────────────────────────────────────────────────┐
│                     Saga Transaction Flow                    │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   ORDER      │───►│   PAYMENT    │───►│  INVENTORY   │  │
│  │  CREATED     │    │  PROCESSED   │    │   RESERVED   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                   │            │
│         ▼                   ▼                   ▼            │
│    ┌──────────┐       ┌──────────┐       ┌──────────┐      │
│    │ On Fail: │       │ On Fail: │       │ On Fail: │      │
│    │  Cancel  │◄──────│  Refund  │◄──────│  Release │      │
│    │  Order   │       │  Payment │       │  Stock   │      │
│    └──────────┘       └──────────┘       └──────────┘      │
└─────────────────────────────────────────────────────────────┘
                    │
                    └────────────────────────────────────────►
                                               notifications.sent
```

---

## Services

| Service | Language | Port | Responsibility |
|---------|----------|------|----------------|
| **API Gateway** | Go | 8080 | Request routing, auth middleware, rate limiting |
| **Order Service** | Go | 8081 | Order lifecycle, saga orchestration, PostgreSQL |
| **Payment Service** | Go | 8082 | Payment processing, fraud detection, PostgreSQL |
| **Inventory Service** | Node.js/TS | 8083 | Stock management, Redis cache, reservation |
| **Notification Service** | Node.js/TS | 8084 | Email/SMS notifications, event listener |

---

## Kafka Topics

| Topic | Purpose | Partitions |
|-------|---------|------------|
| `orders.created` | New order events | 6 |
| `orders.cancelled` | Order cancellation | 6 |
| `payments.processed` | Payment success/failure | 6 |
| `payments.refunded` | Payment refunds | 6 |
| `inventory.reserved` | Stock reservation | 6 |
| `inventory.released` | Stock release (rollback) | 6 |
| `notifications.sent` | Notification dispatch | 3 |
| `dead-letter` | Failed message DLQ | 3 |

---

## Key Design Patterns

### 1. Event Sourcing
All state changes are captured as immutable events in Kafka. Services reconstruct state by replaying events.

### 2. CQRS (Command Query Responsibility Segregation)
- **Write Model**: PostgreSQL handles commands (create, update)
- **Read Model**: Redis cache for fast queries
- **Event Bus**: Kafka synchronizes read/write models

### 3. Saga Pattern
Distributed transactions coordinated via events. Compensating actions on failure:
- Payment fails → Cancel order
- Inventory fails → Refund payment → Cancel order

### 4. Outbox Pattern
Database writes and event publishing are atomic. Events written to an outbox table and published by a relay.

### 5. Idempotent Consumers
All consumers track processed message IDs to prevent duplicate processing.

---

## Technology Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Gateway | Go + stdlib net/http | 1.21+ |
| Services (Core) | Go + gorilla/mux | 1.21+ |
| Services (Support) | Node.js + Express | 20+ |
| Event Bus | Redpanda (Kafka-compatible) | 23.3+ |
| Databases | PostgreSQL | 15 |
| Cache | Redis | 7.0 |
| Orchestration | Docker Compose | 2.20+ |
| Observability | Structured logging (zap) | - |

---

## Getting Started

### Prerequisites
- Docker 24.0+ and Docker Compose
- Go 1.21+ (for local development)
- Node.js 20+ (for local development)

### Quick Start

```bash
# Clone the repository
git clone https://github.com/rajeshwarrao1253/event-driven-microservices.git
cd event-driven-microservices

# Start all services
docker-compose up -d

# Verify all services are healthy
docker-compose ps

# View logs
docker-compose logs -f api-gateway

# Stop everything
docker-compose down -v
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `http://localhost:8080/health` | GET | Gateway health check |
| `http://localhost:8080/api/orders` | POST | Create new order |
| `http://localhost:8080/api/orders/:id` | GET | Get order by ID |
| `http://localhost:8080/api/orders/:id` | DELETE | Cancel order |
| `http://localhost:8080/api/payments/:id` | GET | Get payment status |
| `http://localhost:8080/api/inventory/:sku` | GET | Check stock |
| `http://localhost:8080/api/inventory/:sku` | PUT | Update stock |

### Example: Create an Order

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-123",
    "items": [
      {"sku": "PROD-001", "quantity": 2, "price": 29.99},
      {"sku": "PROD-002", "quantity": 1, "price": 49.99}
    ],
    "shipping_address": {
      "street": "123 Main St",
      "city": "San Francisco",
      "zip": "94102"
    }
  }'
```

---

## Project Structure

```
event-driven-microservices/
├── README.md
├── docker-compose.yml
├── docs/
│   └── ARCHITECTURE.md
├── shared/
│   └── events/
│       └── types.go          # Shared event schemas
├── api-gateway/
│   ├── main.go
│   ├── Dockerfile
│   └── middleware/
│       ├── auth.go
│       ├── logging.go
│       └── ratelimit.go
└── services/
    ├── order-service/
    │   ├── main.go
    │   ├── Dockerfile
    │   ├── handlers/
    │   ├── models/
    │   ├── repository/
    │   ├── outbox/
    │   └── saga/
    ├── payment-service/
    │   ├── main.go
    │   ├── Dockerfile
    │   ├── handlers/
    │   ├── models/
    │   └── processor/
    ├── inventory-service/
    │   ├── src/
    │   │   ├── index.ts
    │   │   ├── handlers/
    │   │   ├── models/
    │   │   └── services/
    │   ├── package.json
    │   ├── tsconfig.json
    │   └── Dockerfile
    └── notification-service/
        ├── src/
        │   ├── index.ts
        │   ├── handlers/
        │   └── providers/
        ├── package.json
        ├── tsconfig.json
        └── Dockerfile
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Go for core services** | High concurrency, low latency, strong typing for financial operations |
| **Node.js for support** | Rapid development, rich ecosystem for integrations |
| **Redpanda over Kafka** | Simpler operations, no ZooKeeper dependency, better performance |
| **Separate DBs per service** | Database per service pattern ensures loose coupling |
| **Saga over 2PC** | Better availability, no distributed locks, natural fit for events |

---

## Monitoring & Observability

All services emit structured JSON logs with:
- Request correlation IDs
- Event trace IDs
- Processing latency metrics
- Error stacks

Health check endpoints available at `/health` on all services.

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'feat: add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Standards
- Go: `gofmt`, `golint`, `go vet`
- TypeScript: `eslint`, `prettier`
- All new code must include tests
- Follow conventional commits

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- [Chris Richardson's Microservices Patterns](https://microservices.io/book)
- [Redpanda Documentation](https://docs.redpanda.com)
- [Confluent Kafka Patterns](https://www.confluent.io/design-patterns/)
