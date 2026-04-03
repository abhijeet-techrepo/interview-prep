# Event-Driven Design

> Decouple services, scale independently, and build resilient distributed systems using asynchronous messaging patterns.

[← Back to Architecture Patterns](../README.md)

---

## What Is Event-Driven Architecture?

- A design paradigm where **state changes (events)** are published and consumed asynchronously
- Producers don't know (or care) who consumes their events
- Enables loose coupling, independent scaling, and temporal decoupling
- Core building block for microservices, CQRS, and real-time data pipelines

---

## Navigation

| # | Topic | File | What You'll Learn |
|---|-------|------|-------------------|
| 1 | Core Principles | [01-principles.md](./01-principles.md) | Foundational rules, common mistakes, real-world analogies |
| 2 | Messaging Options | [02-messaging-options.md](./02-messaging-options.md) | Kafka vs RabbitMQ vs SQS vs Redis — comparison matrix |
| 3 | Kafka Internals | [03-kafka-internals.md](./03-kafka-internals.md) | Topics, partitions, ISR, ZooKeeper vs KRaft |
| 4 | Kafka Configs | [04-kafka-configs.md](./04-kafka-configs.md) | Broker, producer, consumer config tables with recommendations |
| 5 | Delivery Semantics | [05-delivery-semantics.md](./05-delivery-semantics.md) | At-most/at-least/exactly-once — configs, risks, patterns |
| 6 | Kafka Performance | [06-kafka-performance.md](./06-kafka-performance.md) | Tuning knobs, partition math, lag monitoring |
| 7 | Outbox Pattern | [07-outbox-pattern.md](./07-outbox-pattern.md) | Dual-write problem, transactional outbox flow |
| 8 | Saga Pattern | [08-saga-pattern.md](./08-saga-pattern.md) | Choreography vs orchestration, compensating transactions |

---

## Interactive View

> Open [kafka-knowledge-graph.html](./kafka-knowledge-graph.html) for a single-page dark-themed interactive version with progress tracking.

---

## Quick Reference: Events vs REST

| Dimension | REST / Sync | Events / Async |
|-----------|-------------|----------------|
| Coupling | Tight — caller knows callee | Loose — producer doesn't know consumers |
| Latency | Blocking — waits for response | Non-blocking — fire and forget |
| Failure impact | Cascading — one service down = chain fails | Isolated — consumers retry independently |
| Scaling | Scale both sides together | Scale consumers independently |
| Ordering | Request order = execution order | Ordering per partition/key |
| Best for | CRUD, queries, user-facing reads | Workflows, notifications, data pipelines, audit logs |
| Avoid when | High-throughput fire-and-forget | Need synchronous response (e.g., "is this username taken?") |

---

## Interview Tip

> **"When would you choose event-driven over synchronous REST?"**
>
> *"I reach for event-driven architecture when services need to be decoupled for independent deployment and scaling — especially for workflows that span multiple bounded contexts. The classic signal is: if the downstream action can happen asynchronously (notifications, analytics, audit), an event is the right tool. I keep REST for cases where the caller genuinely needs an immediate, consistent response — like checking inventory before confirming an order. In practice, most production systems use both: REST at the edge for user-facing reads, events internally for state propagation and side effects."*
