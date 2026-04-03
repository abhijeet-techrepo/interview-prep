# Messaging Options for Event-Driven Systems

Choose the right messaging technology based on delivery guarantees, ordering requirements, and scale.

→ Back to [Event-Driven Design](./README.md)

---

## Message Queues Overview

| Characteristic | Meaning | Why It Matters |
|---|---|---|
| **Delivery Guarantee** | At-most-once, At-least-once, Exactly-once | Determines if messages can be lost or duplicated |
| **Ordering** | FIFO guarantee per partition/queue | Critical for systems where sequence matters |
| **Persistence** | Messages written to disk | Durability during failures |
| **Replay** | Re-read past messages | Helps with recovery, debugging, new consumers |
| **Max Scale** | Throughput capacity (msgs/sec) | Must handle peak load + headroom |

---

## Master Comparison Table

| Option | Delivery | Ordering | Persistence | Replay | Max Scale | Best For | Avoid When |
|--------|----------|----------|-------------|--------|-----------|----------|-----------|
| **Message Queues** | At-least-once | Per-queue | Disk | Limited window | 10K-100K msgs/s | Job queues, task distribution | Extreme throughput, high replay needs |
| **Kafka** | At-least-once | Per-partition | Disk (configurable) | Full history | 1M+ msgs/s | Event sourcing, audit logs, high volume | Simple fire-and-forget, low latency critical |
| **Redis Pub/Sub** | At-most-once | Best-effort | Memory only | No | 500K+ msgs/s | Real-time dashboards, notifications | Durability required, message loss unacceptable |
| **AWS SNS/SQS** | At-least-once | Per-queue (SQS) | Disk | No | 100K+ msgs/s (burst) | AWS-native workflows, multi-consumer fan-out | Complex ordering, cost-sensitive scale |
| **RabbitMQ** | At-least-once | Per-queue | Disk | Limited | 50K-500K msgs/s | Complex routing, traditional message broker | Extreme scale (choose Kafka) |
| **Google Pub/Sub** | At-least-once | Best-effort | Disk (7 days) | 7-day window | 1M+ msgs/s | Google Cloud workflows, global scale | On-premises, hybrid clouds |

---

## Message Queues (Generic)

### Use Cases
- Job queues (email, image processing)
- Task distribution across workers
- Decoupling service-to-service communication
- Simple point-to-point patterns

### Characteristics
- **Delivery**: At-least-once (with deduplication for exactly-once)
- **Ordering**: FIFO per queue
- **Persistence**: Disk-backed
- **Replay**: Limited to configurable retention window

### Considerations
- ✅ Mature, well-understood pattern
- ✅ Dead-letter queues built-in
- ❌ Limited horizontal scalability vs. Kafka
- ❌ Replay window typically 1-30 days

---

## Apache Kafka

### Use Cases
- Event streaming at massive scale
- Event sourcing and audit logs
- Stream processing (join, filter, aggregate)
- Multi-region replication
- Building data lakes

### Key Features
- **Delivery**: At-least-once (configurable)
- **Ordering**: Strict per-partition (can be lossy across partitions)
- **Persistence**: Configurable retention (days to unlimited)
- **Replay**: Full history available
- **Throughput**: 1M+ messages/sec per broker

### Architecture Concepts
| Concept | Purpose |
|---------|---------|
| **Partition** | Parallel processing unit; preserves ordering |
| **Consumer Group** | Multiple consumers read same topic independently |
| **Offset** | Message position; enables replay and resumption |
| **Replication Factor** | Fault tolerance (typically 3) |

### Configuration Tradeoffs
| Setting | Trade-off |
|---------|-----------|
| `acks=0` | Fastest; risk of data loss |
| `acks=1` | Balanced; leader lost = data loss |
| `acks=all` | Durability; slower writes |
| `compression=snappy/lz4` | Faster network; CPU cost |

### Interview Talking Points
- "Kafka scales horizontally; add partitions for more throughput"
- "Consumer lag = difference between latest offset and consumer position"
- "Rebalancing happens when consumers join/leave; causes brief pauses"
- "Use log compaction for state topics; keeps latest value per key"

### Anti-patterns
- ❌ Using Kafka for simple fire-and-forget messaging
- ❌ Single partition with multiple producers (bottleneck)
- ❌ Ignoring consumer lag in production

---

## Redis Pub/Sub

### Use Cases
- Real-time dashboards and notifications
- Live chat and collaborative tools
- Cache invalidation broadcasts
- Leaderboard updates

### Key Features
- **Delivery**: At-most-once (fire-and-forget)
- **Ordering**: Best-effort
- **Persistence**: Memory-only (lost on restart)
- **Replay**: None
- **Throughput**: 500K+ messages/sec

### Memory Considerations
| Scenario | Impact |
|----------|--------|
| High message rate + long retention | Memory bloat; eviction |
| Many subscribers | Single publish fans to all |
| Network partition | Subscribers miss messages |

### When NOT to Use
- ❌ Requires guaranteed delivery
- ❌ Message loss unacceptable
- ❌ Need replay capability
- ❌ Long-lived message retention

> [!NOTE]
> Redis Streams (not Pub/Sub) provide persistence and consumer groups—hybrid of Pub/Sub and Queue patterns.

---

## AWS SNS/SQS

### SNS (Simple Notification Service) — Fan-out Publisher

**Use**: Multi-target broadcasts, topic subscriptions

| Feature | Details |
|---------|---------|
| **Delivery** | At-least-once |
| **Ordering** | No ordering guarantee |
| **Persistence** | Disk-backed, configurable retention |
| **Replay** | No native replay |
| **Fan-out** | Publish once → N subscribers (cost: per delivery) |

### SQS (Simple Queue Service) — Point-to-Point Queue

**Use**: Decoupling, reliable task processing

| Feature | Details |
|---------|---------|
| **Delivery** | At-least-once |
| **Ordering** | FIFO queues available (`.fifo` suffix) |
| **Persistence** | 14-day default retention |
| **Replay** | Limited to retention window |
| **Visibility Timeout** | Message hidden during processing; reappears if not deleted |

### SNS + SQS Pattern (Common)
```
SNS Topic (fan-out) → SQS Queue (durability) → Worker
                   → SQS Queue (durability) → Different Worker
```

**Benefit**: Topic ensures all queues get message; SQS ensures durability.

### Cost Model
- SNS: Per publish + per delivery to SQS
- SQS: Per million requests + per month of retention

### Limitations
- ❌ SQS FIFO: Lower throughput (300 msgs/sec)
- ❌ No native consumer groups
- ❌ Limited ordering in standard queues

---

## RabbitMQ

### Use Cases
- Complex message routing (topic routing, headers-based)
- Traditional enterprise message broker patterns
- Multi-datacenter replication with plugins
- Hybrid on-premises + cloud setups

### Key Features
- **Delivery**: At-least-once (with publisher confirms)
- **Ordering**: Per-queue (queue per subscriber if ordering critical)
- **Persistence**: Disk and/or memory queues
- **Replay**: Limited; no native consumer groups
- **Throughput**: 50K-500K msgs/sec (cluster dependent)

### Exchange Types (Routing Logic)
| Type | Behavior | Use Case |
|------|----------|----------|
| **Direct** | Key match: routing_key == binding_key | Task queues |
| **Topic** | Wildcard matching: `logs.*.error` | Pub/sub with filters |
| **Fanout** | Copy to all bindings | Broadcasts |
| **Headers** | Match on message headers | Content-based routing |

### Configuration Patterns
- **Durable Queues**: Survive broker restarts
- **Publisher Confirms**: Wait for broker ACK before declaring success
- **Consumer Ack**: Manual vs. auto-ack (safety vs. performance)

### When to Choose RabbitMQ
- ✅ Complex routing requirements
- ✅ Need flexible exchange patterns
- ✅ Hybrid cloud (Kubernetes + on-premises)
- ❌ Need Kafka-scale throughput → use Kafka
- ❌ Simple queue → use SQS

---

## Google Pub/Sub

### Use Cases
- Google Cloud-native event streaming
- Global, low-latency message distribution
- Bigquery and Dataflow integration
- Multi-region fan-out at scale

### Key Features
- **Delivery**: At-least-once
- **Ordering**: Best-effort (can enable per-subscriber ordering)
- **Persistence**: 7-day default retention; adjustable
- **Replay**: 7-day window (or custom retention)
- **Throughput**: 1M+ msgs/sec
- **Push vs. Pull**: Both supported (unlike SNS/SQS which is mostly pull)

### Comparison to Kafka
| Aspect | Pub/Sub | Kafka |
|--------|---------|-------|
| **Global Scale** | Native multi-region | Requires replication |
| **Serverless** | Fully managed | Self-managed or Confluent |
| **Consumer Groups** | Built-in (subscriptions) | Native |
| **Schema Registry** | Separate service | Confluent plugin |
| **Cost Model** | Per-GB transferred | Per-broker hours |

### Integration Advantages
- Dataflow: Unified batch + streaming
- Bigquery: Direct load from Pub/Sub
- Cloud Functions: Native trigger

---

## Decision Flowchart

```
START: Need Messaging?
  │
  ├─→ Fire-and-forget, no durability?
  │   └─→ YES: Redis Pub/Sub ✓
  │   └─→ NO: Continue...
  │
  ├─→ Extreme scale (1M+ msgs/sec) + event sourcing?
  │   ├─→ YES + AWS ecosystem → Kafka on MSK ✓
  │   ├─→ YES + GCP → Google Pub/Sub ✓
  │   └─→ NO: Continue...
  │
  ├─→ Complex routing (topic patterns, headers)?
  │   ├─→ YES + on-premises → RabbitMQ ✓
  │   ├─→ YES + AWS → SNS + SQS ✓
  │   └─→ NO: Continue...
  │
  ├─→ Simple point-to-point queue + AWS?
  │   ├─→ YES + need ordering → SQS FIFO ✓
  │   └─→ YES + standard → SQS Standard ✓
  │
  └─→ Need full-history replay + stream processing?
      └─→ YES: Apache Kafka ✓
```

---

## Quick Decision Matrix

| Question | Answer → Best Choice |
|----------|----------------------|
| "How fast must we process?" | <10K msgs/s: Any | 10K-100K: SQS/RabbitMQ | >100K: Kafka/Pub/Sub |
| "How long keep messages?" | <1 day: Redis | 1-7 days: SQS/Pub/Sub | Unlimited: Kafka |
| "Must guarantee delivery?" | YES: Any except Pub/Sub | NO: Redis Pub/Sub |
| "Need consumer replay?" | YES: Kafka/Pub/Sub | NO: SQS/RabbitMQ |
| "Where deployed?" | AWS: SQS | GCP: Pub/Sub | Multi-cloud: Kafka |
| "Ordering critical?" | YES: Kafka/SQS FIFO | NO: SNS/Pub/Sub |

---

## Interview Tip

**Senior/Principal-level answer** to "How do you choose a messaging system?"

> I start with **non-functional requirements**: throughput (msgs/sec), latency (p99), retention window, and geographic distribution. Then I check **delivery semantics**—does the business tolerate duplicates or require exactly-once?
>
> For **high-volume event streaming** with replay needs, **Kafka wins** on scalability and consumer groups; we've scaled it to billions of events daily. For **AWS-only shops**, **SNS + SQS** is cost-effective and operationally simple, though SNS lacks native consumer groups. **Redis Pub/Sub** is great for **real-time notifications** where loss is acceptable—chat, dashboards.
>
> The **partition/topic strategy** is critical: in Kafka, one partition per logical stream ensures ordering, but creates a single-producer bottleneck. We use **consumer groups** for scale. In RabbitMQ, we leverage **exchange types**—topic exchanges for flexible subscriptions, direct for strict routing.
>
> Last, I consider **operational burden**: Kafka requires more infrastructure (ZooKeeper, broker management); managed services like **Google Pub/Sub or AWS MSK** reduce this. For a startup, **SQS + SNS is pragmatic**; for a platform team, **Kafka enables self-serve streaming**. Always validate against your SLA—message loss, latency, and ordering requirements shape everything.

---

## See Also
- [Event Sourcing](./03-event-sourcing.md)
- [Consumer Patterns](./04-consumer-patterns.md)
- [Dead Letter Queues](./05-dlq-strategies.md)
- [Kafka Deep Dive](./kafka-internals.md)
