# Event-Driven Design: Core Principles

Event-driven architecture represents a fundamental shift in how systems think about communication and state change. Rather than building tightly coupled request-response chains, we think in terms of *what happened* — immutable facts that multiple independent services can react to. These principles exist because they solve the hardest problems in distributed systems: coupling, consistency, and operational complexity.

→ Back to [Event-Driven Design](./README.md)

---

## Quick Revision Map

> Scan the keywords. If you can explain the one-liner, skip to the next. If not, click the link for the full story.

```
                    ┌──────────────────────────────────────────┐
                    │    EVENT-DRIVEN DESIGN: 10 PRINCIPLES    │
                    └────────────────────┬─────────────────────┘
                                         │
          ┌──────────────────────────────┼──────────────────────────────┐
          │                              │                              │
    COMMUNICATION                      DATA                      CONSISTENCY
    ──────────────                ──────────────              ────────────────
    1. Async Comms                3. Immutability             5. Eventual
       fire-and-forget               append-only log            consistency
       decouple timing               audit trail                not immediate
                                                                 but bounded
    2. Loose Coupling             4. Single Responsibility
       schema is the              6. Event Ordering
       only contract                 one event = one fact        per-partition
       producers ≠ consumers         composable                  not global
          │                              │                              │
          └──────────────────────────────┼──────────────────────────────┘
                                         │
          ┌──────────────────────────────┼──────────────────────────────┐
          │                              │                              │
      RESILIENCE                    EVOLUTION                    ANTI-PATTERNS
    ──────────────              ────────────────              ────────────────
    7. Idempotency              10. Schema Versioning         Choreography
       same msg 2x =               evolve without                spiral
       same result                  breaking consumers         Logic in events
                                                               Payload bloat
    8. Observability            USE WHEN:                      No DLQ
       correlation IDs          → multi-service reactions      Circular deps
       trace everything         → independent deploy           Timestamp
                                → audit trail needed              ordering
    9. Backpressure             AVOID WHEN:
       graceful overload        → simple CRUD
       don't drop, queue        → strong consistency needed
```

### Jump to Section

| # | Principle | One-Liner Trigger | Deep Dive |
|---|-----------|-------------------|-----------|
| 1 | **Async Communication** | email vs phone call; fire event, don't wait | [→ details](#1-asynchronous-communication) |
| 2 | **Loose Coupling** | schema is the only contract between services | [→ details](#2-loose-coupling) |
| 3 | **Event Immutability** | append-only facts; never mutate, only amend | [→ details](#3-event-immutability) |
| 4 | **Single Responsibility** | one event = one business fact, composable | [→ details](#4-single-responsibility) |
| 5 | **Eventual Consistency** | not immediate, but bounded and predictable | [→ details](#5-eventual-consistency) |
| 6 | **Event Ordering** | per-partition guarantee, NOT global | [→ details](#6-event-ordering--guarantees) |
| 7 | **Idempotent Processing** | same message twice → same result; dedup key | [→ details](#7-idempotent-processing) |
| 8 | **Observable Event Flow** | correlation ID on every event; trace everything | [→ details](#8-observable-event-flow) |
| 9 | **Backpressure Handling** | don't drop under load; queue, slow, or shed | [→ details](#9-backpressure-handling) |
| 10 | **Schema Versioning** | evolve events without breaking consumers | [→ details](#10-event-versioning--schema-evolution) |
| — | **Common Mistakes** | 10 anti-patterns with root cause and fix | [→ details](#common-mistakes--how-to-avoid-them) |
| — | **Decision Framework** | when to use EDA vs when to avoid it | [→ details](#decision-framework-when-to-use-event-driven-architecture) |
| — | **Interview Answer** | 60-second architect-level response | [→ details](#interview-tip) |

---

## Understanding the Mental Model

Before diving into individual principles, let's establish why event-driven design matters. In traditional architectures, Service A calls Service B which calls Service C. Each service waits for a response. This creates tight coupling — if B changes its API, A breaks. It also creates cascading failures — if C is slow, A waits, and your system degrades.

In event-driven systems, Service A publishes an *event* saying "something happened." Service B and C independently subscribe to that event and react asynchronously. Neither B nor C knows A exists. A doesn't know what they do with the event. This decoupling is the core insight that makes event-driven architecture powerful.

---

## Communication Principles

### 1. Asynchronous Communication

**What It Means**

Components communicate through events without waiting for immediate responses. A producer publishes an event and continues; consumers process that event whenever they're ready.

**Why It Matters**

Synchronous communication creates a chain of waiting. If you're waiting for a response, you hold resources. Threads, connections, memory — all waiting. In a microservices environment with dozens of services, synchronous chains become bottlenecks and failure amplifiers. One slow service cascades latency to the entire system.

Asynchronous communication decouples timing. A producer fires an event in milliseconds. Consumers process whenever they have capacity. This is how real systems handle high load — think of a concert ticket sales spike. You don't process each ticket synchronously; you queue them and process asynchronously.

**Real-World Analogy**

Email vs. a phone call. When you email, you send the message and don't wait for a reply. The recipient reads it eventually. With a phone call, you both must be available simultaneously. Email scales; phone calls don't.

**Concrete System Example**

In an e-commerce platform: when a customer places an order, the Order Service publishes an `OrderPlaced` event. The Payment Service, Inventory Service, Notification Service, and Analytics Service each independently subscribe to this event and react asynchronously — debiting the card, reserving stock, sending a confirmation email, and logging the sale. None of them blocks. None of them know about the others. The entire system remains responsive even if inventory updates take 5 seconds.

**What Breaks Without It**

Without asynchronous communication, the Order Service must synchronously call Payment → Inventory → Notification. If Notification Service is slow or down, the entire order fails. Load spikes compound — high order volume means high API call volume, which exhausts connection pools and cascades failures upstream.

---

### 2. Loose Coupling

**What It Means**

Event producers don't need to know about consumers; only the event contract (schema) matters. A service publishes events without caring who listens. A service consumes events without caring who published them.

**Why It Matters**

Tight coupling is the enemy of scale and evolution. If Service A must know about Services B, C, and D, then adding Service E requires changing A. Changes ripple. Teams can't move independently. Deployments become coordinated nightmares.

Loose coupling means each team owns their service end-to-end. B doesn't need to wait for A's team to add a new event format. B can start consuming an event that A publishes, and they need only agree on the event schema — not implementation details.

**Real-World Analogy**

News outlets publish stories; readers subscribe to topics. The New York Times doesn't know who's reading, and readers don't know how the Times works internally. What matters is the story (the contract). This scales to millions of readers without the Times changing anything.

**Concrete System Example**

In a user authentication system, the Auth Service publishes a `UserRegistered` event. Years later, the Product team decides to add referral tracking, so the Referral Service starts consuming that event. No changes to Auth. No redeployment. Referral wires up independently.

**What Breaks Without It**

Without loose coupling, adding a new service requires code changes across multiple existing services. Every integration point becomes a deployment dependency. Large organizations with many teams can't ship independently. Coupling also makes the system fragile — if B goes down, does A know to retry or ignore? If the contract changes, which services break?

---

## Data Principles

### 3. Event Immutability

**What It Means**

Events are immutable facts about what happened. Once published, they can't be changed or deleted. They form an append-only log.

**Why It Matters**

Immutability creates an audit trail. If you need to debug why a customer was charged twice, you have a perfect record: here's the charge event, the refund event, the reason. You can replay events to understand system state at any point in time. You can also run multiple independent analyses on the same event stream without worrying about data being mutated beneath you.

Mutability breaks this. If an event can be changed after publication, replay produces different results. Consumers might have already processed the old version. State becomes unpredictable.

**Real-World Analogy**

Historical records. Once a ledger entry is written in ink, it's immutable. You can't erase 1975's tax return. You can add an amendment, but the original fact remains. This is why accounting uses this model — it maintains integrity and auditability.

**Concrete System Example**

When a payment is processed, the `PaymentProcessed` event is published with an immutable timestamp, amount, and transaction ID. If there's a dispute, you don't change the original event. You publish a `PaymentDisputed` event that references it. The entire history is preserved.

**What Breaks Without It**

Mutable events mean there's no single source of truth. If you change an event, which version did consumers see? Did they already process the old one? If they process again, do they use the new version? Debugging becomes impossible — you can't replay to verify behavior.

---

### 4. Single Responsibility

**What It Means**

Each event represents one atomic business fact or state change, not a collection of loosely related changes.

**Why It Matters**

An event should answer the question: "What happened?" with a single, clear answer. If an event contains multiple facts, consumers are forced to handle all of them even if they care about only one. This couples consumers to information they don't need.

Single-fact events are composable. You can publish multiple events if multiple facts occurred. Consumers subscribe to exactly what they need. This is the event-driven equivalent of the Single Responsibility Principle.

**Real-World Analogy**

A restaurant receipt. It shows one transaction: items purchased, total paid. It doesn't mix multiple orders on one receipt. If you want to track the purchase, you know exactly what happened. If you wanted to track every item, individual order, and delivery separately, a mixed receipt would be confusing.

**Concrete System Example**

In a booking system, publish separate events: `RoomReserved`, `PaymentCaptured`, `ConfirmationEmailSent`. Don't publish a single `BookingCompleted` event that contains room details, payment info, and email template. Why? The Accounting team cares only about `PaymentCaptured`. The Inventory team cares only about `RoomReserved`. The Notification team cares only about `ConfirmationEmailSent`. Three focused consumers beat one bloated event.

**What Breaks Without It**

Multi-fact events bloat. Consumers process information they don't need, increasing latency. Filtering becomes implicit — "ignore that field." Schemas become unwieldy. When you need to change the email template, you have to worry about breaking downstream systems that process payment data from the same event.

---

## Consistency Principles

### 5. Eventual Consistency

**What It Means**

Systems don't require immediate synchronization across all services. Consistency is reached over time as events propagate and are processed.

**Why It Matters**

Strong consistency (immediate synchronization) requires distributed consensus, which is expensive and slow. Eventual consistency trades immediate consistency for availability and responsiveness. After an order is placed, the inventory system eventually reflects the new stock level. After a user updates their profile, that update eventually propagates to all read replicas.

This is pragmatic. In practice, systems tolerate some inconsistency for brief windows. A customer's address update takes 100ms to propagate? Fine. But immediate consistency across 10 services? That's expensive and often unnecessary.

**Real-World Analogy**

Bank tellers updating ledgers. Each teller updates their ledger immediately (local consistency). Throughout the day, ledgers are slightly inconsistent — one branch shows a balance that differs from another. At end-of-day, a batch process reconciles everything. The system is eventually consistent, but remains responsive all day.

**Concrete System Example**

When a customer is granted premium status, the User Service publishes `CustomerUpgradedToPremium`. The Analytics Service, Recommendation Service, and Billing Service all consume this event asynchronously. For a moment, Analytics might show the old status, but within 500ms, all services reflect the upgrade. The system is responsive immediately, and consistent within a bounded time window.

**What Breaks Without It**

Requiring strong consistency means waiting for synchronous replication. Every write stalls until all replicas acknowledge. This kills throughput and latency. In a distributed system with network partitions, strong consistency can cause complete unavailability (you choose consistency over availability, per CAP theorem). Most systems need responsiveness more than they need guaranteed instantaneous consistency.

---

### 6. Event Ordering & Guarantees

**What It Means**

Establish clear contracts for message ordering. Most systems guarantee ordering *within a partition or stream* but not globally. Delivery semantics also matter: at-most-once, at-least-once, exactly-once.

**Why It Matters**

Without ordering guarantees, causality breaks. If `OrderCreated` must arrive before `OrderShipped`, global disorder destroys the sequence. However, requiring total global ordering across all events is impractical — it's a bottleneck.

The pragmatic solution: partition events by a key (e.g., customer ID). All events for customer X arrive in order within their partition. Events across different customers may be out of order globally, but that's fine — they're independent.

Similarly, delivery semantics matter. At-least-once means an event might be delivered twice — your system must be idempotent. At-most-once means an event might be lost — acceptable for analytics, not for payments.

**Real-World Analogy**

An assembly line. Parts for one car arrive in sequence at each workstation. Different cars' parts may arrive out of global order (car B's bumper might arrive before car A's door), but that's fine because they're independent. What matters is that car A's parts arrive in the right sequence *for car A*.

**Concrete System Example**

In an inventory system, use Customer ID as the partition key. Customer A's events (`InventoryReserved` → `OrderPlaced` → `InventoryDeducted`) arrive in order for customer A. Customer B's events arrive in order for customer B. They can interleave globally without breaking causality. If you used no partitioning, you might process `InventoryDeducted` before `InventoryReserved`, causing negative stock.

**What Breaks Without It**

Without ordering, state machines break. A user's lifecycle might process `AccountDeleted` before `AccountCreated`. Payment might deduct funds before charging the card. Systems that assume order break silently and unpredictably. Without delivery semantics, you don't know if an event was lost (do we retry?) or duplicated (do we deduplicate?).

---

## Resilience Principles

### 7. Idempotent Processing

**What It Means**

Consuming the same event multiple times should produce the same result. The handler is safe to invoke repeatedly without side effects.

**Why It Matters**

The network fails. Brokers crash. Consumers crash after processing an event but before acknowledging it. In distributed systems, retries are necessary. But retries mean an event might be processed twice.

If your handler isn't idempotent, processing twice means double charges, duplicate records, or conflicting state. Idempotent handlers are safe — processing the same event 10 times produces the same outcome as processing it once.

This is essential. It's not optional.

**Real-World Analogy**

Paying a bill. You should only pay once, even if you accidentally submit twice. The billing system should detect duplicates (e.g., same payment ID) and not double-charge. Or, if you pay an exact amount and that exact amount has already been paid, the second attempt is safely ignored.

**Concrete System Example**

When `OrderPlaced` is processed, don't just increment a counter. Use the order ID as a unique key. Store a mapping: "order 12345 has been processed." If the event arrives again, check the mapping first. If already processed, skip. If not, process and record.

```sql
INSERT INTO processed_events (event_id, processed_at)
VALUES ('order-12345', NOW())
ON DUPLICATE KEY UPDATE processed_at = NOW();
```

**What Breaks Without It**

Without idempotency, retries cause data corruption. Charging a customer twice. Creating duplicate records. Incrementing counters by 2 when only 1 event occurred. The system appears to work fine until a crash triggers a retry, and suddenly data is wrong. These bugs are insidious — they show up unpredictably during failures.

---

### 8. Observable Event Flow

**What It Means**

Events and their transformations must be traceable and debuggable. You can follow an event from publication to all downstream processing.

**Why It Matters**

Distributed systems are hard to debug. A customer reports "my order didn't arrive." Where did it get stuck? Event sourcing and event IDs enable end-to-end tracing.

Every event should have a trace ID that flows through all consuming services. Logs, metrics, and traces should capture event flow. You should be able to ask: "What happened to event X?" and get a complete story.

**Real-World Analogy**

A flight manifest showing every passenger check-in and movement. You can track passenger X from ticket purchase through boarding through arrival. If something goes wrong, you have the full history.

**Concrete System Example**

Assign each event a unique `correlation_id`. When Order Service publishes `OrderPlaced`, it includes a `correlation_id`. Every downstream service adds their own span ID but keeps the `correlation_id`. In your observability platform, search by `correlation_id` and see: Order → Payment → Inventory → Notification, all connected.

```json
{
  "event_id": "evt_12345",
  "correlation_id": "corr_98765",
  "event_type": "OrderPlaced",
  "timestamp": "2026-04-03T10:30:00Z",
  "data": { "order_id": "order_456" }
}
```

**What Breaks Without It**

Without observability, debugging a distributed failure is guesswork. An event disappears and you don't know why. Did it get dropped? Is the consumer down? Is it stuck in a queue? There's no trail to follow. You end up adding ad-hoc logging, which is expensive and incomplete.

---

### 9. Backpressure Handling

**What It Means**

Systems gracefully handle scenarios where event producers outpace consumers. Rather than dropping events or crashing, the system applies backpressure — slowing producers or buffering events.

**Why It Matters**

Load is uneven. One minute you have 10 orders/sec; the next, 1000. Without backpressure, one of two things happens: events are dropped (data loss), or the consumer gets overwhelmed and crashes.

Backpressure ensures that if a consumer is slow, the producer knows to slow down or queue. This is how the system self-regulates under load.

**Real-World Analogy**

A ticket booth queue. If sales are too fast and the booth gets overwhelmed, customers wait in line (backpressure). The booth doesn't lose customers or crash; it processes them at its own pace. The queue grows temporarily but remains stable.

**Concrete System Example**

A message broker like Kafka applies backpressure through consumer groups. If a consumer falls behind, it has a lag. The broker doesn't drop messages. The consumer catches up when it has capacity. If you add more consumers, throughput increases. This is self-regulating.

```python
# Consumer applies backpressure by pausing polling
while True:
    messages = consumer.poll(timeout_ms=1000)  # Pause if behind
    if len(messages) > 0:
        process(messages)
```

**What Breaks Without It**

Without backpressure, high-load scenarios cause cascading failures. A spike in event production overwhelms consumers. They crash or drop events. Data is lost. The system becomes unreliable under exactly the conditions where reliability matters most.

---

## Evolution Principles

### 10. Event Versioning & Schema Evolution

**What It Means**

Events must support schema changes without breaking existing consumers. Old versions and new versions must coexist peacefully.

**Why It Matters**

Systems evolve. You add fields to events, remove obsolete ones, or change data types. But old consumers might still be deployed and expecting the old schema. New consumers expect the new schema. You need both to work simultaneously.

Schema versioning lets you update producers and consumers independently. Consumers are updated first (to accept new fields), then producers update (to send new fields). Or producers update first (keeping both old and new fields), then consumers catch up.

**Real-World Analogy**

A restaurant menu that evolves. Old customers understand the core items (burger, salad). When you add a new item (vegan option), old customers can ignore it. They still order what they know. New customers use the new items. The menu is backward compatible.

**Concrete System Example**

Version 1 of `UserRegistered`:
```json
{ "event_version": 1, "user_id": "123", "email": "user@example.com" }
```

Later, you want to track phone numbers. Version 2:
```json
{
  "event_version": 2,
  "user_id": "123",
  "email": "user@example.com",
  "phone": "+1234567890"
}
```

Old consumers ignore the `phone` field. New consumers use it. Producers send both. No breaking changes.

**What Breaks Without It**

Without versioning, adding a field breaks old consumers that don't expect it. Removing a field breaks new consumers that need it. You're forced to coordinate updates across many teams, which is slow and error-prone. Evolution stalls.

---

## Common Mistakes & How to Avoid Them

### The Choreography Spiral

**The Story**

Teams often adopt event choreography — services reacting to events and publishing new events in a chain — because it feels simple and decoupled. Service A publishes `OrderCreated` → Service B publishes `PaymentProcessed` → Service C publishes `OrderFulfilled`. No central orchestrator. What could go wrong?

**What Actually Happens**

As the system grows to 5+ services, choreography becomes a distributed state machine that's impossible to reason about. A bug in Service B breaks the entire flow. Compensation logic (what if payment fails?) requires reversing events in C, which publishes a new event that triggers B, which cascades everywhere. Debugging a failure means tracing through 7 different service logs. The apparent simplicity becomes operational chaos.

**The Fix**

For workflows involving more than 3–4 services, add an explicit orchestrator. A single service (Workflow Service, Saga Orchestrator, or state machine) owns the flow. It publishes commands and listens for events. Choreography still happens, but it's coordinated. The trade-off: a central point of coordination, but vastly better observability and debuggability.

---

### Storing Logic in Events

**The Story**

Teams encode business logic in event payloads: "include the user's permissions so the consumer can decide what to do." Or: "include the calculation logic so the consumer applies it correctly." This feels efficient — all context in one place.

**What Actually Happens**

Logic in events becomes frozen in time. Once that event is published, you can't change the logic without breaking replay. You also duplicate logic across consumers — each implements the same rules differently. If permissions change, you have no way to re-evaluate old events under new rules. State becomes inconsistent.

**The Fix**

Events are data: immutable facts about what happened. Handlers are logic: the rules for reacting to those facts. Separate them. Publish `OrderPlaced` with an order ID. The handler looks up the order, applies current business rules, and acts. If rules change, replay works correctly because handlers are fresh.

```python
# WRONG: Logic in event
event = {"order_id": 123, "discount_rules": ["prom_code_ABC", "loyalty_5pct"]}

# RIGHT: Logic in handler
event = {"order_id": 123}
def handle(event):
    order = load_order(event["order_id"])
    rules = load_current_discount_rules()
    apply_rules(order, rules)
```

---

### Ignoring Partition Ordering

**The Story**

"We're using Kafka, so messages are ordered, right?" Teams assume global ordering without setting up partition keys. An `OrderPlaced` event and an `OrderShipped` event for the same customer arrive out of order because they're in different partitions.

**What Actually Happens**

State becomes inconsistent. A customer is marked as shipped before they're placed in the system. Inventory is deducted twice because the same event is processed from two different partitions. The system works fine in development (single partition) and fails sporadically in production (multiple partitions).

**The Fix**

Always use a partition key. For orders, use customer ID or order ID. For inventory, use SKU. Partition key ensures related events stay in order within a consumer group.

```python
# WRONG
producer.send("orders", value=order_event)

# RIGHT
producer.send("orders", value=order_event, key=order.customer_id)
```

---

### Lost Events in Failures

**The Story**

A consumer processes an event, then crashes before acknowledging it. On restart, the event is redelivered. But if you haven't implemented idempotency and acknowledgment, that event might be processed, or lost, depending on timing.

**What Actually Happens**

Events disappear silently. A payment is deducted once. A notification is sent but not tracked. Data integrity degrades. You discover weeks later that some orders are missing from analytics.

**The Fix**

Implement idempotent handlers (principle 7) and explicit acknowledgment. Process the event, store idempotency information (event ID or deduplication key), then acknowledge. If the process crashes, the event will be redelivered and safely re-processed.

---

### Event Payload Bloat

**The Story**

To avoid lookups, teams include everything in the event: user details, permissions, full product catalog, derived fields. "It's more efficient than multiple queries," they say.

**What Actually Happens**

Event payload grows to kilobytes. Storage costs explode. Network transfer becomes a bottleneck. Consumers that care about one field pay the price of receiving 100 fields. If user details change, old events have stale data. Replaying old events with new consumers breaks because the schema has drifted.

**The Fix**

Keep events minimal. Include IDs and essential context. Let consumers look up full data if needed.

```json
// WRONG: Bloated event
{
  "event_type": "UserRegistered",
  "user_id": "123",
  "full_name": "John Doe",
  "email": "john@example.com",
  "phone": "+1234567890",
  "address": "...",
  "company": "...",
  "permissions": ["read", "write", "admin"],
  "profile_pic": "base64_encoded_image..."
}

// RIGHT: Minimal event
{
  "event_type": "UserRegistered",
  "user_id": "123",
  "timestamp": "2026-04-03T10:30:00Z"
}
```

---

### No Dead Letter Queue

**The Story**

A consumer fails to process an event. The broker retries, but the consumer crashes every time. The event gets stuck or dropped. "We'll handle it manually" — but you don't.

**What Actually Happens**

Critical events disappear. A refund request fails to process, and the customer is never contacted. A data sync event fails, and two systems become permanently out of sync. Days later, the impact is discovered.

**The Fix**

Implement a Dead Letter Queue (DLQ). Events that fail processing after N retries go to a DLQ. A separate process or team monitors the DLQ, investigates, and replays when fixed. This separates the happy path from the exception path.

```python
try:
    process(event)
    consumer.commit()
except Exception as e:
    logger.error(f"Failed to process {event}", e)
    dlq.send(event)
    consumer.commit()
```

---

### Circular Event Dependencies

**The Story**

Service A publishes `OrderPlaced` → Service B publishes `PaymentProcessed` → Service A reacts with `InventoryDeducted` → Service B reacts again. A circular loop emerges.

**What Actually Happens**

Events cascade indefinitely. A single order causes an avalanche of events that trigger each other. The system becomes unstable. Event counts spiral. Processing becomes impossible to track.

**The Fix**

Model the workflow explicitly. Use a Saga orchestrator that owns the sequence. Or add idempotency keys and circuit breakers to prevent loops.

```python
# WRONG: Circular dependency
A publishes OrderPlaced
B listens, publishes PaymentProcessed
A listens, publishes InventoryDeducted
B listens, publishes OrderConfirmed
... cycle continues

# RIGHT: Explicit orchestration
Orchestrator:
  1. Publishes OrderPlaced
  2. Waits for PaymentProcessed
  3. Publishes InventoryDeduction
  4. Waits for InventoryDeducted
  5. Publishes OrderConfirmed
```

---

### Temporal Coupling Through Timestamps

**The Story**

"Let's use timestamps for ordering. Event with the later timestamp is the latest state." Teams rely on wall-clock time to establish causality.

**What Actually Happens**

Clock skew causes inversions. Service A publishes event at 10:00:01, Service B publishes at 10:00:00 (clock slightly behind). B's event has an earlier timestamp but represents a later fact. Consumers see them in the wrong order. State becomes inconsistent.

**The Fix**

Use monotonic event IDs or sequence numbers assigned by a central authority (like a broker). These aren't affected by clock skew.

```json
// WRONG
{ "event_type": "OrderPlaced", "timestamp": "2026-04-03T10:00:01Z" }

// RIGHT
{ "event_type": "OrderPlaced", "sequence_number": 12345, "timestamp": "2026-04-03T10:00:01Z" }
```

---

### Schema Assumptions in Consumers

**The Story**

A consumer hardcodes expectations: "the event always has a customer_id field." When the producer adds an optional billing_id field and removes customer_id in favor of account_id, the consumer breaks.

**What Actually Happens**

Version mismatches cause silent failures or crashes. A consumer that expects customer_id but gets account_id silently treats it as missing. Logic breaks. Or the consumer crashes on a NullPointerException. Deployments must be perfectly coordinated.

**The Fix**

Use defensive parsing and schema validation at boundaries. Handle missing fields gracefully. Validate against a schema during deserialization.

```python
# WRONG
customer_id = event["customer_id"]  # Crashes if missing

# RIGHT
customer_id = event.get("customer_id")  # Defaults to None if missing
if not customer_id:
    customer_id = event.get("account_id")
validate_schema(event, "OrderPlaced_v2")
```

---

### No Event Retention Policy

**The Story**

"Keep all events. They're facts." Teams never delete. Years of events accumulate.

**What Actually Happens**

Storage costs explode. Replication becomes expensive. Backups take hours. Compliance violations (GDPR requires deletion of personal data). Performance degrades.

**The Fix**

Define a retention policy. Keep events for 90 days? 1 year? Or archive to cold storage and delete hot storage. Balance compliance, cost, and replay scenarios.

```
Event Retention Policy:
- Hot storage (Kafka): 30 days, 3 replicas
- Warm storage (S3): 1 year, single region
- Cold storage (Glacier): 7 years, compliance archive
- Delete: after 10 years
```

---

## Common Mistakes at a Glance

| Mistake | Root Cause | Impact | Fix |
|---------|-----------|--------|-----|
| **Event Choreography Spiral** | Assuming no central coordination needed | Untrackable workflows, cascading failures | Add explicit orchestrator for 4+ services |
| **Logic in Events** | Trying to avoid lookups | Frozen logic, inconsistent state, replay breaks | Separate data (events) from rules (handlers) |
| **Ignoring Partition Ordering** | Assuming global ordering | Out-of-order state changes, data corruption | Use consistent partition keys (customer ID, order ID) |
| **Lost Events in Failures** | No idempotency or acknowledgment | Silent data loss | Implement idempotent handlers + explicit acks |
| **Event Payload Bloat** | Including all context to avoid lookups | Storage overhead, network bottlenecks, stale data | Keep events minimal; let consumers look up details |
| **No Dead Letter Queue** | Ignoring failure scenarios | Critical events disappear | Implement DLQ, monitoring, and manual recovery |
| **Circular Event Dependencies** | Allowing cascading reactions | Event avalanche, unstable system | Use orchestrator to break loops |
| **Timestamp-Based Ordering** | Relying on wall-clock time | Clock skew causes causality inversions | Use monotonic sequence numbers instead |
| **Hardcoded Schema Assumptions** | Tight coupling to event structure | Breaks on schema evolution | Use defensive parsing and validation |
| **No Retention Policy** | Keeping events forever | Cost explosion, compliance violations | Archive old events, define TTL |

---

## Decision Framework: When to Use Event-Driven Architecture

**Use event-driven architecture when:**

- Multiple services must react to the same business event independently
- You need high throughput and low latency for asynchronous operations
- Services must deploy independently without tight coordination
- You value an immutable audit trail or event replay capability
- Real-time analytics or event streaming is part of the requirement
- Data consistency across services can be eventual (within bounded time)

**Avoid event-driven architecture when:**

- The system is simple CRUD with tight coupling acceptable (a monolith might be fine)
- You require strong consistency across all services immediately
- Your team lacks experience debugging distributed systems (operational overhead is real)
- The bulk of traffic is synchronous request-response with little async work
- Compliance requires immediate consistency or precludes eventual consistency models

---

## Interview Tip

> When asked about event-driven architecture in a senior-level interview, demonstrate depth of thinking, not just recitation of concepts.
>
> **Opening (Show the "why"):**
> "Event-driven design is fundamentally about decoupling systems through asynchronous, immutable facts. Instead of Service A calling Service B which calls Service C — creating a chain of dependencies — we have Service A publish an event saying 'something happened,' and B and C independently react. This removes the coupling, which is the core problem it solves."
>
> **Core Principles (Show your judgment):**
> "Three principles I focus on: First, loose coupling — consumers don't know about producers. Second, idempotent processing — the network fails and messages are redelivered, so handlers must be safe to invoke multiple times. Third, observable event flow — tracing is critical in distributed systems; every event needs a correlation ID."
>
> **Common Pitfalls (Show experience):**
> "The biggest anti-pattern I see is treating events like function calls — waiting for responses, storing logic in payloads. Events are facts, not commands. Another trap: assuming global ordering when Kafka guarantees it only per partition. You must use a consistent partition key (customer ID, order ID) or events arrive out of order and state becomes corrupt."
>
> **Practical Concern (Show rigor):**
> "If the system involves 5+ services coordinating work, I'd move away from pure choreography (services reacting to each other) and add an explicit orchestrator. Choreography *feels* simple but becomes a nightmare to debug and maintain. Orchestration is more transparent and lets you model the entire workflow in one place."
>
> **Concrete Pattern (Show you've built this):**
> "For guaranteeing events aren't lost, I implement the Outbox Pattern: write the event to our local database in the same transaction as the state change, then a separate process publishes to the broker. This gives us exactly-once semantics at the application level — no events lost, no duplicates (if the consumer is idempotent)."
>
> You've now shown: understanding of fundamentals, judgment on when to apply patterns, awareness of real failure modes, and a concrete implementation technique. That's architect-level thinking.

---
