# Event-Driven Design: Core Principles

Foundational principles for building scalable, decoupled systems using event-driven architecture patterns.

→ Back to [Event-Driven Design](./README.md)

---

## Core Principles

| # | Principle | Description | Real-World Analogy |
|---|-----------|-------------|-------------------|
| 1 | **Asynchronous Communication** | Components communicate through events without waiting for responses | Email instead of a phone call—sender doesn't wait for immediate reply |
| 2 | **Loose Coupling** | Event producers don't need to know about consumers; only the event contract matters | News outlets publish stories; readers subscribe to topics, not authors |
| 3 | **Event Immutability** | Events are immutable facts about what happened; they cannot be changed retroactively | Historical records—once written, they become an audit trail |
| 4 | **Single Responsibility** | Each event should represent one atomic business fact or state change | A restaurant receipt shows one transaction; you don't mix multiple orders |
| 5 | **Eventual Consistency** | Systems don't require immediate synchronization; consistency is reached over time | Bank tellers update ledgers throughout the day; final reconciliation happens later |
| 6 | **Event Ordering & Guarantees** | Establish clear contracts for message ordering (per partition/stream) and delivery semantics | Assembly line: parts must arrive in sequence for each workstation |
| 7 | **Idempotent Processing** | Consuming the same event multiple times should produce the same result | Paying a bill—paying it twice should detect duplicates and not double-charge |
| 8 | **Observable Event Flow** | Events and their transformations must be traceable and debuggable | Flight manifest showing every passenger check-in and movement |
| 9 | **Backpressure Handling** | Systems gracefully handle scenarios where event producers outpace consumers | Queue at a ticket booth—if sales are too fast, customers wait in line |
| 10 | **Event Versioning & Schema Evolution** | Events must support schema changes without breaking existing consumers | Restaurant menu updates—old customers still understand core items, new items added over time |

---

## Common Mistakes

| Mistake | Why It Happens | Fix |
|---------|----------------|-----|
| **Event Choreography Complexity** | Trying to coordinate 5+ services through events without clear ownership | Use Saga patterns or command processors; explicitly model workflows |
| **Storing Logic in Events** | Including business logic in event payloads instead of handlers | Events are data (what happened); handlers are logic (what to do). Separate them |
| **Ignoring Event Ordering** | Assuming global ordering when only partition-level guarantees exist | Use keyed partitions; ensure dependent events share same partition key |
| **Lost Events in Failures** | No acknowledgment or idempotency; events disappear on consumer crash | Implement idempotent handlers + acknowledgment before processing next event |
| **Event Payload Bloat** | Including every possible context in the event (user info, permissions, etc.) | Use event ID + lookups; keep payloads minimal; external references for large data |
| **No Dead Letter Queue** | Processing failures silently ignored or crashed consumers lose messages | Set up DLQ for failed events; implement retry logic with backoff |
| **Circular Event Dependencies** | Service A publishes event→Service B publishes event→Service A again | Model state machines explicitly; use circuit breakers or idempotency keys |
| **Tight Coupling via Timestamps** | Relying on exact timing; using wall-clock time for ordering guarantees | Use monotonic event IDs or sequence numbers; abstract away temporal coupling |
| **Schema Assumptions in Consumers** | Hardcoding expected fields; breaks when schema evolves | Use defensive parsing; version events; validate schemas at boundaries |
| **No Event Retention Policy** | Never deleting old events; storage costs explode | Define retention: TTL, archival strategy, and replay scenarios needed |

---

## Key Implementation Patterns

- **Event Sourcing**: Store every state change as an event; rebuild state by replaying events
- **CQRS (Command Query Responsibility Segregation)**: Separate write models (commands → events) from read models (event consumers → projections)
- **Saga Pattern**: Distributed transactions using event choreography or orchestration
- **Outbox Pattern**: Write event to local DB in same transaction; separate process publishes to message broker (prevents lost events)
- **Dead Letter Queue (DLQ)**: Capture events that fail processing for debugging and recovery

---

## Interview Tip

> **For Senior/Principal-Level Roles:**
>
> When asked about event-driven architecture in interviews, structure your answer like this:
>
> *"Event-driven design is fundamentally about decoupling systems through asynchronous messaging. The core idea is that services don't call each other directly—they publish immutable events that represent facts about what happened in their domain.*
>
> *Three critical principles I always focus on:*
> - *Loose coupling (consumers and producers are independent), which gives us flexibility to scale and evolve services*
> - *Idempotent processing, because the network will fail and messages may be delivered multiple times—we must handle that gracefully*
> - *Observable event flow, because distributed debugging is hard; every event should be traceable end-to-end*
>
> *The biggest anti-pattern I see is treating events like function calls—waiting for responses, storing logic in payloads. Events are facts, not commands. That distinction matters.*
>
> *If the system involves multiple services coordinating work, I'd clarify: are we doing event choreography (services reacting to each other) or event orchestration (a coordinator service)? Choreography scales better for loose coupling but can become hard to reason about beyond 4–5 services. Orchestration is more explicit but centralizes coordination logic.*
>
> *Concrete concern: How do we ensure no events are lost? I'd implement the Outbox Pattern—write the event to our local database in the same transaction as the state change, then a separate process publishes to the broker. This guarantees exactly-once semantics at the application level."*

---

## When to Use Event-Driven Architecture

✓ Multiple services must react to the same business fact
✓ System needs high scalability with loose coupling
✓ Real-time processing or analytics (streaming data)
✓ Audit trails and immutable event logs are valuable
✓ Services have independent deployment schedules

✗ Simple CRUD app with tight coupling acceptable
✗ Synchronous request-response is the primary pattern
✗ Operational complexity / debugging load is a concern
✗ Strong consistency requirements across all services
