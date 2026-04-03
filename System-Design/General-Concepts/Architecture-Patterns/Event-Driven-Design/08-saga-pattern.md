# Saga Pattern

Distributed transaction pattern for coordinating long-running processes across multiple services without relying on 2-phase commit.

→ Back to [Event-Driven Design](./README.md)

---

## What SAGA Solves

| Problem | Traditional Approach | Why It Fails | SAGA Solution |
|---------|---------------------|-------------|---------------|
| Distributed transactions across services | 2-Phase Commit (2PC) | Creates tight coupling, poor availability, locks resources long-term | Breaks transaction into local ACID transactions + compensation logic |
| Long-running business processes | Single monolith | Doesn't scale; violates microservice boundaries | Coordinates async steps across services |
| Rollback in distributed systems | Transaction rollback across DBs | Network partitions make 2PC unreliable | Backward compensation (e.g., cancel instead of rollback) |
| Consistency guarantees | Immediate strong consistency | Impossible across services | Eventual consistency with compensating transactions |

---

## Choreography Approach

**How it works:**
- Each service listens to domain events and emits new events
- No central coordinator; services directly trigger each other via event chain
- Each step publishes success/failure events that trigger next step

**Flow Example (Order → Payment → Inventory → Shipping):**

```
  1. OrderService publishes "OrderCreated"
           ↓
  2. PaymentService listens, processes payment → "PaymentProcessed"
           ↓
  3. InventoryService listens, reserves stock → "StockReserved"
           ↓
  4. ShippingService listens, schedules delivery → "DeliveryScheduled"
           ↓
  ✓ Transaction complete (all local ACID txns committed)
```

**Failure compensation (Payment fails):**

```
  1. PaymentService → "PaymentFailed"
           ↓
  2. OrderService listens → "OrderCancelled"
           ↓
  3. InventoryService listens → "StockReleased" (compensating transaction)
           ↓
  ✗ Saga rolled back
```

**Pros:**
- Simple, decoupled architecture
- No single point of failure (no orchestrator)
- Low latency (no coordinator bottleneck)
- Easy to add new steps (just emit events)

**Cons:**
- Hard to understand flow (distributed across services)
- Difficult to debug (circular event dependencies)
- Testing is complex (need to mock other services)
- Eventual consistency requires careful handling of duplicate events
- If Service B doesn't emit expected event, saga stalls with no visibility

**When to use:**
- Simple workflows with clear, linear steps
- Low coupling preferred over visibility
- Most services are highly available
- Small number of participating services (< 5)

---

## Orchestration Approach

**How it works:**
- Central orchestrator (separate service or state machine) controls workflow
- Orchestrator tells each service what to do and waits for response
- Orchestrator decides next step based on responses

**Flow Example (Order → Payment → Inventory → Shipping):**

```
  Orchestrator (Saga Engine)
        ↓
   Send command: "ProcessPayment"
        ↓
  PaymentService replies: "PaymentOK"
        ↓
   Send command: "ReserveStock"
        ↓
  InventoryService replies: "StockOK"
        ↓
   Send command: "ScheduleShipping"
        ↓
  ShippingService replies: "DeliveryOK"
        ↓
  ✓ Saga complete
```

**Failure compensation (Stock unavailable):**

```
  Orchestrator receives: "StockFailed"
        ↓
   Send command: "ReleasePayment" (compensating transaction)
        ↓
  PaymentService replies: "PaymentReleased"
        ↓
  ✗ Saga rolled back
```

**Pros:**
- Clear, auditable workflow (all logic in one place)
- Easy to understand and debug saga flow
- Centralized retry logic and error handling
- Can enforce strict ordering and conditional branches
- Good visibility into process status

**Cons:**
- Orchestrator becomes single point of failure
- Higher operational complexity (one more service to manage)
- Potential bottleneck if orchestrator receives all requests
- Tighter coupling (orchestrator knows about all services)
- Need to handle orchestrator state persistence

**When to use:**
- Complex workflows with branching logic
- Need clear audit trail and visibility
- Multiple failure scenarios and compensation paths
- Large number of participating services (5+)
- Strong consistency requirements within saga

---

## Comparison Table

| Aspect | Choreography | Orchestration |
|--------|--------------|---------------|
| **Control Flow** | Distributed event chain | Centralized orchestrator |
| **Coupling** | Loosely coupled | Tightly coupled to orchestrator |
| **Visibility** | Poor (events scattered) | Excellent (single source of truth) |
| **Testing** | Hard (mock all services) | Easy (mock orchestrator) |
| **Scaling** | Linear (add service = add handler) | Potential bottleneck (orchestrator) |
| **Failure Recovery** | Implicit (via events) | Explicit (orchestrator manages) |
| **Debugging** | Difficult (distributed traces) | Simple (orchestrator logs) |
| **Persistence** | Event log | Saga state + steps |
| **Complexity** | Low setup, high cognitive load | High setup, low cognitive load |
| **Best For** | Simple, linear processes | Complex, branching workflows |

---

## Failure Handling & Compensating Transactions

**Core Concept:**
- SAGA cannot roll back like a traditional transaction (no centralized log)
- Instead, each step must define its own "undo" operation
- If step N fails, execute compensating transactions for steps 1..N-1 in **reverse order**

**Examples of Compensating Transactions:**

| Forward Transaction | Compensating Transaction | Notes |
|-------------------|------------------------|-------|
| Reserve stock | Release stock | Idempotent (safe to call twice) |
| Charge payment | Refund payment | Must track refund ID to avoid double-refunds |
| Book flight | Cancel booking | Must check cancellation policy |
| Send email | [None] | Some actions can't be undone; handle in retry or manual review |
| Create user account | Deactivate user | Better than delete (preserves audit trail) |

**Execution Order on Failure:**

```
Forward steps executed: 1 → 2 → 3 → 4 (fails)

Compensation executed in reverse: 4_compensate → 3_compensate → 2_compensate → 1_compensate
(or partial: 3_compensate → 2_compensate → 1_compensate if 4 had no side effects)
```

**Idempotency is Critical:**
- Network failures may cause duplicate messages
- Compensating transaction might execute twice
- Always use idempotency keys: `compensation_txn_id = hash(saga_id + step_name)`
- Check before executing: "Have I already refunded this payment?"

**Handling Unsure States:**
- If orchestrator crashes mid-saga, restart and replay from last known state
- Use saga ID + step counter to detect what was already done
- Implement timeout logic: if a service doesn't respond in 10s, trigger compensation

---

## Key Patterns for Production

> [!NOTE]
> **Event Sourcing + SAGA:** Store all events in append-only log. On orchestrator crash, replay events to determine current state.

> [!WARNING]
> **Idempotency Keys:** Every compensating transaction must include unique key. Services must check "have I already done this?" before executing.

> [!TIP]
> **Timeout & Deadletter Queues:** If service doesn't respond in N seconds, move to deadletter. Manual review determines if retry or compensation.

---

## Interview Tip

**For Senior/Principal-level interviews, have this ready:**

> "SAGA pattern solves distributed transactions without 2PC's availability and performance costs. There are two styles:
>
> **Choreography** is event-driven—services listen and react, triggering the next step. It's loosely coupled but hard to debug because flow is spread across services. It works well for simple, linear workflows.
>
> **Orchestration** uses a central service that commands each step and waits for responses. It's easier to understand and debug, but the orchestrator becomes a coordination point. Better for complex workflows with branching logic.
>
> The key difference: choreography is *pull*-based (I'll do my part and signal others), orchestration is *push*-based (orchestrator tells me what to do).
>
> For failure handling, each step defines a *compensating transaction*—an undo operation. If step 4 fails, we run compensations 3→2→1 in reverse. The tricky part is idempotency: what if the network duplicates a refund message? You must track which operations already completed using an idempotency key.
>
> I'd use orchestration for order-to-shipping workflows (complex, many failure paths) and choreography for audit logs (simple: publish event → multiple services consume). The rule of thumb: if you need to explain the flow in a diagram, use orchestration."

---

## Related Patterns

- **Event Sourcing:** Store state as immutable event sequence (pairs well with SAGA)
- **Compensating Transactions:** Explicit undo operations for each step
- **Idempotency Keys:** Prevent duplicate side effects on network retries
- **Outbox Pattern:** Ensure event is published even if message broker is down
- **Dead Letter Queue:** Hold failed messages for manual review
