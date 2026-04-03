# Saga Pattern

The distributed transaction architecture for coordinating long-running processes across multiple services without relying on 2-phase commit—and the hard-won lessons from production systems that tried both choreography and orchestration.

→ Back to [Event-Driven Design](./README.md)

---

## The Distributed Transaction Problem

Here's the gap that most engineers don't fully appreciate until they've shipped a real system:

In a monolith, a transaction is deceptively simple. You're in a single database. BEGIN, execute four operations atomically, COMMIT or ROLLBACK. If step three fails, everything reverts. Done.

In microservices, those same four operations now live in four different databases. There is no single BEGIN-COMMIT boundary. Each service has its own data store. You've traded away a single point of failure but gained something infinitely harder: distributed transactions across boundaries you don't directly control.

Two-Phase Commit (2PC) was the theoretical answer. Coordinator says "prepare" to all participants, collects votes, then says "commit" to all. If any votes no, the whole thing rolls back. Theoretically sound. Practically, it fails in production at scale:

1. **Availability costs.** If the coordinator dies, all transactions hang. If *any* participant can't be reached, the whole system blocks. In a geographically distributed system, this becomes a serious availability problem.

2. **Latency penalty.** 2PC requires synchronous coordination. That coordinator becomes a chokepoint. Every transaction pays the cost of synchronous round-trips to all participants. By the time you've added network latency, service processing time, and lock contention, you're looking at multi-second transaction times.

3. **Lock contention at scale.** Each step holds locks waiting for the coordinator. As transaction volume grows, lock hold times dominate. Deadlocks become endemic. I've seen systems grind to a halt under load because 2PC's locking model couldn't scale.

4. **Coupling amplification.** 2PC couples every service to every other service via the coordinator. You can't deploy one service without potentially affecting transaction semantics across the entire system.

This is why the Saga pattern exists: **Stop trying to distribute the ACID properties themselves. Instead, break the big transaction into a sequence of local transactions, each with its own undo operation.** Trade immediate consistency for eventual consistency, but keep the system available and responsive.

---

## The Core Saga Idea

A saga is a sequence of local transactions coordinated in one of two ways. Each step is a local ACID transaction within a single service. If a step fails, the saga coordinates compensation—a deliberate undo—in reverse order through all previous steps.

This is fundamentally different from a rollback. A rollback is automatic and instantaneous. A compensating transaction is explicit, can fail, and might be slow. That distinction matters.

There's no single "right way" to coordinate these steps. The two approaches—choreography and orchestration—represent different philosophical bets about how to manage complexity in distributed systems.

---

## Choreography: The Event-Driven Democracy

Choreography means: no central coordinator. Services communicate through events. When something meaningful happens, a service publishes an event. Other services listen. When they finish their work, they publish events that trigger the next step.

### The Happy Path: Order to Shipping

Picture an e-commerce platform. A customer places an order:

```
Customer places order
       ↓
OrderService: CREATE order record (local txn)
       ↓ publishes "OrderCreated"
       ↓
PaymentService: listening → CHARGE payment (local txn)
       ↓ publishes "PaymentProcessed"
       ↓
InventoryService: listening → RESERVE stock (local txn)
       ↓ publishes "StockReserved"
       ↓
ShippingService: listening → SCHEDULE delivery (local txn)
       ↓ publishes "DeliveryScheduled"
       ↓
✓ Order saga complete. Customer sees tracking number.
```

Each service owns its operation and its outcome. Nothing waits synchronously. The order flows through the system as pure events. There is no orchestrator, no central point that knows about the others.

### When Payment Fails: Compensation in Choreography

This is where things get real. Payment declines. Now the system must undo what it's already done:

```
OrderService: CREATE order
       ↓ publishes "OrderCreated"
       ↓
PaymentService: listening → CHARGE payment
       ↓ CHARGE FAILS (insufficient funds)
       ↓ publishes "PaymentFailed"
       ↓
InventoryService: listening → (waits, no StockReserved yet, so nothing to release)
       ↓
OrderService: listening to PaymentFailed → CANCEL order (local txn)
       ↓ publishes "OrderCancelled"
       ↓
ShippingService: listening → (never scheduled, no compensation needed)
       ↓
✗ Saga rolled back. Customer sees "Payment declined. Order cancelled."
```

Simple case. But now imagine:

```
OrderService: CREATE order
       ↓ publishes "OrderCreated"
       ↓
PaymentService: CHARGE payment → succeeds
       ↓ publishes "PaymentProcessed"
       ↓
InventoryService: RESERVE 10 units of product → succeeds
       ↓ publishes "StockReserved"
       ↓
ShippingService: SCHEDULE delivery to warehouse → FAILS (warehouse closed)
       ↓ publishes "DeliveryScheduleFailed"
       ↓
InventoryService: listening → RELEASE stock (compensating txn)
       ↓ publishes "StockReleased"
       ↓
PaymentService: listening → REFUND payment (compensating txn)
       ↓ publishes "PaymentRefunded"
       ↓
OrderService: listening → CANCEL order
       ↓
✗ Saga rolled back completely.
```

Notice the order of compensation: it flows **reverse** through the chain of what succeeded. Shipping failed first, so it needs nothing undone. Inventory succeeded and must be undone. Payment succeeded and must be undone. The system has to remember which steps actually completed.

### The Real-World Problem with Choreography

I've been in production war rooms where choreography broke down. Here's what happened:

We built a saga across six services. Order → Payment → Inventory → Shipping → Notification → Analytics. Each service published events. Things worked... until they didn't.

**The debugging nightmare:** A customer's order got stuck in limbo. We had no idea which service was supposed to do what next. Events were scattered across six service logs, six databases, six monitoring systems. The order had a payment, had stock reserved, had a shipping label—but no notification was sent. Which service was supposed to listen? Did the Notification service miss the event? Did it process it but fail to save the state? Is it retrying? We spent hours reconstructing the state from event logs because there was no central visibility.

**The coupling problem:** After six months, we added a new Fraud Detection service. It needed to listen to PaymentProcessed and decide whether to approve the payment. But if it rejected, it had to trigger compensation through the entire chain. This meant Inventory, Shipping, and Notification all had to know about fraud rejection. The "decoupling" was illusory—we'd just made coupling implicit and distributed.

**The cascade failure:** One night, the Notification service went down briefly. It recovered fine. But it had missed events while it was down. Orders were stuck—they'd completed all the way through shipping but never got the confirmation email. We had to manually replay events, which created duplicate notifications. Then we had to add idempotency keys. Then testing became a nightmare because services could process events out of order.

**The hard rule:** Choreography works cleanly for **linear workflows with fewer than 5 services**. Order → Payment → Inventory is fine. The moment you have branching logic (fraud check, multi-warehouse allocation, international shipping) or more than 5 services, choreography becomes a distributed debugging tax that you'll pay forever.

### When Choreography Shines

- **Simple, linear flows:** Order creation with 2-3 steps
- **High autonomy:** Services don't need to know about each other's logic
- **Low infrastructure cost:** Event broker handles coordination. No orchestrator service to run
- **Resilience:** No single point of failure. If one service is slow, it doesn't block others

---

## Orchestration: The Centralized Director

Orchestration is the opposite bet. A single Saga Orchestrator service owns the workflow. It tells each service what to do, waits for a response, then decides the next step.

### The Same Order Saga, Orchestrated

```
Customer places order
       ↓
OrderService: CREATE order, SEND to Orchestrator
       ↓
Orchestrator: "I'll manage this from here"
       ↓ sends "ProcessPayment" command to PaymentService
       ↓
PaymentService: CHARGE payment, RESPOND with "PaymentOK"
       ↓
Orchestrator: "Payment succeeded. Next: reserve inventory."
       ↓ sends "ReserveStock" command to InventoryService (quantity: 10)
       ↓
InventoryService: RESERVE stock, RESPOND with "StockOK"
       ↓
Orchestrator: "Inventory reserved. Next: schedule shipping."
       ↓ sends "ScheduleShipping" command to ShippingService
       ↓
ShippingService: SCHEDULE delivery, RESPOND with "ShippingOK"
       ↓
Orchestrator: "All steps done. Mark saga complete."
       ↓
✓ Order saga complete.
```

Every step goes through the Orchestrator. It has total visibility. It knows *exactly* what's been done and what's next.

### When Shipping Fails in Orchestration

```
Orchestrator sent all commands successfully up to shipping.
       ↓ ShippingService responds: "SchedulingFailed"
       ↓
Orchestrator: "Shipping failed at step 3. I'll compensate steps 2, 1."
       ↓ sends "ReleaseStock" command to InventoryService
       ↓
InventoryService: RELEASE stock, RESPOND with "StockReleased"
       ↓
Orchestrator: "Stock released. Next: refund payment."
       ↓ sends "RefundPayment" command to PaymentService
       ↓
PaymentService: REFUND payment, RESPOND with "PaymentRefunded"
       ↓
Orchestrator: "All compensations done. Mark saga failed."
       ↓
✗ Saga rolled back. Send customer a "Shipping unavailable" message.
```

The Orchestrator runs through a failure path defined in code. It knows the compensation order. It tracks what's been done and what remains.

### Real-World Orchestration: The Bottleneck

On the flip side, I've been in production where orchestration became the constraint.

We implemented a Saga Orchestrator using a commercial workflow engine (won't name names, but it was popular). The orchestrator ran in a single process, handling all order sagas. It worked great at 100 orders/second.

At 500 orders/second, the Orchestrator became saturated. Every decision flowed through it. If Orchestrator processing latency increased from 10ms to 50ms, suddenly all orders slowed down proportionally. We tried to scale horizontally, but the workflow engine's distributed state synchronization became a problem. We had to shard sagas by customer ID to avoid contention.

**The operational burden:** The Orchestrator had its own state machine. If it crashed mid-saga, we needed to replay from the last known state. If state got corrupted, we had to manually inspect saga instances and update their state. Adding a new service to the workflow meant updating the Orchestrator's state machine definition and redeploying. The Orchestrator became a critical service that required obsessive monitoring.

**But here's the win:** When something went wrong, we had one place to look. If an order saga failed in an unexpected way, we had the entire state history in the Orchestrator. We could see exactly which step failed and why. Debugging was infinitely easier than with choreography.

### When Orchestration Makes Sense

- **Complex workflows with branches:** "If fraud check passes, charge immediately. If it fails, hold and review."
- **Multiple failure paths:** Different compensation strategies for different failures
- **5+ services:** Coordination overhead is dominated by the central orchestrator, not by service autonomy
- **Strong audit requirements:** You need a clear log of what happened and in what order
- **Strict ordering requirements:** Services must be called in a specific sequence, potentially with conditional branching

---

## Choreography vs. Orchestration: How I Decide

Here's how I think about it in an interview or in real architecture discussions:

| Aspect | Choreography | Orchestration |
|--------|--------------|---------------|
| **Control Flow** | Events trigger next step (distributed) | Orchestrator decides next step (centralized) |
| **Coupling** | Loosely coupled services | Tightly coupled to Orchestrator |
| **Visibility** | Poor (need to trace across logs) | Excellent (single source of truth) |
| **Testing** | Hard (mock all service interactions) | Easy (mock orchestrator responses) |
| **Scaling** | Linear (add service, add event handler) | Potential bottleneck (orchestrator limits throughput) |
| **Debugging** | Distributed (reconstruct from events) | Centralized (read saga state) |
| **Failure Recovery** | Implicit (events trigger compensation) | Explicit (orchestrator manages compensation) |
| **Operational Complexity** | Distributed reasoning required | Centralized state to manage |
| **Best For** | Simple, linear, high-autonomy workflows | Complex, branching, audit-heavy workflows |

**My rule of thumb:** If you can draw the workflow in a simple left-to-right line with no branches, and it has fewer than 5 services, use choreography. The moment you need branching logic (fraud checks, A/B tests, conditional shipping) or more than 5 services, the operational complexity of choreography exceeds the operational overhead of orchestration. Switch to orchestration.

---

## Compensating Transactions: The Hardest Part

This is where architect-level thinking separates from junior-level understanding. Anyone can execute a happy path. The real skill is designing failure gracefully.

### What Compensating Transactions Actually Are

A compensating transaction is **not** an automatic rollback. It's an explicit, deliberate business operation that undoes the side effects of the forward transaction. This distinction is crucial.

In a monolith:
```
BEGIN TRANSACTION
  INSERT order
  INSERT payment
  RESERVE inventory
COMMIT
```

If RESERVE inventory fails, everything rolls back automatically. Instant, atomic, no partial state.

In a saga:
```
STEP 1 (local txn): INSERT order → succeeds, now committed
STEP 2 (local txn): INSERT payment → succeeds, now committed
STEP 3 (local txn): RESERVE inventory → FAILS
→ Now we're partially committed. We must explicitly undo steps 1 and 2.
```

The compensating operations are:
- Compensate step 2: **Refund payment** (not automatic reversal—an actual business operation)
- Compensate step 1: **Cancel order** (another business operation)

But here's what makes compensation hard: **Some operations cannot be undone.**

### Transactions That Can't Be Compensated

Consider these real-world examples:

1. **Sending an email:** You've published a customer confirmation email. The compensation transaction is... what? You can't "unsend" an email. You could send a cancellation email, but the customer's already seen the first one. The best you can do is send a follow-up.

2. **Calling an external API with side effects:** You've submitted a payment to a third-party processor. The processor has taken the money. Compensation is to call the refund endpoint, but the refund is a separate API call that could fail. If it fails, you're in an inconsistent state—the payment went through but the refund didn't. You need manual intervention.

3. **Creating a user account in another system:** You've created a user in an identity provider (like Auth0). Compensation would be to delete the user, but that's risky—the user might have already logged in and started using the system. Better to deactivate the user and let an admin review.

4. **Allocating inventory from multiple warehouses:** You've reserved 5 units from Warehouse A and 5 from Warehouse B. Compensation would release both. But what if Warehouse A's release succeeds and Warehouse B's fails? Now you have partial compensation. Idempotency becomes critical.

5. **Publishing data to read replicas:** You've written an order to your read-only data warehouse (used for analytics). Compensation is to delete it. But the deletion is asynchronous. Other services might have already read the write-only version. Eventual consistency issues.

### Execution Order on Failure: Reverse Dependency Order

The key principle: compensate in **reverse order of successful execution**.

```
Forward steps executed:
  1: CreateOrder → success
  2: ChargePayment → success
  3: ReserveStock → success
  4: ScheduleShipping → FAIL

Steps to compensate:
  3: ReleaseStock (depends on ReserveStock)
  2: RefundPayment (depends on ChargePayment)
  1: CancelOrder (depends on CreateOrder)

Order matters: if ReleaseStock fails and we've already RefundPayment,
we're in an even worse state.
```

But what if compensation itself fails?

```
Forward: 1 → 2 → 3 → 4 (fails)
Compensation: 3 (fails)
            → What do we do?
```

You're now in a stuck state. The forward couldn't complete, and the compensation can't complete. You need a **saga abort handler**:
- Log the failure with context (saga ID, step, error)
- Move the saga to a Dead Letter Queue
- Alert ops. This requires manual intervention.
- Once ops investigates and fixes (e.g., warehouse comes back online, payment processor recovers), replay the compensation.

This is where **saga state persistence** becomes non-negotiable. You must store the saga's state in a durable database, not in memory.

### Idempotency: The Non-Negotiable Requirement

Networks fail. Messages duplicate. If a service times out while processing a refund, the client might retry, causing the refund to be issued twice.

Every compensating transaction must be **idempotent**. You must be able to execute it twice and get the same outcome as executing it once.

**How to achieve idempotency:**

1. **Idempotency keys:** Each compensating transaction has a unique key, typically a hash of saga ID + step name:

```
RefundPayment(
  saga_id: "order-12345",
  step_name: "refund_payment",
  idempotency_key: hash("order-12345" + "refund_payment")
)
```

2. **Check before executing:** Before issuing the refund, check: "Have I already refunded this saga?" Use the idempotency key as a lookup.

```
refund_id = db.query("SELECT refund_id FROM refunds WHERE idempotency_key = ?", key)
if refund_id exists:
  return refund_id  // already done, return same result
else:
  refund_id = payment_processor.refund(amount)
  db.insert("refunds", { idempotency_key, refund_id })
  return refund_id
```

3. **Persist idempotency records:** Store which operations you've already done. If the orchestrator crashes, it replays the saga and sees "oh, I already issued this refund" and skips it.

Without idempotency, a network retry during compensation can cause cascade failures: you refund the customer twice, they get two confirmations, accounts payable is confused, and you're now debugging financial discrepancies.

### The "Unsure State" Problem: Orchestrator Crashes

The orchestrator is executing the saga. It's in step 3 of 4. It sends a "ReleaseStock" command to Inventory Service. Inventory executes it, publishes "StockReleased". But before the orchestrator can receive the response, it crashes.

On restart, the orchestrator replays the saga from state. It sees "step 3 was started but status is unknown." Does it retry ReleaseStock? If it retries without idempotency, stock gets released twice.

**The solution:**

1. **Saga state persistence:** Store the saga's state in durable storage (PostgreSQL, DynamoDB, etc.), not in memory. Saga looks like:

```json
{
  "saga_id": "order-12345",
  "status": "in_progress",
  "current_step": 3,
  "steps": [
    { "name": "CreateOrder", "status": "succeeded" },
    { "name": "ChargePayment", "status": "succeeded" },
    { "name": "ReserveStock", "status": "in_progress", "sent_at": 1234567890 },
    { "name": "ScheduleShipping", "status": "pending" }
  ],
  "compensation_status": null
}
```

2. **Heartbeat and timeout:** If a step's status is "in_progress" for longer than expected (e.g., 30 seconds), assume the service is down or the message was lost. Trigger compensation.

3. **Idempotency on replay:** When you retry "ReleaseStock", the Inventory service sees the same idempotency key and returns the same result without duplicating the operation.

---

## Production Patterns: The Reality of Running Sagas

### Timeouts and Deadletter Queues

Set a timeout for each step. If a service doesn't respond within the timeout, assume failure and trigger compensation:

```
Orchestrator sends command to PaymentService
timeout_at = now() + 30_seconds

if response received before timeout_at:
  handle response
else:
  // timeout
  move saga to deadletter queue
  trigger compensation (or wait for manual intervention)
```

A Deadletter Queue holds sagas that timed out or failed in ways the system can't handle automatically. Ops reviews these manually:
- Was the service down? Wait for it to recover, then replay.
- Was the operation partially executed? Inspect the service's state and decide whether to continue or compensate.
- Is there a persistent bug? Fix the code, then replay.

### Event Sourcing with Sagas

Store all saga events in an append-only log. On orchestrator restart, replay the log to reconstruct the saga state:

```
EventLog:
  saga-1: OrderCreated (order-12345)
  saga-1: PaymentProcessed (order-12345)
  saga-1: StockReserved (order-12345)
  saga-1: ShippingFailed (order-12345, reason: warehouse_closed)
  saga-1: StockReleased (order-12345) [compensation]
  saga-1: PaymentRefunded (order-12345) [compensation]
  saga-1: SagaFailed (order-12345) [final state]
```

Replaying the log tells you exactly what happened, in order, with timestamps. It's invaluable for debugging and auditing.

### Monitoring and Observability

Sagas are invisible by default. You must instrument them:

1. **Saga duration:** Track how long sagas take end-to-end. A spike in duration means a service is slow or backing up.

2. **Failure rate by step:** "Which step fails most often?" If compensation always fails at step 2, that service is unreliable.

3. **Compensation frequency:** How often do sagas fail and trigger compensation? If it's common, your services aren't stable enough.

4. **Deadletter queue size:** Growing deadletter queue is an alarm. It means sagas can't complete automatically.

5. **State recovery latency:** When an orchestrator restarts, how long does it take to resume pending sagas? Slow recovery means customer-facing delays.

### Saga State Persistence Strategy

**Option 1: Relational Database**
- Pros: ACID guarantees, easy to query, triggers can automate timeouts
- Cons: Schema changes require migration

**Option 2: NoSQL (DynamoDB, MongoDB)**
- Pros: Flexible schema, scales horizontally
- Cons: eventual consistency if you need to read-after-write your own writes

**Option 3: Event Sourcing**
- Pros: Full audit trail, reproducible
- Cons: Event store queries can be slow, need snapshots for performance

I prefer option 1 (relational DB) with a trigger-based timeout mechanism. Store the saga state, create an index on `current_step_timeout_at`, and a background job queries expired sagas and moves them to compensation.

---

## The Interview Response: Architect-Level Framing

When asked about sagas, here's what shows you understand at an architect level:

> "Sagas solve distributed transactions by breaking them into a sequence of local transactions, each with compensation. There are two coordination models.
>
> **Choreography** uses events. Services listen and react, triggering the next step. It's loosely coupled and has no single point of failure. But flow is scattered across services. Debugging is hard because you have to reconstruct the state from distributed logs. And testing is complex—you have to mock all the services. I use choreography for simple, linear workflows with fewer than 5 services. Once you have branching logic or more services, the cognitive and debugging overhead explodes.
>
> **Orchestration** uses a central Saga Orchestrator. It tells each service what to do and waits for responses. It has total visibility into the workflow. Debugging is easy—everything's in one place. But the orchestrator is a coordination point. If it fails, sagas stall. And it becomes a bottleneck under load.
>
> The trickiest part is **compensating transactions**. They're not automatic rollbacks—they're explicit, deliberate business operations. Some operations can't be compensated (you can't 'unsend' an email). You have to execute compensation in reverse order of successful steps. And every compensating transaction must be idempotent—if a network retry causes it to execute twice, it should have the same effect as executing once. I use idempotency keys to track which operations have already run, so replays are safe.
>
> The hardest scenario is the orchestrator crash. If it crashes mid-saga, it has to restart, query its persistent state, and figure out where it left off. If it retried a step without idempotency, you get duplicate operations. So you need saga state in durable storage, timeouts for stuck steps, and a deadletter queue for manual review.
>
> In production, I've used choreography for simple order pipelines and orchestration for complex financial transactions where we need audit trails and deterministic behavior. The choice depends on your workflow complexity and your tolerance for operational complexity."

---

## Interview Tip

**At senior/principal level, you're expected to know not just how sagas work, but the production costs of each approach.**

Prepare examples:
- A successful choreography (3-step order pipeline)
- Where choreography broke (more than 5 services, branching logic, debugging nightmare)
- A successful orchestration (financial saga with 7 services and complex compensation)
- A failure case (orchestrator bottleneck, orchestrator crash scenario)

Show that you understand the trade-off between visibility and coupling. Show that you've thought about compensation failures and idempotency. Show that you know sagas aren't magic—they move the complexity from "distributed transactions" to "distributed failure handling."

The best answer isn't "use orchestration, it's better." It's "choreography for this use case, orchestration for that one, and here's why I'd monitor these metrics to make sure it's working."
