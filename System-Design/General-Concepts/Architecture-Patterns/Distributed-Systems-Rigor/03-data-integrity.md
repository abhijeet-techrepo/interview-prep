# Data Integrity

> "In a distributed system, the hard question is not 'how do we store data' — it's 'how do we keep multiple stores consistent when any one of them can fail at any time.'"

[← Back to Distributed Systems Rigor](./README.md) | **Related:** [Idempotency](./01-idempotency.md) · [Concurrency](./02-concurrency.md)

---

## Quick Revision Mind Map

```
                          DATA INTEGRITY
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
       FOUNDATIONS        TRANSACTIONS          CONSISTENCY
       ────────────        ────────────          ───────────
       ACID properties     Single DB: ACID        Strong
       BASE principles     Multi-service: Saga    Eventual
       CAP theorem         2-Phase Commit         Causal
       Isolation levels    Compensating TXs       Read-your-writes
                           Outbox pattern         Monotonic reads

       CAP Decision:
       Consistency  →  CP systems (ZK, etcd, HBase)
       Availability →  AP systems (Cassandra, DynamoDB)
```

---

## Jump to Section

| Concept | One-Liner | Details |
|---------|-----------|---------|
| **ACID** | 4 properties that make DB transactions reliable | [→](#acid-properties) |
| **BASE** | The distributed system trade-off to ACID | [→](#base-principles) |
| **CAP Theorem** | You can only have 2 of 3: C, A, P | [→](#cap-theorem) |
| **Isolation Levels** | How much "bleeding" between concurrent transactions | [→](#isolation-levels) |
| **2-Phase Commit** | Cross-DB atomic commit — powerful but dangerous | [→](#two-phase-commit-2pc) |
| **Saga Pattern** | Distributed transactions without 2PC | [→](#saga-pattern) |
| **Compensating Transactions** | The "undo" for failed sagas | [→](#compensating-transactions) |
| **Consistency Models** | Strong, eventual, causal, read-your-writes | [→](#consistency-models) |
| **Write-Ahead Log** | How databases survive crashes | [→](#write-ahead-log-wal) |
| **Walmart Scenarios** | Cross-service data integrity at scale | [→](#walmart-scenarios) |

---

## ACID Properties

### A — Atomicity

**All or nothing.** A transaction either commits entirely or rolls back entirely. There is no partial commit.

```sql
-- Example: Transfer $100 from Account A to Account B
BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 'A';
  UPDATE accounts SET balance = balance + 100 WHERE id = 'B';
COMMIT;

-- If the second UPDATE fails: ROLLBACK
-- Account A is NOT debited without Account B being credited
```

**In distributed systems:** Atomicity across multiple services is the hard problem. You cannot call Service A's DB and Service B's DB in a single atomic SQL transaction.

### C — Consistency

**Data moves from one valid state to another.** DB constraints (foreign keys, UNIQUE, CHECK) are never violated.

```sql
-- Constraint: balance must never be negative
ALTER TABLE accounts ADD CONSTRAINT chk_balance CHECK (balance >= 0);

-- Transaction that would violate consistency is rejected
BEGIN;
  UPDATE accounts SET balance = balance - 1000 WHERE id = 'A' AND balance = 50;
  -- balance would become -950 → CHECK constraint violation → ROLLBACK
COMMIT;
```

### I — Isolation

**Concurrent transactions behave as if they ran serially.** See [Isolation Levels](#isolation-levels) for the spectrum.

### D — Durability

**Committed transactions survive crashes.** Once COMMIT is acknowledged, the data is on disk (via WAL). A power loss after COMMIT does not lose data.

**How it works:**
```
Write-Ahead Log (WAL):
  1. Append transaction to WAL on disk (sequential, fast)
  2. Write "COMMIT" record to WAL
  3. Return success to client
  4. Asynchronously apply to actual data pages (checkpointing)

On crash: replay WAL from last checkpoint → guaranteed durability
```

---

## BASE Principles

The alternative to ACID for distributed systems that prioritize availability.

| Letter | Full Name | Meaning |
|--------|-----------|---------|
| **B** | Basically Available | System remains available, even during partial failures |
| **S** | Soft State | State may change over time even without new input (replication lag) |
| **E** | Eventually Consistent | Given enough time with no new writes, all nodes converge to same value |

**ACID vs BASE:**

| Dimension | ACID | BASE |
|-----------|------|------|
| Consistency | Immediate | Eventually |
| Availability | May sacrifice for consistency | Prioritized |
| Partition handling | May become unavailable | Remains available with degraded consistency |
| Example systems | PostgreSQL, MySQL, Oracle | Cassandra, DynamoDB, CouchDB |
| Best for | Financial transactions, inventory | User preferences, social feeds, analytics |

**Not a binary choice:** Most systems use ACID within a single service and accept BASE semantics between services. This is the microservices norm.

---

## CAP Theorem

**In a distributed system, during a network partition, you must choose between Consistency and Availability.** You cannot have both.

```
        Consistency
        (every read gets
        the latest write)
              △
             / \
            /   \
           / CA  \   ← Only possible on single node
          /   (no \     (no partition to handle)
         /  partition)\
        ─────────────────
       /                 \
      /  CP               AP  \
     (ZooKeeper,         (Cassandra,
      etcd, HBase)        DynamoDB,
      ← Refuse reads       CouchDB)
        during partition   ← Accept stale
                             reads during
                             partition
     Partition Tolerance ────────────────── Partition Tolerance
```

**Always choose P (Partition Tolerance):** Distributed systems MUST handle network partitions. The real choice is C vs A when partition occurs.

### CP Systems (Choose Consistency over Availability)

| System | Trade-off During Partition |
|--------|--------------------------|
| ZooKeeper | Stops serving reads/writes to maintain consistency |
| etcd | Same — leader election required before serving |
| HBase | Regions become unavailable |
| Traditional RDBMS (clustered) | May block or fail to avoid returning stale data |

**Use when:** Correctness is critical — financial data, inventory counts, configuration management.

### AP Systems (Choose Availability over Consistency)

| System | Trade-off During Partition |
|--------|--------------------------|
| Cassandra | Serves reads and writes; may return stale data |
| DynamoDB | Eventually consistent by default (strong consistency option costs more) |
| CouchDB | Serves local replica; sync later |

**Use when:** User experience requires availability — social feeds, shopping cart contents, user preferences.

**Tunable Consistency (Cassandra):**

```
Consistency Level = ALL  → Reads from all replicas. Consistent but slow.
Consistency Level = ONE  → Reads from nearest replica. Fast but possibly stale.
Consistency Level = QUORUM → Reads from majority. Balance of both.

For strong consistency: Write QUORUM + Read QUORUM with RF=3
  → Guarantees at least one overlapping node between write and read quorum
```

---

## Isolation Levels

How much "bleeding" is permitted between concurrent transactions.

| Isolation Level | Dirty Read | Non-Repeatable Read | Phantom Read | Write Skew | Performance |
|----------------|-----------|--------------------|-----------|-----------|----|
| Read Uncommitted | ✅ Possible | ✅ Possible | ✅ Possible | ✅ Possible | Fastest |
| **Read Committed** | ❌ Protected | ✅ Possible | ✅ Possible | ✅ Possible | Good (PG default) |
| **Repeatable Read** | ❌ Protected | ❌ Protected | ❌ PG protects | ✅ Possible | Medium (MySQL default) |
| **Serializable** | ❌ Protected | ❌ Protected | ❌ Protected | ❌ Protected | Slowest |

```sql
-- Set isolation level per transaction
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
  -- All anomalies prevented; transactions appear to run serially
COMMIT;

-- Or set globally
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

**Walmart guidance:**
- **Inventory deduction:** Use Repeatable Read or Serializable (prevent phantom adds)
- **Payment processing:** Serializable (prevent write skew on concurrent payments)
- **Analytics reads:** Read Committed (performance > consistency)
- **User profile updates:** Read Committed (low contention, retries acceptable)

---

## Two-Phase Commit (2PC)

**The classic protocol for atomic commits across multiple databases.**

### How It Works

```
Coordinator (Order Service)
        │
        ├──── Phase 1: PREPARE ────────────────────┐
        │                                          │
        ▼                                          ▼
  Participant A                             Participant B
  (Payment DB)                             (Inventory DB)
  "Can you commit?"                        "Can you commit?"
        │                                          │
        │ YES / NO                                 │ YES / NO
        └────────────────┐         ┌───────────────┘
                         ▼         ▼
                    Coordinator receives votes
                         │
                    ┌────┴────┐
                    │         │
                   ALL YES   ANY NO
                    │         │
                    ▼         ▼
               Phase 2:   Phase 2:
               COMMIT     ROLLBACK
               (all parts) (all parts)
```

### 2PC Failure Modes

| Failure Point | What Happens | Impact |
|--------------|-------------|--------|
| Coordinator crash after PREPARE, before COMMIT | All participants are in "prepared" state forever | **Blocking protocol** — participants can't unilaterally commit or rollback |
| Participant crash after PREPARE | On recovery, participant asks coordinator what to do | Coordinator must be alive to respond |
| Network partition during Phase 2 | Some participants commit, others don't | Data inconsistency! |

### When to Use 2PC

**Use:** When you have a single external system (payment gateway + your DB) and can tolerate the blocking.

**Avoid:** For microservices communication across 3+ services. Coordinator becomes SPOF.

```
XA Transactions in Java/Spring:
  @Transactional  // with XA datasource
  public void processOrder(Order order) {
      paymentRepository.save(payment);    // DB A
      inventoryRepository.update(item);  // DB B
      // 2PC handles atomic commit
  }
  // Requires: XA-compatible drivers, JTA transaction manager (Atomikos, Bitronix)
```

**Modern consensus:** Use Saga instead of 2PC for cross-service distributed transactions.

---

## Saga Pattern

**The practical solution for distributed transactions in microservices.**

Instead of one atomic commit, a saga is a sequence of local transactions, each publishing an event. If any step fails, compensating transactions undo the previous steps.

### Choreography-Based Saga

Services react to events from other services. No central coordinator.

```
Order Service        Payment Service       Inventory Service
     │                     │                      │
     │── OrderCreated ─────►│                      │
     │                      │                      │
     │               PaymentDebited ───────────────►│
     │                      │                      │
     │              InventoryReserved (success)     │
     │◄──────────────────────────────────────────── │
     │                      │                      │
  OrderCompleted            │                      │

FAILURE SCENARIO:
     │── OrderCreated ─────►│                      │
     │                      │                      │
     │             PaymentFailed ─────────────────► │
     │◄────────── OrderCancelled (compensation) ─── │
```

**Problem:** At 4+ services, tracing the flow becomes very hard. Use when ≤ 3 services.

### Orchestration-Based Saga (Preferred for Complex Flows)

A central saga orchestrator owns the workflow.

```java
@Service
public class OrderSagaOrchestrator {

    @SagaStart
    public void processOrder(OrderCreatedEvent event) {
        // Step 1: Reserve Inventory
        inventoryService.reserve(event.getOrderId(), event.getItems());
    }

    @SagaEventHandler
    public void on(InventoryReservedEvent event) {
        // Step 2: Process Payment
        paymentService.charge(event.getOrderId(), event.getAmount());
    }

    @SagaEventHandler
    public void on(PaymentSucceededEvent event) {
        // Step 3: Confirm Order
        orderService.confirm(event.getOrderId());
        SagaLifecycle.end();
    }

    // COMPENSATION HANDLERS
    @SagaEventHandler
    public void on(PaymentFailedEvent event) {
        // Compensate Step 1
        inventoryService.release(event.getOrderId());
        orderService.cancel(event.getOrderId());
        SagaLifecycle.end();
    }
}
```

### Saga State Machine

```
         ┌─────────┐
         │ STARTED │
         └────┬────┘
              │ inventoryReserved
              ▼
    ┌──────────────────┐
    │ INVENTORY_RESERVED│
    └────────┬─────────┘
             │ paymentProcessed
             ▼
    ┌──────────────────┐
    │ PAYMENT_PROCESSED│
    └────────┬─────────┘
             │ orderConfirmed
             ▼
        ┌─────────┐
        │COMPLETED│
        └─────────┘

    At any step, failure triggers compensation backwards
```

---

## Compensating Transactions

**The "undo" operations that reverse saga steps. Each step must have a compensating action.**

| Step | Forward Action | Compensating Action |
|------|---------------|-------------------|
| Reserve Inventory | `inventory.reserve(orderId, items)` | `inventory.release(orderId, items)` |
| Charge Payment | `payment.charge(orderId, amount)` | `payment.refund(orderId, amount)` |
| Create Shipment | `shipping.create(orderId)` | `shipping.cancel(orderId)` |
| Notify Customer | `notifications.sendConfirmation(orderId)` | `notifications.sendCancellation(orderId)` |

**Critical constraint:** Compensating transactions must also be idempotent. If the saga fails mid-compensation, it will retry.

```java
@Transactional
public void releaseInventory(String orderId) {
    // Idempotent: check if already released
    Optional<InventoryReservation> res = reservationRepo.findByOrderId(orderId);
    if (res.isEmpty() || res.get().getStatus() == RELEASED) {
        log.info("Inventory for order {} already released, skipping", orderId);
        return;  // Safe no-op
    }
    inventoryRepo.incrementQty(res.get().getSku(), res.get().getQty());
    reservationRepo.updateStatus(orderId, RELEASED);
}
```

---

## Consistency Models

**What does "consistency" mean for reads?**

### Strong Consistency

Every read reflects the most recent write. No stale reads ever.

```
Write: inventory.qty = 50 → committed
Read (1ms later, any node): inventory.qty = 50 ← guaranteed

Cost: All reads must go through primary/leader. Higher latency. Lower throughput.
Use: Payment balances, inventory counts, anything where stale data causes damage.
```

### Eventual Consistency

Given enough time with no new writes, all nodes converge to the same value.

```
Write: user.address = "123 Main St" → committed to primary
Read (from replica 1ms later): user.address = "100 Old Ave" ← stale (replication lag)
Read (200ms later, after replication): user.address = "123 Main St" ← converged

Cost: Readers may see stale data during replication window.
Use: User preferences, social feed, recommendation data, analytics.
```

### Causal Consistency

If A happened before B (causally), every client sees A before B.

```
User A: Posts a message (event M1)
User A: Replies to that message (event M2, caused by M1)

Causal consistency guarantees: any client that sees M2 has also seen M1.

Use: Comments/replies, document version chains, collaborative editing.
```

### Read-Your-Writes (Session Consistency)

After you write, your subsequent reads always reflect that write.

```
User updates profile photo
User immediately navigates to profile → sees new photo (not stale)

Implementation options:
  - Route user reads to primary for 5 seconds after a write
  - Include write timestamp in session; replica returns only data with ts >= that value
  - Sticky session: route this user to the same replica for N seconds

Use: Profile updates, settings changes, any "I just did this" scenario.
```

### Monotonic Reads

You never see older versions of data after seeing a newer version.

```
Read 1: user.plan = "premium"  (from replica with replication lag = 200ms)
Read 2: user.plan = "free"     ← MONOTONIC READ VIOLATION (older replica)
Read 3: user.plan = "premium"

Fix: Pin user to same replica, or use version vectors.
Use: Anything where "going back in time" is confusing (e.g., order status).
```

---

## Write-Ahead Log (WAL)

**How databases guarantee durability without syncing every write immediately.**

```
Write Flow:
  1. Transaction begins
  2. Changes written to WAL (sequential disk write — fast, O(1) seek)
  3. WAL flushed to disk (fsync)
  4. "COMMIT" record appended to WAL
  5. Acknowledgment returned to client ← client considers data durable
  6. (Async) Data pages updated from WAL (checkpointing)

Crash Recovery:
  1. On restart, replay WAL from last checkpoint
  2. All committed transactions re-applied
  3. Uncommitted (no COMMIT record) transactions rolled back
  4. DB returns to consistent state

Log Shipping / Replication:
  Primary streams WAL to replicas
  Replicas replay WAL to stay in sync
  → The basis of Postgres streaming replication, MySQL binlog
```

**Implications for distributed data integrity:**

```
WAL replication lag = eventual consistency window
  → Synchronous replication: replica must ack before primary commits (strong, slow)
  → Asynchronous replication: primary commits, replica catches up (fast, stale reads)

Postgres synchronous_standby_names = 'replica1'
  → Primary waits for replica1 to confirm WAL write before returning COMMIT
  → Failover to replica1 has no data loss
```

---

## Walmart Scenarios

### Scenario 1: Payment + Inventory — Distributed Transaction

```
Problem: Customer pays and we deduct inventory. These are different services.
         Payment commits, then inventory service crashes → inventory not deducted.
         We charged the customer for an item we might ship or might not.

Solution: Saga with compensation

Step 1: PaymentService.charge(orderId, amount)
  → SUCCESS → publish PaymentSucceeded event
  → FAILURE → publish PaymentFailed → saga ends, order cancelled

Step 2 (on PaymentSucceeded): InventoryService.reserve(orderId, items)
  → SUCCESS → publish InventoryReserved event
  → FAILURE → publish InventoryFailed
               → compensate: PaymentService.refund(orderId, amount)
               → order cancelled

Compensation is idempotent: refund(orderId) checks if already refunded.
Saga state persisted: if saga orchestrator crashes, it resumes from last known step.
```

### Scenario 2: Global Inventory Count — Consistency Trade-off

```
Walmart stores + online → shared inventory system
10,000 stores reading inventory counts simultaneously

Strong consistency: All reads hit primary
  → Primary becomes bottleneck under global read load
  → Unacceptable latency for product availability checks

Eventual consistency: Reads from regional replicas
  → Product shows "In Stock" on website, customer adds to cart
  → Inventory was 1, two customers buy simultaneously
  → One order fulfilled, one cancelled
  → Acceptable trade-off: cancellation rate vs global read performance

Solution: Soft reservation
  1. Website reads from replica (eventual) for display
  2. At cart commit: write to primary, pessimistic lock for last-mile reservation
  3. After reservation: send confirmation
  → Avoids both global bottleneck and silent overselling
```

### Scenario 3: Price Update Consistency

```
Problem: Price update for "iPhone 15" must appear consistently globally.
         Customer in one region sees $999, another sees $1,099.

Options:
  Option A: Synchronous replication (all regions must ack before COMMIT)
    → Low availability during network issues
    → High write latency (global round-trip)

  Option B: Eventual consistency with version timestamp
    → All regions see price change within X seconds
    → Brief window where regions disagree
    → Acceptable for pricing (price changes aren't instant in-store either)
    → Add: "Price may vary by location" during propagation window

  Option C: Dual-write with cache invalidation
    → Write to primary, invalidate CDN caches
    → Next read fetches fresh price from replica
    → Converges in seconds; acceptable for pricing
```

### Scenario 4: Order Status Consistency (Read-Your-Writes)

```
Customer places order → redirected to "Order Confirmation" page
Page fetches order status from read replica
Replica hasn't synced yet → "Order Not Found" error

Fix: Read-your-writes consistency for order confirmations
  1. After POST /orders → include write timestamp in session cookie
  2. GET /orders/{id} passes timestamp
  3. Backend: serve from primary if replica_lag > 0
     OR: route order confirmation reads to primary for 5 seconds after create
  4. After 5 seconds: replica has synced, normal routing resumes
```

---

## Interview Tip

> **Ready-to-say answer on data integrity:**
>
> *"Data integrity in distributed systems breaks down into three levels. First, within a single service: use ACID transactions, choose the right isolation level for the workload (Read Committed for most things, Serializable for financial operations with complex invariants). Second, across services: 2PC is rarely worth the operational cost — coordinator becomes a SPOF and the protocol is blocking. I prefer saga + compensating transactions. Each step publishes an event; failure triggers compensating actions in reverse order. Third, replication consistency: I align the consistency model with the business requirement. Strong consistency for inventory counts at checkout, eventual consistency for product catalog browsing, read-your-writes for order confirmation."*
>
> **Follow-up they'll ask:** "What if a compensating transaction also fails?"
>
> *"That's the hard case. Compensating transactions must themselves be idempotent and retried. If they keep failing, the saga enters a 'stuck' state that needs human intervention — but the key is we detect it. The saga's state machine persists every step to a database. Operations teams can see: 'Order 12345 is stuck in compensation at step 2, payment refund failed 3 times.' We can replay, manually intervene, or escalate. The critical thing is the system is never silently inconsistent — it's noisily stuck, which is far better."*

---

**Navigation:** [02 Concurrency ←](./02-concurrency.md) | [04 Walmart Scenarios →](./04-walmart-scenarios.md)
