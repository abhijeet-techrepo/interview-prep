# Idempotency

> "Distributed networks fail. Services crash. Messages are redelivered. Idempotency is what makes retries safe."

[← Back to Distributed Systems Rigor](./README.md) | **Related:** [Concurrency](./02-concurrency.md) · [Data Integrity](./03-data-integrity.md)

---

## Quick Revision Mind Map

```
                          IDEMPOTENCY
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
     DEFINITION          WHY IT MATTERS          PATTERNS
     ──────────          ──────────────          ──────────
     f(f(x)) = f(x)     At-least-once           1. Idempotency Key
     Same op, N times   delivery = retries       2. DB UNIQUE
     = same result      Network fail +           3. ON CONFLICT
                        retry = duplicate        4. Redis SETNX
                        charge / record          5. Conditional
                                                    Update
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   NATURAL vs           ANTI-PATTERNS         WALMART CONTEXT
   ENGINEERED           ─────────────         ───────────────
   ─────────────────    Timestamp dedup       Payment dedup
   GET/PUT/DELETE       "Once delivery"       Order placement
   = natural            assumption            Inventory deduction
   POST = not           No TTL on             Flash sale
   idempotent           dedup store           protection
```

---

## Jump to Section

| Concept | One-Liner | Details |
|---------|-----------|---------|
| **Definition** | f(f(x)) = f(x) — calling N times = calling once | [→](#definition) |
| **Why It's Critical** | At-least-once delivery guarantees retries; retries need idempotency | [→](#why-idempotency-is-critical) |
| **Natural Idempotency** | GET, PUT, DELETE are naturally idempotent; POST is not | [→](#natural-vs-engineered-idempotency) |
| **Idempotency Key Pattern** | Client generates UUID; server deduplicates on first process | [→](#pattern-1-idempotency-key) |
| **DB UNIQUE Constraint** | Database enforces uniqueness; duplicate insert fails silently | [→](#pattern-2-db-unique-constraint) |
| **Redis SETNX** | Distributed dedup with TTL; prevents cross-instance duplicates | [→](#pattern-3-redis-setnx) |
| **Conditional Update** | WHERE version = N ensures no double-process | [→](#pattern-4-conditional-update-optimistic-guard) |
| **Exactly-Once Processing** | Outbox pattern gives transactional event publishing | [→](#exactly-once-processing-in-queues) |
| **Anti-Patterns** | Timestamps, trusting "once delivery", no TTL | [→](#anti-patterns) |
| **Walmart Scenarios** | Payment, order, inventory, flash sale | [→](#walmart-scenarios) |

---

## Definition

**Idempotency** means that performing the same operation multiple times produces the same result as performing it once.

```
Idempotent:     charge_card(order_123, $50)  → $50 charged  (1st call)
                charge_card(order_123, $50)  → $50 charged  (2nd call — no-op, not double-charged)

NOT Idempotent: charge_card($50)             → $50 charged  (1st call)
                charge_card($50)             → $100 charged (2nd call — double charge!)
```

**The key insight:** the uniqueness of the operation is determined by a **stable identifier** (order ID, idempotency key), not by the number of calls.

---

## Why Idempotency Is Critical

In distributed systems, the question is **not** *if* a message will be delivered more than once. It's *when*.

```
Failure Scenarios That Cause Duplicate Delivery:
─────────────────────────────────────────────────────────────────────────────
Scenario                     What Happens
─────────────────────────────────────────────────────────────────────────────
Network timeout              Client retries → 2 requests reach server
Consumer crash after process Consumer re-reads → processes same message twice
Kafka at-least-once delivery Rebalance during commit → message reprocessed
Load balancer retry          Request reaches 2 different instances
Client-side retry logic      Client sends same request on 4xx/5xx
─────────────────────────────────────────────────────────────────────────────
```

**Without idempotency:**
- Double charges
- Duplicate inventory deductions
- Duplicate orders in the system
- Duplicate notifications to users

**With idempotency:**
- Second call is detected and short-circuited
- Same response returned to caller
- No side effects from the duplicate

---

## Natural vs Engineered Idempotency

### HTTP Methods — Natural Idempotency

| Method | Naturally Idempotent? | Why |
|--------|----------------------|-----|
| `GET` | ✅ Yes | Read-only, no state change |
| `PUT` | ✅ Yes | Sets state to exact value — calling N times = calling once |
| `DELETE` | ✅ Yes | Deleting already-deleted resource = no-op (should return 204) |
| `POST` | ❌ No | Creates new resource each time — calling twice = two records |
| `PATCH` | ❌ Usually No | Incremental changes (`+10`) are not idempotent |

**Critical:** POST is how most APIs create resources. You must **engineer** idempotency into POST.

### Engineered Idempotency (the real work)

For POST/Kafka consumers/gRPC mutations, you must explicitly design for idempotency. The four patterns below cover 95% of real scenarios.

---

## Pattern 1: Idempotency Key

**Concept:** Client generates a unique key for each logical operation. Server stores key → result mapping. On duplicate, server returns stored result.

**Flow:**
```
Client                              Server
  │                                   │
  ├─── POST /charge                   │
  │    Idempotency-Key: uuid-abc       │
  │    {amount: 50, card: ****}        │
  │                                   │
  │                          ┌────────┤
  │                          │ Check:  │
  │                          │ uuid-abc│
  │                          │ exists? │
  │                          └────────┤
  │                                   │
  │                          ┌────────┤
  │                          │ NO:     │
  │                          │ Process │
  │                          │ charge  │
  │                          │ Store   │
  │                          │ result  │
  │                          └────────┤
  │◄── 200 OK {charge_id: ch_123} ────┤
  │                                   │
  │    [network drop — client retries] │
  │                                   │
  ├─── POST /charge                   │
  │    Idempotency-Key: uuid-abc       │
  │                                   │
  │                          ┌────────┤
  │                          │ YES:    │
  │                          │ Return  │
  │                          │ stored  │
  │                          │ result  │
  │                          └────────┤
  │◄── 200 OK {charge_id: ch_123} ────┤  ← Same result, no new charge
```

**Implementation:**

```sql
-- Idempotency store table
CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(128) PRIMARY KEY,  -- client-generated UUID
    operation       VARCHAR(64)  NOT NULL,     -- "charge", "order_create"
    response_code   INT          NOT NULL,
    response_body   JSONB        NOT NULL,
    created_at      TIMESTAMP    DEFAULT NOW(),
    expires_at      TIMESTAMP    NOT NULL       -- TTL: 24h or 7 days
);
```

```java
// Spring Boot service
@Transactional
public ChargeResponse charge(ChargeRequest req, String idempotencyKey) {
    // Check for existing result
    Optional<IdempotencyRecord> existing = idempotencyRepo.findById(idempotencyKey);
    if (existing.isPresent()) {
        return existing.get().getResponse(ChargeResponse.class);  // return cached
    }

    // Process the charge
    ChargeResponse response = paymentGateway.charge(req);

    // Store result atomically with the charge
    idempotencyRepo.save(new IdempotencyRecord(
        idempotencyKey, "charge", response, Instant.now().plus(7, DAYS)
    ));

    return response;
}
```

**Key Design Decisions:**

| Decision | Recommendation | Reason |
|----------|---------------|--------|
| Who generates the key? | **Client** | Client knows when to retry; server can't know intent |
| Key format | UUID v4 | Random, collision-resistant |
| Storage TTL | 24h–7 days | Balance storage cost vs retry window |
| On duplicate: return cached or reprocess? | **Return cached** | Idempotency = same result, not re-execution |
| Store in same DB transaction? | **Yes** | Prevents partial failures (process but not store) |

---

## Pattern 2: DB UNIQUE Constraint

**Concept:** Use the database's uniqueness enforcement as the idempotency guard. Duplicate insert fails; you handle the exception.

**Best for:** When the operation IS the insert — creating orders, payments, records.

```sql
-- Order table with natural idempotency
CREATE TABLE orders (
    id              BIGINT PRIMARY KEY AUTO_INCREMENT,
    client_order_id VARCHAR(128) UNIQUE NOT NULL,  -- ← Idempotency enforced here
    customer_id     BIGINT NOT NULL,
    amount          DECIMAL(10,2) NOT NULL,
    status          VARCHAR(32) DEFAULT 'PENDING',
    created_at      TIMESTAMP DEFAULT NOW()
);

-- First insert: succeeds
INSERT INTO orders (client_order_id, customer_id, amount)
VALUES ('client-ref-abc', 1001, 99.99);
-- → 1 row inserted, id = 5001

-- Duplicate insert: fails with UNIQUE constraint violation
INSERT INTO orders (client_order_id, customer_id, amount)
VALUES ('client-ref-abc', 1001, 99.99);
-- → ERROR 1062 (Duplicate entry 'client-ref-abc' for key 'client_order_id')
```

```java
@Transactional
public Order createOrder(CreateOrderRequest req) {
    try {
        Order order = orderRepo.save(new Order(req.getClientOrderId(), req.getAmount()));
        return order;
    } catch (DataIntegrityViolationException e) {
        // Duplicate — return existing record
        return orderRepo.findByClientOrderId(req.getClientOrderId())
            .orElseThrow(() -> new RuntimeException("Unexpected race condition"));
    }
}
```

**`ON CONFLICT` (Postgres) / `ON DUPLICATE KEY` (MySQL) — cleaner approach:**

```sql
-- Postgres: upsert-safe idempotency
INSERT INTO orders (client_order_id, customer_id, amount)
VALUES ('client-ref-abc', 1001, 99.99)
ON CONFLICT (client_order_id) DO NOTHING
RETURNING *;

-- MySQL: same
INSERT INTO orders (client_order_id, customer_id, amount)
VALUES ('client-ref-abc', 1001, 99.99)
ON DUPLICATE KEY UPDATE client_order_id = client_order_id;  -- no-op update to trigger RETURNING
```

**When to use UNIQUE vs Idempotency Key table:**

| Criteria | UNIQUE Constraint | Idempotency Key Table |
|----------|-----------------|----------------------|
| Operation | Is an INSERT | Is a multi-step mutation |
| Response | The inserted row | Any response (including external API results) |
| Complexity | Low — DB handles it | Higher — explicit store/retrieve |
| Cross-service | Same DB only | Works cross-service (different DBs) |
| TTL | No built-in TTL | Explicit TTL field |

---

## Pattern 3: Redis SETNX

**Concept:** Use Redis `SET NX PX` (set if not exists, with expiry) as a distributed deduplication lock.

**Best for:** Stateless services with multiple instances, high-throughput scenarios.

```
SETNX Flow:
────────────────────────────────────────────────────────────
Instance A receives event-123 → SET event-123 "processed" NX PX 86400000
                               → OK (first to win)
                               → Process event
                               → Update DB

Instance B receives event-123  → SET event-123 "processed" NX PX 86400000
(duplicate, different instance) → NIL (key already exists)
                               → Skip processing
                               → Acknowledge to queue
```

```java
@Autowired
private StringRedisTemplate redis;

public void processEvent(Event event) {
    String dedupKey = "dedup:" + event.getEventId();
    String result = redis.opsForValue().setIfAbsent(
        dedupKey,
        "processed",
        Duration.ofDays(1)  // TTL: 24 hours
    ) ? "SET" : "EXISTS";

    if ("EXISTS".equals(result)) {
        log.info("Duplicate event {}, skipping", event.getEventId());
        return;  // Safe no-op
    }

    // Process event — we're the first and only processor
    processPayment(event);
}
```

**Critical: TTL Selection**

| Scenario | Recommended TTL | Reason |
|----------|----------------|--------|
| Payment processing | 7 days | Retry window for failed payments |
| Order creation | 24 hours | Business retry window |
| Kafka consumer events | 48 hours | Covers maximum consumer lag scenarios |
| Flash sale inventory | 1 hour | Short window, high throughput |

**Redis SETNX Pitfall: GC Pause / Clock Drift**

```
Problem:
  1. Instance A acquires SETNX lock, TTL = 30s
  2. Instance A pauses for GC for 35s
  3. Lock expires
  4. Instance B acquires lock, starts processing
  5. Instance A resumes — both now processing!

Fix: Fencing token (monotonic version) — DB update includes token check
  UPDATE inventory SET qty = qty - 1, fence = 42
  WHERE id = 123 AND fence < 42;   ← Rejects stale writes
```

---

## Pattern 4: Conditional Update (Optimistic Guard)

**Concept:** Use a version number or hash to ensure you're processing the same state as expected. If state changed, the update fails.

**Best for:** Preventing double-processing on stateful records.

```sql
-- Mark an order as "paid" only if it's currently "pending"
UPDATE orders
SET    status = 'PAID', paid_at = NOW()
WHERE  id = 5001
AND    status = 'PENDING';         -- ← Idempotency guard
-- Returns rows_affected: if 0, already processed (or gone)

-- With version number
UPDATE orders
SET    status = 'PAID', version = version + 1
WHERE  id = 5001
AND    version = 3;                -- ← Only succeeds if version matches
```

```java
@Transactional
public void markOrderPaid(Long orderId, String chargeId) {
    int rowsAffected = orderRepo.updateStatusIfPending(orderId, "PAID", chargeId);
    if (rowsAffected == 0) {
        Order order = orderRepo.findById(orderId).orElseThrow();
        if ("PAID".equals(order.getStatus())) {
            log.info("Order {} already marked paid — idempotent", orderId);
            return;  // Safe — same intended state
        }
        throw new ConflictException("Order in unexpected state: " + order.getStatus());
    }
}
```

---

## Exactly-Once Processing in Queues

No messaging system truly guarantees exactly-once delivery at the infrastructure level. Kafka `enable.idempotence=true` gives exactly-once **within the broker** but not end-to-end. You must implement it at the application level.

**The pattern: Transactional Outbox + Idempotent Consumer**

```
Producer side (Transactional Outbox):
  ─────────────────────────────────────────────────────────────────────
  BEGIN TRANSACTION
    UPDATE orders SET status='PAID' WHERE id=123
    INSERT INTO outbox (event_type, payload) VALUES ('PaymentProcessed', {...})
  COMMIT
  → Relay picks up outbox row and publishes to Kafka

Consumer side (Idempotent Consumer):
  ─────────────────────────────────────────────────────────────────────
  On receive event:
    1. Check: event_id in processed_events table?
       YES → acknowledge, skip
       NO  → continue
    2. BEGIN TRANSACTION
         INSERT INTO processed_events (event_id) VALUES (evt_id)
         -- Process business logic here
       COMMIT
    3. Acknowledge Kafka offset
```

```sql
-- Processed events table (with TTL cleanup)
CREATE TABLE processed_events (
    event_id   VARCHAR(128) PRIMARY KEY,
    topic      VARCHAR(128) NOT NULL,
    processed_at TIMESTAMP  DEFAULT NOW(),
    INDEX idx_processed_at (processed_at)  -- for cleanup job
);

-- Cleanup job: DELETE FROM processed_events WHERE processed_at < NOW() - INTERVAL 7 DAY;
```

---

## Anti-Patterns

| Anti-Pattern | Why It Fails | Fix |
|-------------|-------------|-----|
| **Timestamp-based dedup** | Clock skew: two events at "same" ms; duplicates slip through | Use event UUID, not timestamp |
| **Trusting "at-most-once"** | Networks fail; brokers retry; you will get duplicates | Always design for at-least-once |
| **No TTL on dedup store** | Storage bloat; memory exhaustion in Redis | Set explicit TTL, run cleanup jobs |
| **Checking before inserting without transaction** | Race: two threads both see "not exists", both insert | Use DB UNIQUE or SETNX atomically |
| **Using mutable identifiers as dedup key** | Customer email can change; order reference can be reused | Use immutable UUIDs generated at request time |
| **Caching idempotency result in memory only** | Service restarts lose cache; duplicates processed after restart | Persist to DB or Redis |

---

## Walmart Scenarios

### Scenario 1: Payment Processing

```
Problem: Customer clicks "Pay" → network timeout → app retries → double charge?

Solution:
  1. App generates UUID (idempotency key) before first attempt
  2. POST /payments with Idempotency-Key: uuid-abc-123
  3. Payment service:
     a. Check idempotency_keys table for uuid-abc-123
     b. Not found → charge card → store result → return
     c. Found → return stored result (no new charge)
  4. App receives same response on retry
  5. Customer charged exactly once ✓
```

### Scenario 2: Flash Sale Inventory Deduction

```
Problem: 10,000 concurrent requests for last 100 items.
         Each request does: read qty → if qty > 0 → qty - 1
         Race condition: multiple reads see qty=1, multiple deductions

Solution (idempotency + concurrency):
  1. Each purchase attempt has a unique purchase_request_id
  2. Atomic decrement: UPDATE inventory SET qty = qty - 1
                       WHERE sku='ITEM-X' AND qty > 0
                       AND NOT EXISTS (
                         SELECT 1 FROM inventory_reservations
                         WHERE purchase_request_id = 'req-abc'
                       )
  3. rows_affected = 0 → already processed or out of stock
  4. rows_affected = 1 → success, insert reservation record
```

### Scenario 3: Order State Machine

```
States: PENDING → PAYMENT_PROCESSING → PAID → FULFILLING → SHIPPED → DELIVERED

Idempotency for state transitions:
  UPDATE orders SET status = 'PAID'
  WHERE id = 123 AND status = 'PAYMENT_PROCESSING'  ← guard

  If rows_affected = 0:
    - Already PAID → no-op, return success
    - Still PENDING → error, payment wasn't confirmed yet
    - CANCELLED → error, order was cancelled before payment
```

### Scenario 4: Supplier Inventory Sync (Event Consumer)

```
Problem: Supplier sends inventory update events. Kafka retries on consumer crash.
         Processing twice means inventory count is wrong.

Solution:
  Consumer receives event { event_id: "sup-evt-789", sku: "ITEM-X", qty: 500 }

  BEGIN TRANSACTION
    INSERT INTO processed_events (event_id) VALUES ('sup-evt-789')
      ON CONFLICT DO NOTHING  ← returns 0 rows if duplicate

    IF rows_affected > 0:
      UPDATE inventory SET qty = 500 WHERE sku = 'ITEM-X'
  COMMIT

  Result: qty is set to 500 exactly once, regardless of delivery count
```

---

## Interview Tip

> **Ready-to-say answer on idempotency:**
>
> *"Idempotency is non-negotiable whenever you have retries — and in distributed systems, retries are guaranteed. My go-to pattern is the idempotency key: the client generates a UUID per logical operation, includes it as a header, and the server stores a key-to-result mapping in the same database transaction as the operation. On duplicate, we return the stored result without re-executing. For DB-level enforcement, I lean on UNIQUE constraints with `ON CONFLICT DO NOTHING` — clean and atomic. For message consumers on Kafka, I combine a processed_events table with the business update in a single transaction. The tricky edge case is partial failures — what if we charged the card but crashed before storing the idempotency key? The answer is always: idempotency must be stored in the same transaction as the operation itself."*
>
> **Follow-up they'll ask:** "What if the idempotency key store itself fails?"
>
> *"If we can't write to the idempotency store, we can't guarantee dedup. In that case, I'd rather fail fast with a 503 than risk a double charge. The caller can retry with the same key once the store is healthy. For critical payment paths specifically, I'd use the transaction database itself as the idempotency store — never a separate system that could fail independently."*

---

**Navigation:** [README ←](./README.md) | [02 Concurrency →](./02-concurrency.md)
