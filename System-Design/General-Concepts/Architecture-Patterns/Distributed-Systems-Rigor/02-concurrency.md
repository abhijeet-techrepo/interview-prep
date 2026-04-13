# Concurrency Control

> "Concurrency bugs are the silent killers of distributed systems. They're invisible in unit tests, rare in staging, and catastrophic in production under load."

[← Back to Distributed Systems Rigor](./README.md) | **Related:** [Idempotency](./01-idempotency.md) · [Data Integrity](./03-data-integrity.md)

---

## Quick Revision Mind Map

```
                        CONCURRENCY CONTROL
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
     RACE CONDITIONS      LOCK STRATEGIES      ADVANCED TOPICS
     ─────────────────    ───────────────      ───────────────
     Lost Update          Optimistic           MVCC
     Dirty Read           Pessimistic          Distributed Locks
     Non-Repeatable Read  Advisory Locks       Deadlock Prevention
     Phantom Read                              CAS Operations
     Write Skew

          Decision Matrix:
          Low Contention   →  Optimistic (version++)
          High Contention  →  Pessimistic (SELECT FOR UPDATE)
          Cross-Service    →  Distributed Lock (Redis/ZK)
          Read-Heavy       →  MVCC (snapshot isolation)
```

---

## Jump to Section

| Concept | One-Liner | Details |
|---------|-----------|---------|
| **Race Condition Taxonomy** | 5 failure modes every engineer must name | [→](#race-condition-taxonomy) |
| **Optimistic Locking** | Version number; retry on conflict; low contention | [→](#optimistic-locking) |
| **Pessimistic Locking** | SELECT FOR UPDATE; block others; high contention | [→](#pessimistic-locking) |
| **MVCC** | Postgres/MySQL snapshot isolation; reads never block | [→](#mvcc-multi-version-concurrency-control) |
| **Distributed Locks** | Redis SETNX / Redlock / ZooKeeper for cross-service | [→](#distributed-locks) |
| **Deadlock Prevention** | Lock ordering, timeouts, detection | [→](#deadlock-prevention) |
| **CAS Operations** | Compare-and-Swap — atomic without locks | [→](#compare-and-swap-cas) |
| **Decision Matrix** | When to use which strategy | [→](#decision-matrix) |

---

## Race Condition Taxonomy

Understanding the failure mode is step 1. Name it in the interview, then say how you fix it.

### 1. Lost Update (Most Common)

```
Thread A: reads qty = 10
Thread B: reads qty = 10
Thread A: writes qty = 9  (sold 1 item)
Thread B: writes qty = 9  (sold 1 item)  ← B's write overwrites A's — only 1 item sold but 2 customers charged!

Result: qty should be 8, but it's 9. One sale is "lost."
```

**Fix:** Optimistic locking (version check) or `SELECT FOR UPDATE`

### 2. Dirty Read

```
Thread A: BEGIN TX; UPDATE account SET balance = 0 (paying all out)
Thread B: reads balance = 0  ← sees uncommitted data!
Thread A: ROLLBACK (payment failed)
Thread B: acted on a value that was rolled back

Result: Thread B made a decision based on data that never committed.
```

**Fix:** Read Committed isolation level (default in Postgres, MySQL)

### 3. Non-Repeatable Read

```
Thread A: reads order status = 'PENDING' (first read)
Thread B: updates order status = 'CANCELLED'
Thread A: reads order status = 'CANCELLED' (second read in same TX) ← different!

Result: Same transaction gets different answers to the same query.
```

**Fix:** Repeatable Read isolation level

### 4. Phantom Read

```
Thread A: SELECT COUNT(*) FROM inventory WHERE qty > 0  → returns 5
Thread B: INSERT INTO inventory (sku='NEW-ITEM', qty=50)
Thread A: SELECT COUNT(*) FROM inventory WHERE qty > 0  → returns 6 ← new row appeared!

Result: Set of rows changes within a transaction.
```

**Fix:** Serializable isolation level or explicit range locks

### 5. Write Skew (Hardest to Spot)

```
Constraint: At least 1 doctor must be on call.
Doctor A: reads → 2 doctors on call → leaves (updates own record to OFF)
Doctor B: reads → 2 doctors on call → leaves (updates own record to OFF)
Both reads were valid; both writes were valid individually;
Combined result: 0 doctors on call — CONSTRAINT VIOLATED.

Result: Each transaction sees valid state, but combined they violate an invariant.
```

**Fix:** Serializable isolation or explicit `SELECT FOR UPDATE` on the shared constraint

---

## Optimistic Locking

**Philosophy:** "Conflict is rare. Let everyone read freely. At write time, verify nothing changed."

**Mechanism:** Version number (or ETag, hash) on the record. Increment on every write. Include `WHERE version = N` in updates.

```sql
-- Schema
CREATE TABLE inventory (
    sku      VARCHAR(64) PRIMARY KEY,
    qty      INT NOT NULL,
    version  BIGINT NOT NULL DEFAULT 0   -- ← The key addition
);

-- Read phase (no lock acquired)
SELECT sku, qty, version FROM inventory WHERE sku = 'ITEM-X';
-- Returns: qty=10, version=5

-- Write phase (fails if someone else updated)
UPDATE inventory
SET    qty = 9, version = 6
WHERE  sku = 'ITEM-X'
AND    version = 5;              -- ← Optimistic check

-- rows_affected = 1: success
-- rows_affected = 0: conflict → retry with fresh read
```

**Java/JPA implementation (Spring Data):**

```java
@Entity
public class Inventory {
    @Id
    private String sku;

    private int qty;

    @Version
    private Long version;  // JPA handles this automatically
}

// Service layer
@Transactional
public void sellItem(String sku) {
    Inventory inv = inventoryRepo.findById(sku).orElseThrow();
    if (inv.getQty() <= 0) throw new OutOfStockException();
    inv.setQty(inv.getQty() - 1);
    // JPA adds: WHERE version = N AND sku = X
    // Throws OptimisticLockException if rows_affected = 0
}
```

**With retry logic:**

```java
@Retryable(
    value = OptimisticLockException.class,
    maxAttempts = 3,
    backoff = @Backoff(delay = 50, multiplier = 2)
)
@Transactional
public void sellItem(String sku) {
    Inventory inv = inventoryRepo.findById(sku).orElseThrow();
    if (inv.getQty() <= 0) throw new OutOfStockException();
    inv.setQty(inv.getQty() - 1);
}
```

**When optimistic locking is right:**

| Criterion | Optimistic Wins If... |
|-----------|----------------------|
| Contention level | Low — conflicts are rare |
| Operation duration | Short — read and write complete quickly |
| Read:Write ratio | High reads, low writes |
| Failure handling | Retry is acceptable |
| Example workloads | User profile updates, most CRUD |

**When optimistic locking fails:**

```
Flash sale: 10,000 users fighting for 100 items
→ Optimistic locking causes 9,900 retries, 9,900 more retries, stampede
→ DB becomes overwhelmed by failed version checks
→ Use pessimistic locking or queue-based serialization instead
```

---

## Pessimistic Locking

**Philosophy:** "Conflict is likely. Lock the row upfront. Make others wait."

**Mechanism:** `SELECT ... FOR UPDATE` acquires a row-level lock. Other transactions that try to lock the same rows block until the holder commits/rolls back.

```sql
-- Pessimistic inventory deduction
BEGIN;

SELECT qty FROM inventory
WHERE  sku = 'ITEM-X'
FOR UPDATE;               -- ← Row locked. Other transactions block here.

-- Now we have exclusive access
UPDATE inventory SET qty = qty - 1 WHERE sku = 'ITEM-X' AND qty > 0;

COMMIT;                   -- ← Lock released, next waiter proceeds
```

**Variants:**

| Clause | Behavior |
|--------|----------|
| `FOR UPDATE` | Exclusive lock — no readers or writers |
| `FOR SHARE` | Shared lock — readers ok, writers block |
| `FOR UPDATE NOWAIT` | Lock or fail immediately (no blocking) |
| `FOR UPDATE SKIP LOCKED` | Skip already-locked rows (job queue pattern) |

**Skip Locked — the job queue pattern (great for Walmart):**

```sql
-- Multiple workers pulling from an order queue without conflicts
BEGIN;

SELECT id, order_data
FROM   pending_orders
WHERE  status = 'QUEUED'
LIMIT  10
FOR UPDATE SKIP LOCKED;   -- ← Only grab rows not locked by other workers

UPDATE pending_orders SET status = 'PROCESSING' WHERE id IN (...);
COMMIT;
```

This pattern powers distributed task queues (like competing Kafka consumers, but in SQL).

**When pessimistic locking is right:**

| Criterion | Pessimistic Wins If... |
|-----------|----------------------|
| Contention level | High — conflicts are frequent |
| Operation duration | Medium — acceptable to wait briefly |
| Failure handling | Retry cost is high / retry storms likely |
| Consistency requirement | Must not fail silently |
| Example workloads | Flash sales, financial debits, seat booking |

**Watch out for:** Pessimistic locking with long-running transactions = everything waits. Keep transactions short.

---

## MVCC (Multi-Version Concurrency Control)

**Philosophy:** "Readers and writers don't block each other. Readers see a consistent snapshot of the past."

**How Postgres implements it:**
- Each row has `xmin` (transaction ID that created it) and `xmax` (transaction ID that deleted it)
- A `SELECT` sees all rows where `xmin ≤ current_snapshot` and `xmax` is null or future
- Writers create new row versions; old versions are cleaned up by VACUUM
- Readers get a snapshot at transaction start — they never block writers

```
Timeline:
──────────────────────────────────────────────────────────────
TX 100: BEGIN; reads rows (snapshot at TX 100)
TX 101: BEGIN; UPDATES inventory qty = 50; COMMITS
TX 100: reads inventory → still sees qty = 60 (snapshot isolation)
TX 100: COMMITS

Result: TX 100 had a consistent view throughout, even though TX 101 committed a change.
```

**Isolation levels in Postgres (MVCC-based):**

| Level | Dirty Read | Non-Repeatable Read | Phantom Read | Write Skew | Default? |
|-------|-----------|--------------------|-----------|-----------|----|
| Read Uncommitted | Possible (treated as RC in PG) | — | — | — | No |
| **Read Committed** | Protected | Possible | Possible | Possible | **Yes (PG default)** |
| Repeatable Read | Protected | Protected | Protected in PG | Possible | No |
| **Serializable** | Protected | Protected | Protected | Protected | No |

**MySQL defaults to Repeatable Read** (with gap locks to protect against phantoms).

**When MVCC shines:**

- Long-running reports/analytics that can't block OLTP writes
- Read-heavy workloads where read latency matters
- Audit queries that need point-in-time consistency

---

## Distributed Locks

When you need concurrency control **across multiple service instances** (not just within a single DB transaction).

### Redis-Based Locking

```java
// Spring Data Redis
public boolean acquireLock(String key, String requestId, Duration ttl) {
    Boolean acquired = redisTemplate.opsForValue()
        .setIfAbsent(key, requestId, ttl);
    return Boolean.TRUE.equals(acquired);
}

public void releaseLock(String key, String requestId) {
    // CRITICAL: Only release your own lock (use Lua script for atomicity)
    String script = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """;
    redisTemplate.execute(
        new DefaultRedisScript<>(script, Long.class),
        List.of(key), requestId
    );
}
```

**Usage pattern:**

```java
String lockKey = "lock:inventory:" + sku;
String requestId = UUID.randomUUID().toString();
boolean locked = acquireLock(lockKey, requestId, Duration.ofSeconds(10));

if (!locked) {
    throw new ResourceBusyException("Inventory update in progress, retry later");
}

try {
    // Critical section: only one instance runs this at a time
    updateInventory(sku, delta);
} finally {
    releaseLock(lockKey, requestId);  // Always release in finally
}
```

### The GC Pause Problem (Critical to Mention)

```
Timeline of failure:
1. Process A acquires Redis lock, TTL = 30s
2. Process A stops for GC for 35s
3. Redis expires the lock
4. Process B acquires lock, starts updating inventory
5. Process A resumes — now BOTH are in the critical section

Fix: Fencing Token
  - Lock returns a monotonically increasing token (e.g., Redis INCR value)
  - DB updates include: WHERE fence_token < current_token
  - Stale writes are rejected even if they execute late
```

### Redlock Algorithm (Multi-Node Redis)

For higher availability than single-node Redis:

```
1. Get current time T1
2. Acquire lock on N Redis nodes (N=5 for quorum of 3)
3. Lock is acquired if: at least N/2+1 nodes responded
   AND total time elapsed < lock TTL
4. If quorum not met: release all acquired locks, retry
5. Effective TTL = original TTL - elapsed time

Controversy: Martin Kleppmann argues Redlock is unsafe without fencing tokens.
Correct take: For non-critical coordination, Redlock is fine.
             For protecting critical data (money), use DB transactions.
```

### ZooKeeper Ephemeral Nodes (Leader Election / Distributed Coordination)

```java
// ZooKeeper creates sequential ephemeral nodes
// Lowest sequence number = lock holder
// Others watch the node ahead of them

/locks/inventory-ITEM-X/lock-0000000001  ← holder
/locks/inventory-ITEM-X/lock-0000000002  ← waiting, watching node 1
/locks/inventory-ITEM-X/lock-0000000003  ← waiting, watching node 2
```

**ZK vs Redis for locks:**

| Dimension | Redis | ZooKeeper |
|-----------|-------|-----------|
| Latency | Low (~1ms) | Higher (~5ms) |
| Consistency | AP (eventual) | CP (strict) |
| Lock expiry | TTL-based | Session-based (auto-expire on crash) |
| Fairness | FIFO not guaranteed | FIFO (sequential nodes) |
| Best for | Performance-critical | Strict correctness (leader election) |

---

## Deadlock Prevention

### What Is a Deadlock?

```
Transaction A holds lock on Order #100, wants lock on Payment #200
Transaction B holds lock on Payment #200, wants lock on Order #100

Both wait for each other forever → deadlock
```

### Prevention Strategies

**1. Consistent Lock Ordering (Best Practice)**

```java
// BAD: Each transaction locks in different order → deadlock possible
// TX A: lock(order_100) then lock(payment_200)
// TX B: lock(payment_200) then lock(order_100)

// GOOD: Always lock in the same order (e.g., by ID ascending)
public void processOrderAndPayment(Long orderId, Long paymentId) {
    Long firstLockId = Math.min(orderId, paymentId);
    Long secondLockId = Math.max(orderId, paymentId);

    // Always acquire in ascending ID order
    acquireLock(firstLockId);
    acquireLock(secondLockId);
    // ... process ...
}
```

**2. Lock Timeouts**

```sql
-- MySQL: InnoDB deadlock detection + timeout
SET innodb_lock_wait_timeout = 5;  -- give up after 5 seconds

-- Postgres
SET lock_timeout = '5s';

-- Java JDBC
connection.setQueryTimeout(5);     -- throws SQLTimeoutException
```

**3. Deadlock Detection and Retry**

```java
@Retryable(
    value = DeadlockLoserDataAccessException.class,
    maxAttempts = 3,
    backoff = @Backoff(delay = 100, multiplier = 1.5)
)
@Transactional
public void processTransaction(Long id) { ... }
```

**4. Keep Transactions Short**

```
BAD: BEGIN TX → external API call (200ms) → DB write → COMMIT
     (Lock held for 200ms+ → high deadlock probability)

GOOD: external API call first → BEGIN TX → DB write → COMMIT
     (Lock held for <10ms → minimal deadlock window)
```

---

## Compare-and-Swap (CAS)

**Philosophy:** Atomic check-and-update without holding a lock. Fails if value changed.

```
CAS(memory_location, expected_value, new_value):
  If current_value == expected_value:
    current_value = new_value
    return SUCCESS
  Else:
    return FAILURE (someone else changed it)
```

**Database equivalent:**

```sql
UPDATE inventory
SET    qty = 9, version = 6
WHERE  sku = 'ITEM-X'
AND    qty = 10           -- ← CAS check
AND    version = 5;       -- ← Version CAS check
```

**Redis atomic operations (no lock needed):**

```
INCR  counter       → Atomic increment (thread-safe by Redis single-thread model)
DECR  stock         → Atomic decrement
INCRBY, DECRBY      → Atomic add/subtract
GETSET old new      → Atomic get-and-set
```

**Java AtomicInteger (single-JVM):**

```java
AtomicInteger available = new AtomicInteger(100);

// Sell an item atomically
boolean sold = false;
while (!sold) {
    int current = available.get();
    if (current <= 0) throw new OutOfStockException();
    sold = available.compareAndSet(current, current - 1);  // CAS
}
```

---

## Decision Matrix

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    CONCURRENCY STRATEGY SELECTOR                         │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Is conflict frequent?                                                   │
│    NO  → OPTIMISTIC LOCKING (version number)                             │
│    YES → Is operation short (<50ms)?                                     │
│              YES → PESSIMISTIC LOCKING (SELECT FOR UPDATE)               │
│              NO  → QUEUE-BASED SERIALIZATION (one worker at a time)      │
│                                                                          │
│  Is the resource cross-service (different DBs)?                          │
│    YES → DISTRIBUTED LOCK (Redis for performance, ZK for correctness)   │
│                                                                          │
│  Are reads much more frequent than writes?                               │
│    YES → MVCC / SNAPSHOT ISOLATION (reads never block)                  │
│                                                                          │
│  Need atomic counter/flag without transaction overhead?                  │
│    YES → CAS (Redis INCR, AtomicInteger, DB conditional update)          │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

| Strategy | Contention | Cross-Service | Latency | Failure Handling |
|----------|-----------|--------------|---------|-----------------|
| Optimistic Locking | Low | ❌ (same DB) | Low | Retry on conflict |
| Pessimistic Locking | High | ❌ (same DB) | Medium | Wait or fail-fast |
| MVCC | Any | ❌ (same DB) | Low (reads) | No failure for reads |
| Redis Lock | High | ✅ | Low | TTL expiry, GC pause |
| ZooKeeper Lock | Medium | ✅ | Medium | Session expiry (safe) |
| CAS | Low | Partial | Very Low | Retry loop |

---

## Walmart Scenarios

### Flash Sale: 50,000 Concurrent Requests for 500 Items

```
Strategy: Pessimistic locking + queue to prevent retry storm

Option A: SELECT FOR UPDATE (simple, works up to moderate scale)
  BEGIN;
  SELECT qty FROM inventory WHERE sku='ITEM-X' FOR UPDATE;
  IF qty > 0: UPDATE inventory SET qty = qty - 1;
  COMMIT;
  → Works but becomes bottleneck at 50k RPS

Option B: Queue-based serialization (Walmart-scale preferred)
  1. Accept purchase requests into a Redis queue (LPUSH)
  2. Single consumer (or small pool) pops from queue, processes sequentially
  3. Returns reservation token to customer
  4. No lock contention; DB sees serialized writes

Option C: Pre-allocate slots (event-driven)
  1. Allocate N slots (item count) upfront in Redis
  2. DECR counter atomically → if result >= 0: won; if < 0: out of stock
  3. Queue won requests to DB for persistence
  → Redis single-thread = inherently serialized, very fast
```

### Concurrent Order Updates

```
Two customer service reps try to update the same order simultaneously:
  - Rep A: add item
  - Rep B: change shipping address

With optimistic locking:
  - Rep A reads: version=3
  - Rep B reads: version=3
  - Rep A writes: SET ..., version=4 WHERE version=3 → success
  - Rep B writes: SET ..., version=4 WHERE version=3 → FAILS (version is now 4)
  - Rep B's UI shows: "This order was modified. Please refresh and retry."

With pessimistic locking:
  - Rep A: SELECT FOR UPDATE → gets lock
  - Rep B: SELECT FOR UPDATE → blocks, waits
  - Rep A: commits update
  - Rep B: proceeds with fresh data
```

---

## Interview Tip

> **Ready-to-say answer on concurrency:**
>
> *"My decision process: first, I identify the race condition type — is it a lost update, write skew, or phantom? Then I choose the strategy based on contention level. For low-contention scenarios like user profile updates, optimistic locking with version numbers is my default — no lock overhead, retries handle the rare conflict. For high-contention like flash sale inventory, I reach for pessimistic locking with SELECT FOR UPDATE, or better, queue-based serialization so the DB never sees concurrent writes at all. When the critical section spans multiple services, I use Redis distributed locking, but always with a fencing token — the GC pause problem is real and Redis TTLs alone aren't sufficient for financial data."*
>
> **Follow-up they'll ask:** "How do you detect deadlocks?"
>
> *"Modern databases (MySQL InnoDB, Postgres) detect deadlocks automatically and kill one of the transactions. The fix is prevention, not detection: always acquire locks in a consistent global order — e.g., always lock the lower resource ID first. And keep transactions short — the shorter the lock hold time, the lower the deadlock probability. For application-level, I use @Retryable on DeadlockLoserDataAccessException with exponential backoff."*

---

**Navigation:** [01 Idempotency ←](./01-idempotency.md) | [03 Data Integrity →](./03-data-integrity.md)
