# Distributed Systems Rigor

> "Distributed systems fail in ways that local systems never do. Idempotency, concurrency control, and data integrity are not optional — they are the difference between systems that appear to work and systems that actually do."

[← Back to Architecture Patterns](../README.md)

---

## What Is Distributed Systems Rigor?

- The practice of building systems that are **correct under failure, retries, and concurrent access**
- Three pillars that Walmart (and FAANG) probe at senior/principal level:
  - **Idempotency** — same operation, many times = same result
  - **Concurrency** — many clients, shared state, no corruption
  - **Data Integrity** — data stays consistent across services, failures, and partitions

---

## Navigation

| # | Topic | File | What You'll Learn |
|---|-------|------|-------------------|
| 1 | Idempotency | [01-idempotency.md](./01-idempotency.md) | Dedup keys, DB constraints, Redis SETNX, exactly-once patterns |
| 2 | Concurrency | [02-concurrency.md](./02-concurrency.md) | Optimistic/pessimistic locking, MVCC, distributed locks, deadlocks |
| 3 | Data Integrity | [03-data-integrity.md](./03-data-integrity.md) | ACID, CAP, isolation levels, 2PC, Saga, consistency models |
| 4 | Walmart Scenarios | [04-walmart-scenarios.md](./04-walmart-scenarios.md) | Flash sales, payments, inventory, price sync — ready-to-answer |

---

## Interactive View

> **[knowledge-graph.html](./knowledge-graph.html)** — Dark-themed interactive graph with progress tracking, drill-down panels, and interview cue cards.

---

## Quick Reference: The Three Pillars

| Pillar | Core Problem | Canonical Solution | Watch Out For |
|--------|-------------|-------------------|---------------|
| **Idempotency** | Duplicate messages / retries cause double effects | Idempotency key + DB dedup | TTL expiry, partial failures mid-transaction |
| **Concurrency** | Parallel writes corrupt shared state | Optimistic locking (low contention) or SELECT FOR UPDATE (high) | Deadlocks, starvation, distributed lock expiry |
| **Data Integrity** | State diverges across services during failures | Saga + compensating transactions | 2PC blocking, eventual consistency gaps |

---

## Decision Matrix: Which Pattern When?

```
CONCURRENCY LEVEL: Low ────────────────────────────── High
                        │                              │
                   Optimistic                    Pessimistic
                   Locking                       Locking
                   (version++)                   (SELECT FOR
                                                 UPDATE)

TRANSACTION SCOPE: Single DB ──────────────── Multi-Service
                        │                              │
                      ACID                         Saga +
                    Transaction               Compensating TXs

DELIVERY SEMANTICS: At-Most-Once ────────── At-Least-Once
                        │                              │
                  Accept loss                  Idempotency key
                  (analytics)                  (payments, orders)
```

---

## The "Distributed Systems Rigor" Interview Tell

Interviewers probe this by asking:

- *"What happens if your payment service crashes after charging the card but before writing to the DB?"*
- *"How do you prevent double inventory deduction during a flash sale?"*
- *"If two users buy the last item simultaneously, which one wins?"*
- *"Your order saga fails at step 3 of 5. What happens?"*

**The answer pattern interviewers expect:**

1. Name the failure mode (network retry, race condition, partial commit)
2. Name the pattern (idempotency key, optimistic lock, saga)
3. Show the implementation detail (table schema, SQL, lock TTL)
4. Acknowledge the trade-off (performance cost, complexity, eventual consistency window)

---

## Interview Tip

> **Walmart-specific framing:** Walmart processes millions of transactions per day. At that scale, "we'll handle it manually" is not an answer. Every failure scenario must have a programmatic resolution path. Always close with **operational clarity** — how does an oncall engineer know when something went wrong, and what do they do?
>
> **Ready-to-say opener:**
> *"At Walmart's scale, correctness under failure isn't a feature — it's a baseline requirement. The three properties I always design for are idempotency (so retries are safe), concurrency control (so parallel writes don't corrupt state), and distributed data integrity (so service failures don't leave data inconsistent). Let me walk through how I'd implement each for this scenario..."*
