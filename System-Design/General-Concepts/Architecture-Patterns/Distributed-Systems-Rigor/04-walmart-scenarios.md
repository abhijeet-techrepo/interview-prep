# Walmart Interview Scenarios

> "Walmart-specific questions are always grounded in their reality: millions of transactions per day, global supply chain, Black Friday scale, and zero tolerance for payment or inventory errors."

[← Back to Distributed Systems Rigor](./README.md)

---

## How Walmart Interviews on This Topic

Walmart senior/principal interviews in distributed systems rigor typically follow this pattern:

1. **System design scenario** → design X at Walmart scale
2. **Deep-dive probe** → "what happens when Y fails?"
3. **Trade-off challenge** → "why not just use Z instead?"
4. **Operational question** → "how does oncall know something is wrong?"

**Expected answer depth:** Not just "use idempotency keys" but HOW, WHERE, schema design, failure modes, monitoring.

---

## Scenario Library

| # | Scenario | Primary Pillar | Difficulty |
|---|---------|---------------|------------|
| [1](#scenario-1-flash-sale-inventory-deduction) | Flash sale: 50k requests for 500 items | Concurrency + Idempotency | ⭐⭐⭐ |
| [2](#scenario-2-payment-processing--refund-safety) | Payment + refund safety | Idempotency + Data Integrity | ⭐⭐⭐ |
| [3](#scenario-3-order-saga-across-4-services) | Order saga across 4 services | Data Integrity | ⭐⭐⭐⭐ |
| [4](#scenario-4-global-inventory-sync) | Global inventory sync from suppliers | Idempotency + Concurrency | ⭐⭐⭐ |
| [5](#scenario-5-price-update-across-regions) | Price update consistency worldwide | Data Integrity + CAP | ⭐⭐⭐ |
| [6](#scenario-6-duplicate-order-prevention) | Customer double-clicks "Place Order" | Idempotency | ⭐⭐ |
| [7](#scenario-7-cart-checkout-race-condition) | Two sessions buying last item | Concurrency | ⭐⭐⭐ |
| [8](#scenario-8-supplier-invoice-reconciliation) | Supplier sends invoice twice | Idempotency + Data Integrity | ⭐⭐⭐⭐ |

---

## Scenario 1: Flash Sale Inventory Deduction

**The question:** "Design inventory deduction for a Black Friday flash sale. 50,000 concurrent users want to buy 500 units of an item. How do you prevent overselling, ensure every deduction is counted once, and keep the system fast?"

### The Failure Modes to Name First

```
1. Lost Update:
   - User A reads qty=1, User B reads qty=1
   - Both pass the "qty > 0" check
   - Both deduct → qty=-1 (oversold!)

2. Duplicate Deduction (Idempotency Failure):
   - User successfully deducts but crashes before receiving confirmation
   - User retries → deducts again

3. Thundering Herd:
   - 50,000 DB connections all locking the same row → DB collapses
```

### The Solution

```
Tier 1: Redis Atomic Decrement (Fast path — absorb traffic)
───────────────────────────────────────────────────────────
  1. Load inventory qty into Redis key: SET inv:ITEM-X 500
  2. On each purchase attempt:
       result = DECR inv:ITEM-X
       IF result >= 0:  emit "RESERVATION_CLAIMED" to Kafka
       IF result < 0:   return 429 "Sold Out"
                        INCR inv:ITEM-X  ← restore overshoot

  Why: Redis is single-threaded → DECR is atomic by definition.
       No locking needed. Handles 100,000+ ops/sec.

Tier 2: Kafka Consumer → DB Write (Durability path)
─────────────────────────────────────────────────────────────
  Consumer reads RESERVATION_CLAIMED events
  For each event (with purchase_request_id):
    BEGIN TRANSACTION
      INSERT INTO reservations (purchase_req_id, sku, qty)
      ON CONFLICT (purchase_req_id) DO NOTHING    ← Idempotency
      UPDATE inventory SET qty = qty - 1 WHERE sku = 'ITEM-X' AND qty > 0
    COMMIT

  If rows_affected = 0 on UPDATE: inventory reconciliation alert

Tier 3: Reconciliation Job (Correctness guarantee)
────────────────────────────────────────────────────────────
  Every 60 seconds:
    redis_count = GET inv:ITEM-X
    db_count = SELECT qty FROM inventory WHERE sku = 'ITEM-X'
    IF ABS(redis_count - db_count) > THRESHOLD: alert + sync
```

### Key Trade-offs to Verbalize

| Decision | Trade-off |
|----------|-----------|
| Redis as fast path | Risk: Redis restarts lose in-flight count → Kafka + DB are source of truth |
| Kafka buffer | Risk: Consumer lag → reservation confirmed but DB not yet updated → monitoring needed |
| Reconciliation job | Catches Redis/DB drift; adds operational complexity |
| Not using DB SELECT FOR UPDATE | Avoids lock contention at 50k RPS; risk: Redis count could drift |

### Operational Visibility

```
Metrics to emit:
  - inventory.deductions.success (counter, per SKU)
  - inventory.deductions.rejected.soldout (counter)
  - inventory.deductions.duplicate.skipped (counter)
  - inventory.redis_db_drift (gauge, per SKU)

Alert: redis_db_drift > 5 for any SKU → page oncall
```

---

## Scenario 2: Payment Processing & Refund Safety

**The question:** "A customer clicks 'Pay' and gets a timeout. They click again. How do you guarantee they're not charged twice? And how do you guarantee that if we refund, they get the refund exactly once?"

### Charge Idempotency

```
Flow:
  1. Frontend generates idempotency_key = UUID before first attempt
     (stores in localStorage / session)
  2. POST /payments
     Headers: Idempotency-Key: uuid-abc-123
     Body: { order_id: 5001, amount: 99.99, card_token: "..." }

  Server:
  ─────────
  BEGIN TRANSACTION
    SELECT * FROM idempotency_keys
    WHERE  key = 'uuid-abc-123' AND operation = 'charge'
    FOR UPDATE;

    IF found:
      COMMIT;
      RETURN stored_response;  ← same response, no new charge

    charge_result = payment_gateway.charge(amount, card_token);

    INSERT INTO idempotency_keys
      (key, operation, response, expires_at)
    VALUES
      ('uuid-abc-123', 'charge', charge_result, NOW() + INTERVAL 7 DAYS);

    INSERT INTO payments
      (order_id, gateway_charge_id, amount, status)
    VALUES
      (5001, charge_result.id, 99.99, 'COMPLETED');
  COMMIT;

Why store in same transaction:
  If we charge the card but crash before storing idempotency key,
  we'd charge again on retry. Storing in the same TX prevents this.
  (The idempotency store and payment record are in the same DB → 1 TX = atomic)
```

### Refund Idempotency

```
Problem: Customer requests refund. We call Stripe/Braintree.
         Response times out. We retry → double refund?

Pattern: Same idempotency key approach + check existing refund status

  refund_idempotency_key = "refund:" + orderId
  → Customer can request refund multiple times, always same outcome

  BEGIN TRANSACTION
    SELECT refund_status FROM payments WHERE order_id = 5001 FOR UPDATE;

    IF refund_status IN ('REFUNDED', 'REFUND_PENDING'):
      COMMIT;
      RETURN "Refund already processed";

    UPDATE payments SET refund_status = 'REFUND_PENDING' WHERE order_id = 5001;
  COMMIT;

  result = payment_gateway.refund(gateway_charge_id,
                                  idempotency_key: "refund:" + orderId)
  -- Note: Stripe/Braintree natively support idempotency keys on refund API

  UPDATE payments SET refund_status = 'REFUNDED' WHERE order_id = 5001;
```

### What If the Payment Gateway Returns Ambiguous Response?

```
Timeout / 5xx from gateway:
  - We don't know if charge succeeded
  - DO NOT assume failure
  - DO NOT retry immediately

Correct approach:
  1. Store payment status = 'UNCERTAIN'
  2. Async reconciliation job: query gateway for charge status by reference ID
  3. Update status based on gateway's authoritative answer
  4. If CHARGED: proceed with order fulfillment
  5. If NOT_CHARGED: mark order PAYMENT_FAILED, notify customer

This avoids double-charging while not assuming failure.
```

---

## Scenario 3: Order Saga Across 4 Services

**The question:** "Walk me through how you'd implement placing an order that involves: inventory reservation, payment charging, shipping label creation, and order confirmation email — across 4 separate services. What happens when step 3 fails?"

### The Saga Design

```
Services:
  InventoryService  → reserve items
  PaymentService    → charge customer
  ShippingService   → create shipment
  NotificationService → send email

Orchestrator: OrderSaga (persisted state machine)

Happy Path:
  OrderSaga.start(orderId)
    ├── reserveInventory(orderId, items)
    │     InventoryService: UPDATE inventory WHERE qty > 0
    │     Publish: InventoryReserved{orderId, items, reservationId}
    │
    ├── chargePayment(orderId, amount)  ← triggered by InventoryReserved
    │     PaymentService: charge card with idempotency key = orderId
    │     Publish: PaymentCharged{orderId, chargeId}
    │
    ├── createShipment(orderId)  ← triggered by PaymentCharged
    │     ShippingService: generate label, create shipment record
    │     Publish: ShipmentCreated{orderId, trackingNumber}
    │
    └── sendConfirmation(orderId)  ← triggered by ShipmentCreated
          NotificationService: send order confirmation email
          Publish: OrderConfirmed{orderId}

OrderSaga.complete(orderId)
```

### Failure at Step 3 (ShippingService Fails)

```
State: inventory reserved ✓, payment charged ✓, shipping FAILED

OrderSaga detects ShipmentFailed event:
  ├── Compensate Step 2: PaymentService.refund(orderId)
  │     → Idempotent: uses "refund:orderId" as idempotency key
  │     → Updates payment status to REFUNDED
  │
  ├── Compensate Step 1: InventoryService.release(orderId, items)
  │     → Idempotent: checks reservation status before releasing
  │     → Increments inventory back
  │
  └── Cancel Order: OrderService.cancel(orderId)
        → Notifies customer: "Sorry, shipping not available"
        → OrderSaga.end(CANCELLED)
```

### Saga Persistence Schema

```sql
CREATE TABLE saga_state (
    saga_id         VARCHAR(128) PRIMARY KEY,
    saga_type       VARCHAR(64)  NOT NULL,       -- 'OrderSaga'
    current_step    VARCHAR(64)  NOT NULL,       -- 'INVENTORY_RESERVED'
    status          VARCHAR(32)  NOT NULL,       -- 'IN_PROGRESS', 'COMPLETED', 'COMPENSATING', 'FAILED'
    payload         JSONB        NOT NULL,       -- full context
    last_updated    TIMESTAMP    DEFAULT NOW(),
    retry_count     INT          DEFAULT 0,
    INDEX idx_status_updated (status, last_updated)
);

-- Stuck saga detection:
SELECT * FROM saga_state
WHERE status IN ('IN_PROGRESS', 'COMPENSATING')
AND   last_updated < NOW() - INTERVAL 30 MINUTE;
-- Alert: these need manual review or retry
```

### The Interview Follow-up: "What if the compensation also fails?"

```
Answer:
  1. Compensation is retried with exponential backoff (configured retry policy)
  2. If compensation fails N times: saga moves to COMPENSATION_FAILED status
  3. Alert fires → oncall engineer gets paged
  4. Dead Letter Queue captures the failed compensation event
  5. Manual intervention: human reviews context and processes refund manually

Key point: "We never lose track. The system is noisily stuck, not silently corrupted."

Additional safeguard for payment:
  Even if our saga fails to trigger refund, payment gateway has dispute resolution.
  We have 24 hours to initiate refund before customer files chargeback.
  Monitoring ensures we catch stuck sagas in <15 minutes.
```

---

## Scenario 4: Global Inventory Sync

**The question:** "Walmart has 10,500 stores. Each store scans items out of inventory in real-time. A central system aggregates all inventory. Supplier systems also update inventory counts. How do you handle duplicate or out-of-order inventory updates?"

### Problem Statement

```
Event streams:
  Store POS → inventory_updates topic (100k events/sec)
  Supplier portal → supplier_inventory topic (50k events/sec)
  Online orders → order_reservations topic (30k events/sec)

Challenge:
  - Events may be redelivered (at-least-once Kafka delivery)
  - Events may arrive out of order (network delays, partition reassignment)
  - Same inventory count may be updated concurrently from different sources
```

### Solution Architecture

```
Pattern: Last-Write-Wins with version vectors (for absolute counts)
         Idempotent event processing (for delta updates)

For Absolute Counts (supplier sends "ITEM-X qty = 5000"):
  UPDATE inventory
  SET    qty = 5000, source_version = 'sup-v-1234', updated_at = NOW()
  WHERE  sku = 'ITEM-X'
  AND    (source_version IS NULL OR source_version < 'sup-v-1234')

  -- If source_version is monotonically increasing: only apply if newer
  -- Out-of-order events are safely rejected

For Delta Updates (store POS sends "ITEM-X sold 1"):
  Each sale event has unique event_id (POS_transaction_id):

  BEGIN TRANSACTION
    INSERT INTO processed_pos_events (event_id, sku, delta)
    VALUES ('pos-tx-99887', 'ITEM-X', -1)
    ON CONFLICT (event_id) DO NOTHING;

    IF rows_affected > 0:
      UPDATE inventory SET qty = qty - 1 WHERE sku = 'ITEM-X';
  COMMIT;

  -- Duplicate event: ON CONFLICT skips both the insert and the decrement
  -- Idempotent by construction
```

### Out-of-Order Handling

```
Problem: Store sends events at T=1 and T=3. Due to network, T=3 arrives first.
         If T=3 says qty=4990 and T=1 says qty=4991, applying in order: 4991
         Applying out of order: 4991 (wrong — T=1 is stale)

Solution: Use event sequence numbers from source:
  Event schema: { store_id: "WMT-TX-001", seq: 12345, sku: "ITEM-X", delta: -1 }

  Track: last processed sequence per store:
  UPDATE inventory_store_offsets
  SET    last_seq = 12345
  WHERE  store_id = 'WMT-TX-001'
  AND    last_seq < 12345;  ← Only update if this is newer

  Apply delta only if seq > last_seq in store_offsets
  Reject/queue events with seq <= last known seq (stale or duplicate)
```

---

## Scenario 5: Price Update Across Regions

**The question:** "Walmart operates in 24 countries. A price change for a product must be visible across all regions. How do you propagate this change while maintaining data integrity? What's the consistency model?"

### CAP Analysis

```
Requirements:
  - Price accuracy for checkout (strong consistency at purchase time)
  - Price display for browsing (eventual consistency acceptable)
  - Price change must propagate within 60 seconds globally

CAP decision:
  - During network partition:
    - Display price (AP): serve stale cached price, show warning banner
    - Checkout price (CP): use primary, accept latency degradation
  → Split consistency: eventual for display, strong for transaction
```

### Architecture

```
Write Path:
  1. Admin updates price in Primary DB (US-East)
  2. Write published to price_changes Kafka topic
  3. Regional consumers (EU, APAC, LatAm) consume and update regional DBs
  4. CDN cache invalidated per SKU
  5. Convergence SLA: 60 seconds

Read Path:
  Display:  Regional DB replica → CDN cache (eventual, fast, stale OK)
  Checkout: Primary DB or sync replica (strong, accurate at time of purchase)

Idempotency on propagation:
  Each price change has:
    { change_id: UUID, sku: "ITEM-X", new_price: 29.99, effective_at: "2026-04-13T10:00:00Z" }

  Regional consumer:
    UPDATE products
    SET    price = 29.99, last_change_id = change_id
    WHERE  sku = 'ITEM-X'
    AND    (last_change_id IS NULL OR effective_at > current_effective_at)
    → Out-of-order and duplicate price events safely rejected
```

### The Stale Price Edge Case

```
Customer sees browsing price: $29.99 (CDN cached, 30 seconds old)
Price changes at 10:00:00 to $34.99
Customer adds to cart at 10:00:20 (still cached $29.99)
Customer checks out at 10:00:40 (cache refreshed, checkout sees $34.99)

Options:
  A: Charge at browsing price ($29.99) → price lock for 60 seconds in cart
  B: Charge at checkout price ($34.99) → show price change warning
  C: Lock cart price at "add to cart" time → store price snapshot in cart

Walmart-appropriate answer: B + notification
  "Price has changed since you added this item. Current price: $34.99"
  → Customer can choose to proceed or remove
  → No silent discrepancy; transparent to customer
```

---

## Scenario 6: Duplicate Order Prevention

**The question:** "Customer double-clicks 'Place Order'. Both clicks hit the API within 50ms. How do you guarantee exactly one order is created?"

### Simple and Correct Solution

```
Frontend:
  - Disable button on first click (JavaScript: button.disabled = true)
  - Generate order_request_id = UUID on page load (not on click)
  - Send POST /orders with X-Order-Request-ID: uuid-abc header

Backend:
  CREATE TABLE orders (
    id               BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_request_id VARCHAR(128) UNIQUE NOT NULL,  ← natural dedup
    customer_id      BIGINT NOT NULL,
    status           VARCHAR(32) DEFAULT 'PENDING',
    created_at       TIMESTAMP DEFAULT NOW()
  );

  INSERT INTO orders (order_request_id, customer_id, ...)
  VALUES ('uuid-abc', 1001, ...)
  ON CONFLICT (order_request_id) DO NOTHING
  RETURNING id;

  If rows_inserted = 0:
    SELECT id FROM orders WHERE order_request_id = 'uuid-abc'
    Return existing order (same response as if first creation)
```

### Why "Disable Button" Is Not Enough

```
Frontend disable: good UX, not security
  - Slow network: two tab-opens, both click simultaneously
  - Race condition on button state
  - Mobile: double-tap before JS handler fires

Always enforce at the server level too.
→ Both frontend AND backend dedup = defense in depth
```

---

## Scenario 7: Cart Checkout Race Condition

**The question:** "Two users both try to buy the last unit of an item simultaneously. One browser tab from user A and one from user B. How do you ensure exactly one succeeds?"

### The Answer Pattern

```
Step 1: Identify this as a LOST UPDATE problem
  - Both read qty = 1
  - Both pass qty > 0 check
  - Both update qty = 0
  - Result: -1 sold items (oversold)

Step 2: Choose the right lock strategy
  Contention: HIGH (both concurrent for same item)
  → Pessimistic locking

Step 3: Implementation

  BEGIN TRANSACTION
    SELECT qty FROM inventory
    WHERE  sku = 'ITEM-X'
    FOR UPDATE;              ← Lock this row

    IF qty <= 0:
      ROLLBACK;
      RETURN 409 "Out of Stock"

    UPDATE inventory SET qty = qty - 1 WHERE sku = 'ITEM-X';

    INSERT INTO reservations (customer_id, sku, qty)
    VALUES (customer_id, 'ITEM-X', 1);
  COMMIT;                    ← Lock released

  Thread A commits → Thread B unblocks → Thread B sees qty=0 → returns 409
  Exactly one customer gets the item. ✓

Step 4: Address the scale concern
  "For a single item, pessimistic lock is fine. For Black Friday with
  50k concurrent requests on 500 items, I'd pre-allocate slots in Redis
  with DECR and use queue-based serialization — see Scenario 1."
```

---

## Scenario 8: Supplier Invoice Reconciliation

**The question:** "A supplier submits an invoice. Your system processes it and updates accounts payable. The supplier's system has a bug and submits the same invoice 3 more times. How do you ensure Walmart doesn't pay the supplier 4 times?"

### Full Solution

```sql
-- Accounts payable table
CREATE TABLE invoices (
    id                BIGINT PRIMARY KEY AUTO_INCREMENT,
    supplier_id       BIGINT NOT NULL,
    supplier_invoice_id VARCHAR(128) NOT NULL,  -- Supplier's own reference
    amount            DECIMAL(12,2) NOT NULL,
    status            VARCHAR(32) DEFAULT 'PENDING',
    processed_at      TIMESTAMP,
    UNIQUE KEY uk_supplier_invoice (supplier_id, supplier_invoice_id)  ← dedup
);

CREATE TABLE invoice_payments (
    id          BIGINT PRIMARY KEY AUTO_INCREMENT,
    invoice_id  BIGINT NOT NULL REFERENCES invoices(id),
    payment_id  VARCHAR(128) UNIQUE NOT NULL,  ← payment dedup
    amount      DECIMAL(12,2) NOT NULL,
    paid_at     TIMESTAMP DEFAULT NOW()
);
```

```java
@Transactional
public InvoiceResponse processInvoice(SupplierInvoiceRequest req) {
    // Dedup at invoice level
    try {
        Invoice invoice = invoiceRepo.save(new Invoice(
            req.getSupplierId(),
            req.getSupplierInvoiceId(),
            req.getAmount()
        ));
        // Proceed to payment
        schedulePayment(invoice);
        return InvoiceResponse.created(invoice.getId());

    } catch (DataIntegrityViolationException e) {
        // Duplicate invoice — return existing
        Invoice existing = invoiceRepo.findBySupplierAndInvoiceId(
            req.getSupplierId(), req.getSupplierInvoiceId()
        );
        return InvoiceResponse.duplicate(existing.getId(), existing.getStatus());
    }
}
```

### Operational Monitoring

```
Metrics:
  - invoices.received (counter, per supplier)
  - invoices.duplicates.rejected (counter, per supplier)
  - invoices.paid (counter, amount)

Alert: invoices.duplicates.rejected for supplier X > 10 in 1 hour
  → Supplier system may have a bug → notify supplier integration team
```

---

## Common Interview Follow-ups & Answers

| Follow-up | Answer |
|-----------|--------|
| "What's the TTL for your idempotency keys?" | "Typically 7 days for payments (business retry window), 24h for orders, shorter for high-volume events. Always have a cleanup job." |
| "What if your idempotency store goes down?" | "Fail the request with 503. Better to reject than risk double-processing. The caller retries with the same key when the store recovers." |
| "How do you handle compensation failures?" | "Retry with backoff. After N failures, saga enters STUCK state, oncall is paged. Dead letter queue captures context for manual replay." |
| "What's your consistency model for inventory?" | "Eventual for browsing (cached regional replica), strong for checkout (primary DB with pessimistic lock for last-mile reservation)." |
| "How do you test these patterns?" | "Chaos engineering: kill services mid-saga. Retry storm tests: flood with duplicates. Compare idempotency key counts to actual transactions daily." |
| "Why not just use 2PC?" | "Coordinator SPOF, blocking protocol, poor performance under high load. Saga trades atomicity for availability — acceptable with good compensation and monitoring." |

---

## Interview Tip

> **The Walmart-specific framing that scores points:**
>
> *"At Walmart's scale, I don't just design for correctness — I design for operational visibility. For every pattern I implement (idempotency keys, saga states, distributed locks), I also define: what metric tells me it's working, what alert fires when it's not, and what the oncall runbook says to do. Distributed systems fail. The question isn't if — it's whether we catch the failure in 5 minutes or find out from a customer complaint 48 hours later."*
>
> **Closing statement for any scenario:**
>
> *"Three things I always validate after designing a solution: (1) what's the worst-case failure scenario and does my design handle it gracefully? (2) is every operation idempotent such that any retry is safe? (3) does oncall have enough signal to know something is wrong before a customer does?"*

---

**Navigation:** [03 Data Integrity ←](./03-data-integrity.md) | [README ↑](./README.md)
