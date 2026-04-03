# Event Delivery Semantics

Guarantees that define how many times an event is delivered from producer to consumer (at-most-once, at-least-once, exactly-once)

→ Back to [Event-Driven Design](./README.md)

---

## At-Most-Once Delivery

### How It Works
- Producer sends message, doesn't wait for acknowledgment
- Broker may lose messages if it crashes before persistence
- Consumer processes without storing offset before processing
- No retry mechanism on failure

### Configuration
| System | Setting |
|--------|---------|
| **Kafka** | `acks=0`, consumer doesn't commit offset |
| **RabbitMQ** | No publisher confirms, auto-ack mode |
| **AWS SNS** | Default fire-and-forget |

### When to Use
- **Non-critical analytics**: page view counters, approximate metrics
- **Real-time dashboards**: tolerate occasional dropped events
- **Telemetry data**: duplicate or loss acceptable
- **Performance-critical systems**: absolute lowest latency required

### Risks
- Data loss is silent and undetected
- No way to know if message was lost
- Violates "source of truth" requirements
- Unsuitable for financial/compliance workflows

---

## At-Least-Once Delivery

### How It Works
- Producer waits for broker acknowledgment before returning
- Consumer processes message, then commits offset
- If consumer crashes during processing, offset not committed
- Message redelivered on consumer restart → potential duplicates
- Producer retries on timeout/failure

### Configuration
| System | Setting |
|--------|---------|
| **Kafka** | `acks=all`, `retries=infinite`, consumer commits offset AFTER processing |
| **RabbitMQ** | Publisher confirms enabled, consumer manual ack |
| **AWS SQS** | Long polling enabled, visibility timeout on message |

### When to Use
- **Event sourcing**: all events must be captured
- **Audit logs**: completeness > perfect ordering
- **Analytics pipelines**: loss unacceptable, duplicates tolerable
- **Default choice**: good balance of safety + performance
- **Order notifications**: consumer handles duplicates gracefully

### Idempotent Consumer Pattern
Duplicate messages are inevitable → consumer must process same message safely multiple times

**Pattern:**
- Store processed message ID (idempotency key) in state store
- On arrival, check if already processed
- If yes: return cached result
- If no: process and store result

**Pseudocode:**
```python
class IdempotentConsumer:
    def process_message(self, message_id, event_data):
        # Check deduplication store
        cached_result = self.state_store.get(message_id)
        if cached_result is not None:
            return cached_result  # Already processed

        # Process event
        result = self.business_logic(event_data)

        # Store result atomically with source tracking
        self.state_store.put(message_id, result)

        # Commit offset only after persistence
        self.commit_offset()

        return result
```

**Implementation Approaches:**
- **Database unique constraint**: insert only if message_id + timestamp unique
- **Redis cache**: check SET with TTL, prevents duplicate processing
- **Local state store**: RocksDB, LevelDB for ephemeral deduplication
- **Event deduplication store**: dedicated table with (message_id, consumer_group)

### Spring Kafka Example
```java
@KafkaListener(topics = "order-events", groupId = "order-processor")
public void processOrder(ConsumerRecord<String, OrderEvent> record) {
    String messageId = record.headers().lastHeader("X-Message-ID").value();
    OrderEvent event = record.value();

    // Check if already processed
    if (idempotencyStore.exists(messageId)) {
        log.info("Duplicate message, skipping: {}", messageId);
        return;
    }

    try {
        // Process order
        Order order = orderService.createOrder(event);

        // Store idempotency key atomically
        idempotencyStore.save(messageId, order.getId());

        // Commit offset (automatic in Spring, but can be manual)
        // No action needed if using enable-auto-commit

    } catch (Exception e) {
        log.error("Processing failed for {}", messageId, e);
        // Will retry on rebalance/restart
        throw e;
    }
}
```

### Exactly-Once Guarantee Breaking Points
- Network partition before ack reaches producer
- Consumer crash before offset commit
- Message redelivered → consumer must be idempotent

---

## Exactly-Once Delivery

### How It Works
- **Transactional write**: consumer writes processed result + offset commit in single atomic transaction
- **Idempotent producer**: broker deduplicates based on producer ID + sequence number
- **If replay occurs**: consumer sees same message, checks state store, skips duplicate
- **Trade-off**: 10-50% performance penalty for ordering guarantee

### Configuration

#### Kafka (Transactional)
```properties
# Producer side
enable.idempotence=true
transactional.id=unique-producer-id
acks=all

# Consumer side
isolation.level=read_committed
enable.auto.commit=false  # Manual offset management
```

#### Pattern: Transactions + Idempotent Producer
```java
@KafkaListener(topics = "payments", groupId = "payment-processor")
@Transactional  // Spring transaction wrapping both DB + Kafka
public void processPayment(ConsumerRecord<String, PaymentEvent> record) {
    PaymentEvent event = record.value();
    String idempotencyKey = record.headers()
        .lastHeader("X-Message-ID").value();

    // Begin transaction (DB)
    try {
        // Check if already processed in DB
        Payment existing = paymentRepository.findByIdempotencyKey(idempotencyKey);
        if (existing != null) {
            log.info("Payment already processed: {}", idempotencyKey);
            return;
        }

        // Process payment
        Payment payment = paymentService.process(event);

        // Persist payment + idempotency record atomically
        paymentRepository.save(payment);
        idempotencyRepository.save(new IdempotencyRecord(
            idempotencyKey, payment.getId()
        ));

        // Commit offset as part of transaction
        // (KafkaTemplate with Kafka transaction)

    } catch (Exception e) {
        // Transaction rolls back, offset not committed
        // Message will be redelivered
        throw e;
    }
}
```

### Performance Cost
- **Latency overhead**: 20-50% slower (batching delays, sync flush)
- **Throughput reduction**: fewer messages/sec due to ordering constraint
- **Broker complexity**: maintains producer state, deduplication window
- **Storage**: idempotency metadata stored on all replicas

### When Worth It
- **Financial transactions**: payment processing, money transfers
- **Inventory updates**: stock decrement, prevent overselling
- **Account state**: balance modifications, fraud detection critical
- **Legal compliance**: audit trail, SOX/GDPR requirements
- **High-value events**: cost of duplicate > performance penalty

### When NOT Worth It
- **Analytics**: duplicates acceptable, cost unjustified
- **Real-time dashboards**: eventual consistency OK
- **Metrics/telemetry**: approximate counts sufficient
- **High-throughput streams**: 50% throughput loss unacceptable

---

## Comparison Table

| Semantic | Configuration | Failure Behavior | Risk | Latency | Use Case |
|----------|---------------|------------------|------|---------|----------|
| **At-Most-Once** | `acks=0`, no retry | Silent data loss | Producer crash loses messages | Lowest | Metrics, analytics |
| **At-Least-Once** | `acks=all`, idempotent consumer | Duplicates on replay | Duplicate charges if consumer not idempotent | Medium | Orders, events, audit logs |
| **Exactly-Once** | `isolation.level=read_committed`, transactions | Slowdown on failure | High latency, reduced throughput | Highest | Payments, inventory, balance |

---

## Trade-Off Decision Framework

```
Do you need ZERO data loss?
├─ NO  → At-Most-Once (fire-and-forget)
└─ YES → Proceed below

Is duplication a problem?
├─ NO  → At-Least-Once + Idempotent Consumer (best for most cases)
└─ YES → Exactly-Once (financial, inventory, compliance)

How much latency can you tolerate?
├─ <100ms  → Reconsider Exactly-Once (may be <250ms)
└─ Can wait → Exactly-Once is safe to use
```

---

## Implementation Checklist

### At-Least-Once Setup
- [ ] Producer: enable `acks=all`, set appropriate timeout
- [ ] Consumer: manually commit offset AFTER processing
- [ ] Build idempotent handler (check + process + store atomically)
- [ ] Add message ID to event headers
- [ ] Test consumer crash scenario (message should reprocess)
- [ ] Monitor duplicate rates in logs

### Exactly-Once Setup
- [ ] Producer: enable idempotence (`enable.idempotence=true`)
- [ ] Consumer: set `isolation.level=read_committed`
- [ ] Implement state store with idempotency keys
- [ ] Wrap processing in database transaction
- [ ] Test: kill consumer mid-transaction, verify no partial state
- [ ] Monitor latency impact (expect 20-30% increase)

---

## Common Pitfalls

> [!WARNING] Pitfall: Committing Offset Before Processing
> **Wrong:** Consume → Commit → Process
> **Right:** Consume → Process → Commit
>
> If you commit first and crash during processing, the message is lost

> [!WARNING] Pitfall: No Idempotency Key in Message
> If duplicate arrives but you only check business logic, you might process twice
> **Always include** a unique message ID in headers

> [!WARNING] Pitfall: Assuming Exactly-Once Without Transactions
> Exactly-once requires BOTH idempotent producer AND transactional consumer state
> One without the other gives false confidence

> [!WARNING] Pitfall: Mixing Offset Commit Strategies
> Don't use auto-commit AND manual commit in same consumer
> Pick one and stick with it across the app

---

## Interview Tip

**Q: "Which delivery semantic would you choose for an e-commerce order system and why?"**

**Senior/Principal Answer:**

"I'd use **at-least-once delivery with an idempotent consumer**. Here's the reasoning:

**Why not at-most-once:** We can't afford to silently lose orders—that's revenue loss and a compliance issue.

**Why not exactly-once:** The 20-50% latency overhead doesn't justify the cost. We're not transferring money (that's a separate payment service with exactly-once), we're just taking orders. At-least-once is sufficient.

**How we handle duplicates:** Every order event includes a unique message ID in the Kafka headers. Our consumer stores processed order IDs in the database with a unique constraint. If a duplicate arrives, we query the database, find the existing order, and skip reprocessing. All of this—the query, the insert, the offset commit—happens in a single database transaction, so if we crash mid-processing, the offset never commits and we retry.

**Monitoring:** We track duplicate detection rates in CloudWatch. If we start seeing 30%+ duplication (instead of expected <5%), that signals something is wrong in the producer retry logic, and we investigate.

**If the system evolved:** If we later added direct inventory deductions to the order consumer (instead of async), we'd upgrade to exactly-once at that point, accepting the latency cost for correctness."

---

## References & Further Reading
- Kafka Documentation: Exactly-Once Semantics
- Martin Kleppmann: *Designing Data-Intensive Applications* (Ch. 11)
- Spring Kafka: Transactions and Exactly-Once Semantics
- RabbitMQ: Reliability Guide
