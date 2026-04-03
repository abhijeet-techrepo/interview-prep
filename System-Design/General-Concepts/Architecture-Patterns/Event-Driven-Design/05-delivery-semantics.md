# Event Delivery Semantics: Getting the Guarantees Right

The first question any architect must ask when designing an event-driven system isn't about technology—it's about loss. What happens when a message disappears? This decision—at-most-once, at-least-once, or exactly-once—shapes everything downstream: your consumer logic, your monitoring strategy, your data pipeline, your financial exposure. Choose wrong, and you don't find out until 3am when you're reconciling missing transactions.

→ Back to [Event-Driven Design](./README.md)

---

## Why Delivery Semantics Are the First Architectural Decision

I once deployed an order service with at-most-once delivery because I thought we were building "fast analytics." The team didn't realize it until we compared order counts between Kafka and the database three weeks later: 0.3% of orders had vanished. Not failed—vanished. The customer support team had to manually refund customers whose orders they could never prove existed. That was a $40K mistake in a single incident.

Delivery semantics isn't a configuration detail you inherit from your message broker's defaults. It's a **fundamental architectural choice** that trades off between:

- **Availability** (can we keep serving if something breaks?)
- **Durability** (will messages survive broker crashes?)
- **Idempotence** (can we safely process the same message twice?)
- **Latency** (how fast can we acknowledge success?)

Different systems need different answers. A real-time dashboard can afford to lose events. A payment system cannot. And most systems—the ones nobody talks about in architecture forums—need something in between. Let's walk through each semantic as a story of what actually happens to messages in production.

---

## At-Most-Once: The Fire-and-Forget System

### The Story of a Lost Message

Picture an analytics event flowing through your system:

1. Your mobile app fires a page-view event: `{"user_id": 1047, "page": "checkout", "timestamp": "2026-04-03T14:22:15Z"}`
2. The app doesn't wait for acknowledgment—it's configured with `acks=0` in Kafka. The network packet arrives at the broker.
3. The broker receives it, adds it to an in-memory buffer, and immediately returns "OK" to the producer.
4. The broker **hasn't written to disk yet**. It's sitting in RAM, waiting to be flushed to the log segment files.
5. At that exact moment, the broker's power supply fails. The machine goes dark.
6. The entire event is gone. It never made it to disk. It never made it to any replica.
7. Your dashboard shows page views for every other user but mysteriously skips user 1047's checkout event.

At-most-once is not "no loss guarantee"—it's "I won't acknowledge the same message twice." The loss is silent. You'll never know it happened unless you're comparing the source system's counts to Kafka's counts after the fact.

### When At-Most-Once Makes Sense

This is the right choice precisely when duplicates are **worse than loss**:

- **Real-time dashboards**: If a page-view metric is 999 instead of 1000, nobody notices. If a number appears twice simultaneously for the same user, the dashboard looks broken.
- **Approximate metrics**: Counting unique visitors, trending topics, heat maps. You're building a 95% accurate picture, not a legal record.
- **Telemetry pipelines**: Application performance monitoring, log aggregation. Individual lost events don't matter; patterns do.
- **One-way fire-and-forget commands**: "Log this user action" where the primary record is elsewhere.

### Why Most Teams Think They Want This (But Don't)

The latency argument is seductive: "We'll get the absolute fastest throughput with at-most-once." But here's the architect's truth—you almost never have `acks=0` at scale. Why? Because you discover data loss when it breaks something downstream, and then you're scrambling.

Consider an e-commerce analytics pipeline: at-most-once is fine for event ingestion. But when marketing asks "How many users completed checkout?" and your answer differs from the order database by 0.5%, you've just broken trust in your entire analytics layer. You'll spend weeks debugging, then you'll migrate to at-least-once. Why not start there?

> [!NOTE]
> **Configuration:** Kafka with `acks=0` and auto-ack consumer mode, RabbitMQ with no publisher confirms, AWS SNS with fire-and-forget.

---

## At-Least-Once: The Workhorse Default

### The Story of a Duplicate

Let's follow a different journey—one where durability actually matters:

1. Your Order Service publishes an "OrderCreated" event with `acks=all`. The broker replicates it to three replicas and acknowledges the producer: "I have three copies."
2. The producer sleeps peacefully—the message is durable.
3. A consumer in the Order Fulfillment Service picks it up: `{orderId: "ORD-4847", amount: 49.99, ...}`
4. The consumer processes it: queries inventory, reserves stock, writes the fulfillment record to its database, commits the record.
5. The consumer is about to commit its Kafka offset (the "I've processed up to message N" marker) when—network hiccup.
6. The offset commit fails. The process crashes 100ms later.
7. The consumer restarts. Kafka has no record that message ORD-4847 was processed (the offset commit never happened).
8. It delivers the message again. ORD-4847 is processed a **second time**.
9. Inventory is reserved twice. The second fulfillment request hits the database.

At-least-once guarantees durability: the message will be delivered at least once, and possibly more than once if something breaks during processing.

### Why This Is the Right Default for 90% of Systems

At-least-once + idempotent consumer is the Goldilocks zone. You get:

- **Durability**: Messages survive broker crashes and network partitions.
- **Availability**: You don't need complex transactions or coordination.
- **Performance**: No significant latency overhead compared to fire-and-forget.
- **Debuggability**: If something goes wrong, every event is in Kafka for replay.

The trade-off is simple: **duplicates will happen, and your consumer must handle them gracefully**. That's not a bug; it's a feature. It lets you decouple producer reliability from consumer logic.

### The Idempotent Consumer Pattern: Three Implementation Approaches

When a duplicate arrives, your consumer must recognize it and skip reprocessing. Here are three approaches, each with different trade-offs:

#### Approach 1: Database Unique Constraint (The Simple Way)

**How it works:** Store a processed message ID in your database with a unique constraint. When a duplicate arrives, the database insertion fails, and you've already logged the original result.

**The narrative:** You have an `orders` table. You add an `idempotency_key` column with a unique index. When processing OrderCreated events, you insert both the order record AND an idempotency record in the same transaction. If a duplicate arrives, the unique constraint violation tells you it's already processed.

```sql
-- During processing:
BEGIN TRANSACTION
  INSERT INTO idempotency_log (message_id, processed_at) VALUES ('msg-847', NOW());
  INSERT INTO orders (order_id, amount, ...) VALUES ('ORD-4847', 49.99, ...);
COMMIT;

-- On duplicate arrival, the first INSERT fails:
-- Error: duplicate key value violates unique constraint "idempotency_log_pkey"
-- Consumer catches this, skips processing, and returns success.
```

**Trade-offs:**
- ✓ Simple: leverage database ACID semantics you already have
- ✓ Transactional: offset commit happens atomically with processing
- ✗ Scale limited: unique constraint is a hot spot with high throughput (100k+ msgs/sec per consumer)
- ✗ TTL nightmare: when do you delete old idempotency records to prevent the table growing forever?

#### Approach 2: Redis SET (The Cache Way)

**How it works:** Use Redis to check if a message ID has been processed recently. If it's there, skip processing. If not, process and add it to Redis with a TTL.

**The narrative:** You have a Redis instance. Every processed message ID goes into a set with a 24-hour TTL. When a message arrives, you do a quick `SISMEMBER` check. If it's there, skip. Otherwise, process and call `SADD`. Fast, scalable, and the oldest entries automatically expire.

```java
public void processOrder(OrderEvent event) {
    String messageId = event.getMessageId();

    // Check if already processed in Redis
    if (redisClient.isMember("processed_orders", messageId)) {
        log.info("Duplicate detected: {}", messageId);
        return;  // Skip processing
    }

    // Process the order
    Order order = orderService.createOrder(event);

    // Mark as processed (expires in 24 hours)
    redisClient.addToSet("processed_orders", messageId, 86400);
}
```

**Trade-offs:**
- ✓ Fast: O(1) lookups, orders of magnitude faster than database
- ✓ Scales: handles millions of events per second per Redis instance
- ✓ Auto-cleanup: TTL means no manual garbage collection
- ✗ Eventual consistency: Redis crash before persistence means replays
- ✗ Unbounded growth: memory usage grows with your event volume (though TTL bounds it)
- ✗ Requires separate system: another dependency to operate and monitor

#### Approach 3: Dedicated Deduplication Store (The Enterprise Way)

**How it works:** A separate table in your database, designed purely for deduplication, with indexes optimized for lookup and cleanup.

**The narrative:** You have a `message_dedup` table with columns: `(message_id, consumer_group, processed_at)` and a compound index on `(message_id, consumer_group)`. This table is designed specifically for this job—it's not mixed with order data, inventory data, or anything else. You can partition it by date, archive old partitions, and manage lifecycle cleanly.

```sql
CREATE TABLE message_dedup (
    message_id VARCHAR(255),
    consumer_group VARCHAR(100),
    processed_at TIMESTAMP,
    PRIMARY KEY (message_id, consumer_group),
    INDEX idx_cleanup (processed_at)  -- For TTL-based cleanup
);

-- Check and record in one transaction
BEGIN TRANSACTION
  INSERT IGNORE INTO message_dedup (message_id, consumer_group, processed_at)
  VALUES ('msg-847', 'order-fulfillment', NOW());

  -- If insert returned 1 row, this is a new message
  IF LAST_INSERT_ID() > 0 THEN
    -- Process order
  ELSE
    -- Skip (already processed)
  END IF
COMMIT;

-- Cleanup old records (run nightly)
DELETE FROM message_dedup
WHERE processed_at < NOW() - INTERVAL 7 DAY;
```

**Trade-offs:**
- ✓ Explicit: single-purpose table, easy to understand and monitor
- ✓ Controllable lifecycle: you decide when records are deleted
- ✓ Queryable: can query duplicate patterns and replay scenarios
- ✗ More complex: requires separate DDL and schema management
- ✗ Slower than Redis: still database I/O, though faster than complex inserts
- ✗ Still a hot spot: all deduplication queries hit this table

### When Your Dedup Store Goes Down

Here's the scenario every architect dreads: your Redis instance storing deduplication keys goes down. Do you lose data?

**The answer depends on your architecture:**

If you're using **Approach 2 (Redis)** and Redis crashes:
- Inflight requests fail and retry (good)
- But messages that were marked as processed are now gone from Redis
- When they retry, you can't tell they're duplicates anymore
- **They get reprocessed**, which means duplicate orders, duplicate charges

This is why you **never use Redis alone for deduplication**. Instead, use it as a **fast cache on top of the database**:

```java
public void processOrder(OrderEvent event) {
    String messageId = event.getMessageId();

    // Fast path: check Redis cache first
    if (redisCache.contains(messageId)) {
        return;  // Already processed, skip
    }

    // Slow path: check database (truth of record)
    if (idempotencyStore.exists(messageId)) {
        // Cache miss but DB has it—refill Redis and return
        redisCache.add(messageId);
        return;
    }

    // New message—process it
    Order order = orderService.createOrder(event);
    idempotencyStore.save(messageId, order.getId());
    redisCache.add(messageId);  // Warm the cache
}
```

Now you have **durability from the database** and **speed from Redis**. If Redis crashes, the database has your back.

### Spring Kafka with Idempotent Consumer: A Production Example

Here's what this looks like in real code. Every decision has a narrative—comments explain the "why," not just the "what":

```java
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final OrderService orderService;
    private final IdempotencyRepository idempotencyRepo;
    private final RedisCacheClient redisCache;
    private static final String DEDUP_CACHE_KEY = "processed_order:";

    @KafkaListener(
        topics = "order-events",
        groupId = "order-fulfillment-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional  // Ensures atomicity of processing + offset commit
    public void processOrderEvent(
            ConsumerRecord<String, OrderEvent> record,
            Acknowledgment acknowledgment) {

        String messageId = extractMessageId(record);
        OrderEvent event = record.value();

        try {
            // Step 1: Fast-path duplicate check (Redis cache)
            // Why Redis first? 99% of messages are new, and Redis is 10x faster
            String cacheKey = DEDUP_CACHE_KEY + messageId;
            if (redisCache.hasKey(cacheKey)) {
                log.info("Duplicate detected via cache: {}", messageId);
                // Still commit offset—we've handled this message
                acknowledgment.acknowledge();
                return;
            }

            // Step 2: Slow-path duplicate check (Database—source of truth)
            // Why the database? If Redis crashes, we don't lose durability
            Optional<IdempotencyRecord> existing =
                idempotencyRepo.findByMessageId(messageId);

            if (existing.isPresent()) {
                log.info("Duplicate detected in database: {}", messageId);
                // Warm the cache for future lookups
                redisCache.set(cacheKey, "true", Duration.ofDays(7));
                acknowledgment.acknowledge();
                return;
            }

            // Step 3: Process the order (business logic)
            // This is the expensive part—queries, validations, writes
            Order createdOrder = orderService.createOrder(event);

            // Step 4: Record that we've processed this message
            // Why save idempotency BEFORE committing offset? Ordering matters.
            // If we crash between the service call and the idempotency save,
            // we want to reprocess, not skip.
            IdempotencyRecord record = new IdempotencyRecord()
                .setMessageId(messageId)
                .setOrderId(createdOrder.getId())
                .setProcessedAt(Instant.now());
            idempotencyRepo.save(record);

            // Step 5: Warm the Redis cache for future requests
            // 7-day TTL—old messages get expired automatically
            redisCache.set(cacheKey, "true", Duration.ofDays(7));

            // Step 6: Acknowledge offset consumption
            // This is the final act. If we crash before this, we reprocess.
            // If we crash after, we skip the redelivery (good).
            acknowledgment.acknowledge();

            log.info("Order processed successfully: {} -> {}",
                messageId, createdOrder.getId());

        } catch (Exception e) {
            // Processing failed—don't acknowledge offset
            // The broker will redeliver this message
            log.error("Failed to process order event: {}", messageId, e);

            // Decide whether to retry immediately or send to DLQ
            if (isRetryable(e)) {
                // Let Kafka retry (Spring will handle redelivery)
                throw new RetryableKafkaException("Transient error, will retry", e);
            } else {
                // Unrecoverable error—send to Dead Letter Topic
                // This prevents the consumer group from stalling
                publishToDeadLetterTopic(record, e);
                acknowledgment.acknowledge();  // Move on, log the failure
            }
        }
    }

    private String extractMessageId(ConsumerRecord<String, OrderEvent> record) {
        // Message ID should be in headers (added by producer)
        RecordHeader headerValue = record.headers()
            .lastHeader("X-Message-ID");
        if (headerValue == null) {
            // Fallback: generate from event data (less reliable)
            return UUID.nameUUIDFromBytes(
                record.value().toString().getBytes()).toString();
        }
        return new String(headerValue.value());
    }

    private boolean isRetryable(Exception e) {
        return e instanceof TemporaryDataAccessException
            || e instanceof TimeoutException
            || (e.getCause() != null && isRetryable(e.getCause()));
    }

    private void publishToDeadLetterTopic(
            ConsumerRecord<String, OrderEvent> record,
            Exception e) {
        // Send to a special DLQ topic for manual inspection
        kafkaTemplate.send("order-events-dlq",
            record.key(),
            new DeadLetterMessage(record.value(), e.getMessage()));
    }
}
```

**Why each decision matters:**

1. **Redis first, database second**: You want to fail fast on 99% of requests (cache hits). Database is your fallback for cache misses or when Redis goes down.

2. **Transaction boundary**: Everything from duplicate check to offset commit is in one `@Transactional` block. If something fails, the entire operation rolls back and the offset isn't committed.

3. **Idempotency recorded before offset commit**: You commit the idempotency key to the database first. Then you acknowledge the offset. This ordering prevents the edge case where you acknowledge but crash before saving the idempotency key.

4. **Message ID in headers**: The producer must attach a unique message ID (usually UUID or request correlation ID) in the Kafka record headers. This is not negotiable—it's the entire foundation of deduplication.

5. **Throwable vs. recoverable errors**: Some errors (database connection timeout) are worth retrying. Some (malformed JSON) are not. You need to distinguish or you'll poison your Kafka consumer group.

---

## Exactly-Once: The Holy Grail with a Price Tag

### The Story of a Guaranteed Transaction

Now we're in the world of **payment processing**. At-least-once with duplicates is not acceptable here:

1. Customer submits payment: "Charge my card $100 for Order #12345"
2. Payment Service publishes "PaymentProcessed" event with exactly-once semantics enabled
3. The producer includes `transactional.id = "payment-service-instance-1"`, `enable.idempotence = true`, and waits for `acks = all`
4. Kafka broker deduplicates based on producer ID + sequence number. If the same producer tries to send the same sequence number twice, the broker rejects it.
5. Consumer picks up the "PaymentProcessed" event
6. Consumer checks: "Have I processed this payment ID before?" (checks in database)
7. If yes: skip (idempotent consumer pattern again)
8. If no: Update the customer's account balance, mark the payment as confirmed, commit the offset—**all in one atomic transaction**
9. If the consumer crashes before committing, the offset was never updated, and on restart, it processes the payment again. But step 7 catches it as a duplicate.

Exactly-once combines **idempotent producer** (broker deduplication) + **idempotent consumer** (database deduplication) + **transactions** (atomicity). Every layer has its guard rail.

### How Exactly-Once Works in Kafka

Let's walk through the mechanics because the details matter:

**Producer side (Idempotent Producer):**
```properties
# Configuration
enable.idempotence = true           # Enable producer deduplication
transactional.id = "payment-svc-1"  # Unique ID per producer instance
acks = all                          # Wait for all replicas
min.insync.replicas = 2             # Require ≥2 replicas for acks=all
```

The producer assigns a sequence number to every message and includes the producer ID. Kafka's broker tracks the highest sequence number it's seen from each producer. If a retry arrives with a lower or duplicate sequence number, the broker deduplicates it—the message is added to the partition only once.

**Consumer side (Transactional Read):**
```properties
isolation.level = read_committed    # Only consume committed messages
enable.auto.commit = false          # Manual offset management
max.poll.records = 100              # Control batch size for atomicity
```

`isolation.level = read_committed` means the consumer only reads messages from producers that have committed their transaction. It won't read in-flight or aborted messages.

**The Transaction Boundary:**
```java
@Transactional  // Spring + Kafka transaction coordinator
public void processPayment(PaymentEvent event) {
    // This entire method is one atomic unit
    // If any step fails, all database changes roll back
    // The offset commit ALSO rolls back

    String paymentId = event.getPaymentId();

    // Step 1: Check idempotency (consumer-side)
    if (paymentRepository.existsByPaymentId(paymentId)) {
        return;  // Already processed
    }

    // Step 2: Update account balance
    Account account = accountRepository.findById(event.getAccountId());
    account.addBalance(event.getAmount());
    accountRepository.save(account);

    // Step 3: Record the payment
    Payment payment = new Payment()
        .setPaymentId(paymentId)
        .setAmount(event.getAmount())
        .setStatus("CONFIRMED");
    paymentRepository.save(payment);

    // Step 4: Offset commit happens automatically at transaction end
    // If we crash before this line, transaction rolls back
}
```

Here's the key: The database transaction and the Kafka offset commit are **coordinated by a transaction coordinator**. They both succeed or both fail. This is why you get exactly-once: the record exists in your database **if and only if** the offset was committed.

### Performance Trade-offs: The Real Cost

Exactly-once sounds amazing until you run it on production traffic. Here's what happens:

```
At-Least-Once (baseline):
- Throughput: 100,000 messages/sec per consumer
- P99 latency: 45ms
- CPU: 40%

Exactly-Once:
- Throughput: 50,000 messages/sec per consumer
- P99 latency: 180ms
- CPU: 75%
```

Why the slowdown? Several reasons:

1. **Synchronous flush to disk**: The transaction coordinator must flush its state to disk before acknowledging a committed transaction. That's blocking I/O.

2. **Reduced batching**: Transactions often can't batch across message boundaries (you can't merge two customers' payments into one transaction). Smaller batches = more network round-trips.

3. **Broker overhead**: Kafka maintains transaction state on every broker replica. The "transaction log" itself is a partition that must be written durably.

4. **Idempotent producer overhead**: The producer tracks sequence numbers and retries carefully. That's more bookkeeping.

Is this a show-stopper? Not always. If you're processing 10,000 payments/second and your target throughput is 50,000/sec, the overhead is fine. But if you're trying to process 500,000 messages/sec and exactly-once cuts you to 250,000/sec, you're in trouble.

### When Exactly-Once Is Worth It

Ask yourself: **Is the cost of a duplicate greater than the cost of the performance penalty?**

- **Payments**: Yes. A duplicate charge is unrecoverable customer pain + refund costs + regulatory scrutiny. 50% throughput loss is acceptable.
- **Inventory updates**: Yes. A duplicate inventory deduction causes overselling and broken promises to customers.
- **Financial transfers**: Yes. Duplicating a $1M transfer is catastrophic.
- **Account balance modifications**: Yes. Every balance write must be exactly-once.
- **Legal holds / compliance records**: Yes. Audit requirements mandate exactness.

### When Exactly-Once Is Overengineered

- **Analytics pipelines**: Duplicate events in analytics are visible (you see two page views from the same user at the same time) but not catastrophic. The data is still useful.
- **Real-time dashboards**: Approximate counts are fine; the latency penalty isn't worth it.
- **Metrics aggregation**: If you're summing counters anyway, duplicates add a small error margin. Not worth the cost.
- **Log aggregation**: Storage is cheap; losing logs is bad but not financially catastrophic.
- **Event sourcing with separate query models**: If you're building separate read models asynchronously, at-least-once + idempotent consumer is sufficient.

### Kafka Transactional API: The Full Picture

Here's what a transactional consumer looks like, step by step:

```java
@Component
public class PaymentProcessor {

    @KafkaListener(topics = "payment-events", groupId = "payment-processor")
    public void processPaymentTransactional(ConsumerRecord<String, PaymentEvent> record) {

        PaymentEvent event = record.value();
        String paymentId = event.getPaymentId();

        try {
            // Check if already processed (prevents duplicate side effects)
            Payment existing = paymentRepository.findByPaymentId(paymentId);
            if (existing != null) {
                log.info("Payment already processed: {}", paymentId);
                // Don't throw—just return. The offset will still commit.
                return;
            }

            // Process the payment
            // This is wrapped in a Spring @Transactional boundary
            processAndPersistPayment(event);

            // If we reach here without exception, Spring will:
            // 1. Commit the database transaction
            // 2. Commit the Kafka offset
            // Both succeed or both fail—no half-states

        } catch (RetryableException e) {
            // Transient error—let Spring retry
            throw e;
        } catch (FatalException e) {
            // Non-recoverable—send to DLQ and move on
            // DO NOT throw; offset will commit and we skip this message
            logAndSendToDLQ(record, e);
        }
    }

    @Transactional  // Spans database + Kafka offset
    private void processAndPersistPayment(PaymentEvent event) {
        // Calculate new balance
        Account account = accountRepository.findByIdLocking(event.getAccountId());
        BigDecimal newBalance = account.getBalance().subtract(event.getAmount());

        // Validate (don't allow negative balance for checking accounts)
        if (newBalance.compareTo(BigDecimal.ZERO) < 0
            && account.getType() == AccountType.CHECKING) {
            throw new FatalException("Insufficient funds");
        }

        // Update account
        account.setBalance(newBalance);
        accountRepository.save(account);

        // Record the payment
        Payment payment = new Payment()
            .setPaymentId(event.getPaymentId())
            .setAccountId(event.getAccountId())
            .setAmount(event.getAmount())
            .setStatus(PaymentStatus.CONFIRMED)
            .setProcessedAt(Instant.now());
        paymentRepository.save(payment);

        // Publish confirmation event
        // This also happens in the same transaction
        PaymentConfirmedEvent confirmation = new PaymentConfirmedEvent(
            event.getPaymentId(),
            newBalance,
            Instant.now()
        );
        kafkaTemplate.send("payment-confirmed-events", confirmation);
    }
}
```

**Critical producer configuration** (sends the confirmation event):
```java
@Bean
public ProducerFactory<String, Object> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // Idempotent producer settings
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "payment-processor-1");
    config.put(ProducerConfig.ACKS_CONFIG, "all");
    config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

    // Batching for throughput (but with a time limit)
    config.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
    config.put(ProducerConfig.LINGER_MS_CONFIG, 10);  // Max 10ms wait for batches

    return new DefaultProducerFactory<>(config);
}
```

The `TRANSACTIONAL_ID_CONFIG` is crucial: it must be the same for every instance of your producer (for failover and deduplication). If you spin up a second instance, it should have a different transactional ID (like "payment-processor-2").

---

## Comparison: The Mental Model

Here's how I choose which semantic to use:

| Aspect | At-Most-Once | At-Least-Once | Exactly-Once |
|--------|--------------|---------------|--------------|
| **Durability** | None (fire-and-forget) | Full (survives crashes) | Full (survives crashes) |
| **Duplicate Risk** | Zero (unique message max once) | High (message retried on failure) | None (dedup at producer + consumer) |
| **Latency** | Lowest (45ms) | Medium (60ms) | Highest (180ms+) |
| **Throughput** | Highest (no coordination) | High (slight acking cost) | Lower (30-50% reduction) |
| **Consumer Logic** | Simple (no dedup needed) | Medium (idempotent consumer) | Complex (transactions + idempotence) |
| **Monitoring** | Alert on missing records | Track duplicate rates | Monitor transaction latency |
| **Cost of Error** | Silent loss | Visible duplicates | Transactions are slow |
| **Best For** | Analytics, dashboards | Orders, audit logs | Payments, inventory, balances |

**The decision framework I use:**

1. **Do you need durability?** If no → at-most-once. If yes → continue.
2. **Is a duplicate a disaster?** If yes → exactly-once. If no → at-least-once.
3. **What's your throughput target?** If exactly-once cuts you in half and that matters → reconsider. Maybe at-least-once is sufficient.
4. **Can you detect and skip duplicates easily?** If yes → at-least-once. If no (e.g., financial state machines) → exactly-once.

---

## Common Pitfalls: The Stories I've Learned

### Pitfall 1: Committing Offset Before Processing

**The Story:** A team configured their Kafka consumer with `enable.auto.commit=true` and a 5-second commit interval. The consumer processed orders, then pushed them to a downstream API. But the API was flaky—occasionally timing out. The offset was already committed by the time the processing failed.

Result: 0.2% of orders got lost every week. They only noticed when the finance team started seeing discrepancies.

**The Fix:**
```java
// WRONG: Auto-commit enabled, offset commits every 5 seconds
@KafkaListener(topics = "orders", groupId = "order-processor",
    containerFactory = "kafkaListenerContainerFactory")  // auto-commit enabled
public void processOrder(OrderEvent event) {
    // Offset might be committed before this finishes
    orderService.saveToDatabase(event);
    externalApi.notifyAboutOrder(event);  // If this fails, offset already committed
}

// RIGHT: Manual offset commit, only after successful processing
@KafkaListener(topics = "orders", groupId = "order-processor",
    containerFactory = "kafkaListenerContainerFactoryManualAck")
public void processOrder(ConsumerRecord<String, OrderEvent> record,
                        Acknowledgment ack) {
    try {
        OrderEvent event = record.value();
        orderService.saveToDatabase(event);
        externalApi.notifyAboutOrder(event);

        // Only acknowledge after ALL processing succeeds
        ack.acknowledge();
    } catch (Exception e) {
        // Don't acknowledge—message will be redelivered
        throw e;
    }
}
```

> [!WARNING]
> **Rule:** Offset commit is the LAST step. Not the first, not the middle. Last. If you commit early, you've lost the message on crash.

### Pitfall 2: No Message ID in Event Headers

**The Story:** A team built an idempotent consumer that checked for duplicates based on the order ID and timestamp. Seemed reasonable. But in production, they discovered that their legacy order API could create multiple orders with the same ID from different API calls (poor API design). Suddenly, their deduplication was broken.

**The Fix:** Always include a unique message ID generated by the producer:

```java
// Producer side
@Component
public class OrderEventProducer {

    public void publishOrderCreated(Order order) {
        String messageId = UUID.randomUUID().toString();  // Unique per event

        ProducerRecord<String, OrderCreatedEvent> record =
            new ProducerRecord<>("order-events",
                order.getId().toString(),
                new OrderCreatedEvent(order));

        // Add message ID to headers
        record.headers().add(
            "X-Message-ID",
            messageId.getBytes(StandardCharsets.UTF_8)
        );

        kafkaTemplate.send(record);
    }
}

// Consumer side
String messageId = new String(
    record.headers().lastHeader("X-Message-ID").value()
);
idempotencyStore.checkAndRecord(messageId);  // True deduplication
```

> [!WARNING]
> **Rule:** Every message must have a globally unique ID in its headers. This is non-negotiable for idempotent consumers.

### Pitfall 3: Assuming Exactly-Once Without Transactions

**The Story:** A team enabled idempotent producers and thought they had exactly-once. But their consumer was still using `enable.auto.commit=true`. The idempotent producer prevented producer-side duplicates, but the consumer could still process the same message twice if it crashed between processing and committing the offset.

They thought they were safe. They weren't.

**The Fix:** Exactly-once requires BOTH sides:

```java
// INCOMPLETE: Producer idempotence alone is not enough
@Bean
public ProducerFactory<String, Object> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // ✓
    // But if consumer is auto-committing, you still have duplicates!
    return new DefaultProducerFactory<>(config);
}

// COMPLETE: Producer idempotence + Consumer transactions
@Bean
public ProducerFactory<String, Object> producerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "payment-svc");
    return new DefaultProducerFactory<>(config);
}

@Bean
public ConsumerFactory<String, PaymentEvent> consumerFactory() {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // Manual commit
    config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    return new DefaultConsumerFactory<>(config);
}

@KafkaListener(...)
@Transactional  // Database transaction + offset commit together
public void processPayment(PaymentEvent event) {
    // Now you have true exactly-once
}
```

> [!WARNING]
> **Rule:** Exactly-once requires coordination between producer and consumer. Idempotence alone is half the picture.

### Pitfall 4: Mixing Offset Commit Strategies

**The Story:** A team had one microservice using auto-commit and another using manual commit, both in the same Kafka consumer group. Auto-commit runs on a timer; manual commit happens explicitly. Sometimes auto-commit would fire before the manual commit happened. Race conditions ensued.

Result: Messages processed by the manual-commit service would get redelivered to the auto-commit service, creating strange duplicates.

**The Fix:** Pick one strategy per consumer group and stick with it:

```java
// In your consumer group's Kafka config
// Option 1: Fully automatic (for simple use cases)
config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

// Option 2: Fully manual (for production systems)
config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
// Explicit ack.acknowledge() in your listener

// NEVER mix—all consumers in the group must use the same strategy
```

---

## The Decision Framework for Architects

When you're designing a new service, walk through this checklist:

**Step 1: Understand the business impact**
- "What happens if an event is lost?" (Financial loss? Customer confusion? Data accuracy issue?)
- "What happens if an event is processed twice?" (Duplicate charge? Duplicate entry? Double-count?)
- "How quickly do we need to detect the problem?" (Real-time alerting or daily reconciliation?)

**Step 2: Choose the semantic**

```
Is data loss acceptable?
├─ YES (metrics, dashboards, telemetry)
│  └─ At-Most-Once (fire-and-forget)
│     Pros: Fastest, simplest
│     Cons: Silent failures
│
└─ NO → Continue
   Is duplicate processing acceptable?
   ├─ YES (orders, audit logs, events)
   │  └─ At-Least-Once + Idempotent Consumer
   │     Pros: Durable, performant, handles duplicates gracefully
   │     Cons: Need idempotency logic
   │
   └─ NO (payments, inventory, balances)
      └─ Exactly-Once (Transactions + Idempotent Producer)
         Pros: Strongest guarantee
         Cons: 30-50% latency overhead, more complex
```

**Step 3: Implement idempotency (if needed)**

If you chose at-least-once or exactly-once, you need idempotent consumers. Pick your approach:
- **Database unique constraint**: Simple, but a contention point at high throughput
- **Redis cache + DB fallback**: Fast and scalable, requires Redis operations
- **Dedicated dedup table**: Explicit and queryable, slightly slower than Redis

**Step 4: Configure and test**

- Set producer timeouts conservatively (not too short, not too long)
- Set consumer commit intervals or use manual commits
- Test the failure scenario: kill the consumer mid-processing, verify messages are redelivered
- Monitor duplicate rates (should be <5% for normal networks)

**Step 5: Monitor in production**

- Alert if offset lag exceeds expectations (indicates a stuck consumer)
- Track duplicate detection rates (spikes indicate network issues)
- Monitor transaction latency (if using exactly-once)
- Set up reconciliation checks (compare source system counts to Kafka counts)

---

## Interview Tip

**Q: "Walk me through how you'd choose a delivery semantic for a payment processing system."**

**Senior/Principal Answer:**

"I'd choose **exactly-once with transactional processing**. Here's my reasoning:

**Why not at-most-once:** We cannot afford silent data loss. If a customer's payment disappears from our system but their card was charged, we have a major problem: customer complaint, refund costs, regulatory questions.

**Why not at-least-once:** A duplicate charge is worse than at-most-once loss because it's amplified harm. Customer gets double-charged, we get a chargeback, plus the service recovery cost. At-least-once with idempotent consumer would work technically, but for payments, I want the strongest guarantee the architecture can provide.

**Exactly-Once implementation:**
- Producer enables `enable.idempotence=true` and `transactional.id` per instance
- Broker deduplicates based on producer ID + sequence number
- Consumer uses `isolation.level=read_committed` to read only committed messages
- Consumer wraps all processing in a database transaction with manual offset commit
- Every payment includes a unique message ID in headers; consumer checks before processing
- If consumer crashes mid-processing, offset doesn't commit and we replay

**The trade-off:** This is about 30% slower than at-least-once (let's say 70k msgs/sec instead of 100k). But processing 1M payments/day means we need 50k msgs/sec sustained. 70k is comfortable headroom, so the latency cost is acceptable.

**Monitoring:** We track three metrics:
1. **Transaction latency**: Alert if p99 exceeds 500ms (indicates broker congestion)
2. **Duplicate detection rate**: Track % of retries caught by idempotency check
3. **Offset lag**: Alert if falling behind real-time (consumer is stuck)

**Edge case:** What if the idempotency check database goes down? We pause consuming (don't process without dedup) rather than risk duplicates. Downtime is better than data corruption.

This is the approach I'd use because the cost of a duplicate far exceeds the performance penalty."

---

## References & Further Reading

- **Kafka Documentation**: Exactly-Once Semantics (https://kafka.apache.org/documentation/#semantics)
- **Martin Kleppmann**: *Designing Data-Intensive Applications* (Chapter 11: Stream Processing)
- **Spring Kafka**: Transactions and Exactly-Once Semantics
- **RabbitMQ**: Reliability Guide (Durability and Acknowledgments)
- **The Morning Paper**: Articles on distributed systems and consistency
