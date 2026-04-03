# The Outbox Pattern: Reliability Through Atomic Writes

Architect-level exploration of the pattern that solves the dual-write problem: How to atomically persist domain data and events without distributed transactions.

→ Back to [Event-Driven Design](./README.md)

---

## The Problem: A War Story

It's 2 AM in production.

Your order service just processed a customer order. The database write succeeded—order is in the system. Then the code tries to publish an event to Kafka, but the Kafka cluster is temporarily unreachable due to a network partition. Your application logs the event (or swallows the exception) and moves on.

Three hours later, Kafka comes back. The event was never published. Your inventory system never decremented stock. Your warehouse never received a pick list. Your accounting system never recorded the sale. The customer has been charged, but nothing else in your system knows about it.

Or reverse the sequence. You publish the event first—optimistic ordering! Then the database write fails with a constraint violation. Kafka now has a record of an order that doesn't exist in your database. Your downstream systems begin processing a phantom order. They try to decrement inventory that was never allocated. The audit trail is now permanently inconsistent.

This is the **dual-write problem**, and it's the reason many event-driven systems fail in unexpected, subtle ways.

The danger isn't obvious because it doesn't fail loudly. A missed event publishes isn't an exception that bubbles up. The database rollback doesn't somehow un-publish the event. The systems remain internally consistent—your database has the order, Kafka has the event—they're just not aligned. And now you need to write data recovery scripts at 3 AM.

---

## Why Naive Solutions Fail

Let me walk you through the approaches teams try, and exactly where they crack.

### Attempt 1: Write DB, Then Publish

```
Order Service:
  1. BEGIN TRANSACTION
  2. INSERT INTO orders ...
  3. COMMIT
  4. kafka.send(event)
```

This seems reasonable. You commit your data first. Then you publish.

**What breaks**: The network fails between steps 3 and 4. Or Kafka is down. Or there's a timeout. Your database now contains an order, but the event sits undelivered. You could retry, but if your application crashes before it retries, that event is gone forever unless you have other mechanisms (which you probably don't).

The failure mode is: *persistent, undetectable inconsistency*. You won't know orders are being dropped until days later when accounting asks why the numbers don't match.

### Attempt 2: Publish, Then Write DB

```
Order Service:
  1. kafka.send(event)
  2. BEGIN TRANSACTION
  3. INSERT INTO orders ...
  4. COMMIT
```

This flips the problem. Kafka accepts the event first. Feels safer since events are immutable and durable. Then you write to the database.

**What breaks**: Your database write fails. Maybe a constraint violation. Maybe a deadlock that times out. The database transaction rolls back. But the event is already published. Kafka doesn't know. Downstream systems see an `OrderCreated` event for an order that never existed. Your system becomes inconsistent in a different dimension.

The failure mode is: *phantom events driving phantom state changes*.

### Attempt 3: Use Distributed Transactions (2PC)

"Let's just use a transaction coordinator!" Wrap both Kafka and the database in a two-phase commit.

```
Coordinator:
  1. Prepare database write
  2. Prepare Kafka publish
  3. Commit both
```

This sounds theoretically sound. The problem: **distributed transactions are a tax you don't want to pay**.

Two-phase commit is notoriously slow. It introduces multiple round trips. It requires both systems to maintain locks while coordinating. If the coordinator fails mid-commit, the system can deadlock (either forever or until manual intervention). And most importantly: **not all message brokers even support 2PC**. Kafka certainly doesn't.

The failure mode is: *operational complexity and latency that grows worse over time*.

---

## The Insight: Make Events Part of the Database Transaction

This is where the Outbox Pattern comes from. A single, brilliant insight:

**What if the event wasn't separate from the database write? What if it was part of the same atomic transaction?**

Instead of:
- Writing to database
- Writing to message broker

You do:
- Writing to database table A (orders)
- Writing to database table B (outbox)
- Commit both in the same transaction (both succeed or both fail)

The event publication is completely decoupled from the order creation. The database guarantees atomicity. The message broker is only involved when the database transaction has already succeeded.

Here's the architectural shift:

```
Before (Dual Write):
  Service → DB (commit) → Kafka (hope it works)
           ↑                    ↓
           └─ eventual inconsistency

After (Outbox Pattern):
  Service → DB + Outbox (atomic commit) → Poller → Kafka
           ↑                                        ↓
           └──────── guaranteed delivery ──────────┘
```

The poller is a separate component. It has one job: poll the outbox table for unpublished events and send them to Kafka. If Kafka is down, the poller retries. If the poller dies, the events remain in the database. Nothing is lost. There's no window of vulnerability.

---

## ASCII Flow: The Pattern in Motion

```
┌────────────────────────────────────────────────────────────┐
│ Application (Order Service)                                │
└──────────────────────┬─────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        │   SAME TRANSACTION          │
        │   (atomic boundary)         │
        │                             │
        ↓                             ↓
    INSERT order                 INSERT outbox event
    (id=42,                       (aggregate_id=42,
     customer_id=99,              event_type='OrderCreated',
     total=$99.99)                payload='...')
        │                             │
        └──────────────┬──────────────┘
                       │
                    COMMIT
                       │
        ┌──────────────┴──────────────┐
        │ Success                     │ Failure (violation, deadlock, etc)
        │                             │
        ↓                             ↓
    Both rows inserted          ROLLBACK both
    No inconsistency            No data changed
        │                        (clean failure)
        │
        ├─→ Event sits in outbox
        │   published = FALSE
        │
        └─→ Outbox Poller
            (runs every 100ms)
            │
            SELECT * FROM outbox
            WHERE published = FALSE
            LIMIT 100
            │
            ├─→ Parse each event
            │
            └─→ kafkaTemplate.send()
                   │
                   ├─ Success → UPDATE outbox SET published=TRUE
                   │
                   └─ Failure → log.warn() → retry next cycle
                                (event stays in outbox)
```

The key feature: if the poller crashes, events don't vanish. They sit in the database waiting. If Kafka is down, the poller keeps trying. No event is ever lost unless you explicitly delete it from the outbox table.

---

## Production Implementation: Spring Boot + Kafka

Let me walk you through a battle-tested implementation. Each piece has a reason.

### The Outbox Table: Design

```sql
CREATE TABLE outbox (
    -- Surrogate key. Gives us a natural ordering
    -- and makes it easy to track progress
    id BIGINT PRIMARY KEY AUTO_INCREMENT,

    -- The event type: 'OrderCreated', 'PaymentProcessed', etc.
    -- Used as Kafka topic in the poller
    event_type VARCHAR(255) NOT NULL,

    -- The aggregate ID (order_id, payment_id, etc.)
    -- Used as Kafka partition key for ordering guarantees
    aggregate_id VARCHAR(255) NOT NULL,

    -- The full event payload as JSON
    -- Deserialized by consumers
    payload JSON NOT NULL,

    -- Tracks whether this event was published to Kafka
    -- Poller filters on: WHERE published = FALSE
    published BOOLEAN DEFAULT FALSE,

    -- When the event was created (immediate transaction time)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- When it was successfully published (set by poller)
    -- Useful for SLA tracking
    published_at TIMESTAMP NULL,

    -- The database will use this for efficient polling
    INDEX idx_published (published),
    -- And this for debugging / data retention queries
    INDEX idx_created (created_at)
);
```

**Why these fields**:
- `aggregate_id` as Kafka key ensures all events for the same order go to the same partition. This preserves ordering.
- `published` flag with index makes the poller's query trivial: just scan unpublished rows.
- `created_at` allows us to age out old published events (retention policy).
- `published_at` gives operations visibility into how long events sat in the queue.

### The Entity

```java
@Entity
@Table(name = "outbox")
@Getter
@Setter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String aggregateId;

    @Column(columnDefinition = "JSON")
    private String payload;

    private Boolean published = false;

    @CreationTimestamp
    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;

    // Constructor for convenience
    public OutboxEvent(String eventType, String aggregateId, String payload) {
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.payload = payload;
        this.published = false;
    }
}
```

Simple. Mirrors the table. The magic is in how it's used.

### The Service Layer: Transactional Boundary

```java
@Service
@Slf4j
public class OrderService {

    @Autowired private OrderRepository orderRepo;
    @Autowired private OutboxEventRepository outboxRepo;
    @Autowired private ObjectMapper objectMapper;

    /**
     * The @Transactional annotation creates a database transaction.
     * Everything in this method succeeds together or fails together.
     * No event is published unless the order is saved.
     * No order is saved unless the event is queued.
     */
    @Transactional
    public Order createOrder(CreateOrderRequest request) {

        // Step 1: Create and persist the domain entity
        Order order = new Order(
            request.getCustomerId(),
            request.getItems(),
            request.getShippingAddress()
        );
        Order savedOrder = orderRepo.save(order);
        log.info("Order persisted: id={}", savedOrder.getId());

        // Step 2: Create the event payload
        OrderCreatedEvent domainEvent = new OrderCreatedEvent(
            savedOrder.getId(),
            savedOrder.getCustomerId(),
            savedOrder.getTotalAmount(),
            savedOrder.getItems(),
            savedOrder.getCreatedAt()
        );

        // Step 3: Serialize it to JSON
        String payload;
        try {
            payload = objectMapper.writeValueAsString(domainEvent);
        } catch (JsonProcessingException e) {
            // If we can't serialize, the entire transaction fails
            // This is correct behavior—don't create orders we can't communicate
            throw new RuntimeException("Failed to serialize event", e);
        }

        // Step 4: Insert the event into the outbox
        // This row is inserted to the SAME transaction as the order
        OutboxEvent outboxEvent = new OutboxEvent(
            "OrderCreated",
            savedOrder.getId().toString(),
            payload
        );
        outboxRepo.save(outboxEvent);
        log.info("Event queued in outbox: id={}", outboxEvent.getId());

        // Step 5: Return
        // When this method exits, Spring commits the transaction
        // Both the order and the outbox event are now durable
        return savedOrder;
    }
}
```

**Key insight**: The `@Transactional` boundary is the contract. Everything inside commits together. If there's a constraint violation, a network timeout, or any other error during the method, both the order and the event are rolled back. The caller gets an exception. No inconsistency is possible.

### The Poller: Event Publication

```java
@Component
@Slf4j
public class OutboxPoller {

    @Autowired private OutboxEventRepository outboxRepo;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    /**
     * Runs every 100ms.
     * This frequency is a tunable trade-off:
     * - 50ms: lower latency, more DB load
     * - 500ms: higher latency, less load
     * 100ms is a sweet spot for most systems (under 200ms SLA).
     */
    @Scheduled(fixedDelay = 100, timeUnit = TimeUnit.MILLISECONDS)
    public void pollAndPublish() {

        // Fetch unpublished events in batches
        // Batch size prevents us from loading millions of rows into memory
        List<OutboxEvent> unpublished = outboxRepo.findByPublishedFalse(
            PageRequest.of(0, 100)  // Load 100 at a time
        );

        if (unpublished.isEmpty()) {
            return;  // Nothing to do, exit early
        }

        log.debug("Polling outbox: found {} unpublished events", unpublished.size());

        for (OutboxEvent event : unpublished) {
            try {
                // Send to Kafka
                // Key = aggregate_id ensures ordering per order
                // Topic = event_type ensures events go to the right subscribers
                SendResult<String, String> result = kafkaTemplate.send(
                    event.getEventType(),           // topic
                    event.getAggregateId(),         // key (partition routing)
                    event.getPayload()              // value (the event)
                ).get(5, TimeUnit.SECONDS);         // wait up to 5 seconds

                // If we get here, Kafka accepted it
                log.info("Event published: type={}, aggregateId={}, partition={}",
                    event.getEventType(),
                    event.getAggregateId(),
                    result.getRecordMetadata().partition()
                );

                // Mark as published
                event.setPublished(true);
                event.setPublishedAt(LocalDateTime.now());
                outboxRepo.save(event);

            } catch (TimeoutException e) {
                // Kafka didn't respond within 5 seconds
                log.warn("Timeout publishing event {}: will retry on next poll",
                    event.getId());
                // The event stays in the database with published=FALSE
                // Next polling cycle will try again

            } catch (Exception e) {
                // Network error, broker down, serialization issue, etc.
                log.warn("Failed to publish event {}: {} (will retry next cycle)",
                    event.getId(), e.getMessage());
                // Again, event stays in database. Will retry.
            }
        }
    }
}
```

**Why this design**:
- **Polling instead of push**: We control the rate. If Kafka is slow, we back up in the database (which we have space for). We don't run out of in-memory queues.
- **Batch processing**: 100 events at a time balances memory and throughput.
- **Timeout handling**: 5 seconds gives Kafka time to respond without hanging forever.
- **Failed events stay unpublished**: No special error handling needed. The next poll cycle will retry automatically.
- **Logging**: Operations can see what's being published and when.

### The Consumer: Idempotency

```java
@Component
@Slf4j
public class OrderEventListener {

    @Autowired private OrderProcessingService processingService;
    @Autowired private ObjectMapper objectMapper;

    @KafkaListener(
        topics = "OrderCreated",
        groupId = "order-processing-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(@Payload String message) {
        try {
            OrderCreatedEvent event = objectMapper.readValue(
                message,
                OrderCreatedEvent.class
            );

            log.info("Received OrderCreated event: orderId={}",
                event.getOrderId());

            /**
             * CRITICAL: Handle duplicates.
             * If the poller publishes an event, and Kafka acknowledges it,
             * but our ack is lost before reaching the poller...
             * Kafka will redeliver the event on consumer restart.
             * Or the poller might retry.
             * Either way, we see the same event twice.
             */
            processingService.processOrderWithIdempotency(
                event.getOrderId(),  // use order ID as dedup key
                event
            );

            log.info("Processed OrderCreated event: orderId={}",
                event.getOrderId());

        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize event: {}", e.getMessage(), e);
            // This is a code bug, not transient. Might warrant a dead-letter queue.

        } catch (Exception e) {
            log.error("Error processing OrderCreated event: {}",
                e.getMessage(), e);
            // Transient error? Retry by not consuming the message.
            // Kafka will redeliver next time.
            throw e;
        }
    }
}
```

**Idempotency is non-negotiable**. Here's why: if our application crashes after Kafka delivers the event but before we finish processing, Kafka will redeliver when we come back up. Or if the poller retries publishing because the ack was slow, we get duplicates. Build your handlers to be idempotent:

```java
@Service
@Slf4j
public class OrderProcessingService {

    @Autowired private OrderProcessingStateRepository stateRepo;
    @Autowired private InventoryService inventoryService;

    /**
     * Use order ID as the idempotency key.
     * If we've already processed this order, return early.
     */
    @Transactional
    public void processOrderWithIdempotency(Long orderId, OrderCreatedEvent event) {

        // Check: have we processed this order already?
        if (stateRepo.existsById(orderId)) {
            log.info("Order already processed: {}", orderId);
            return;  // Idempotent no-op
        }

        // Process the order
        inventoryService.decrementInventory(event.getItems());
        // ... send to warehouse, update accounting, etc.

        // Mark as processed
        stateRepo.save(new ProcessedOrder(orderId));
    }
}
```

This way, even if the event arrives 10 times, the side effects happen only once.

---

## Polling vs. CDC: An Architectural Choice

The pattern I've shown you is **polling-based**. It works well. But there's a more elegant alternative: **Change Data Capture** using Debezium.

### Polling (What We've Built)

Pros:
- Simple. A scheduled task. Easy to understand and debug.
- Works with any database. No special tooling.
- Failure recovery is trivial. Restart the poller, it catches up.

Cons:
- Latency. There's inherent delay between insert and publication (up to the polling interval).
- Database load. Polling is essentially repeated queries. At scale (millions of outbox rows), this adds noise.
- No ordering guarantee across events (each poll cycle is independent).

### CDC with Debezium

Instead of polling, we use a CDC tool to watch the database write-ahead log (or equivalent). When a row is inserted into the outbox table, Debezium detects it almost immediately and publishes it.

Pros:
- Ultra-low latency. Events are published within milliseconds of insertion.
- No repeated polling queries. The database doesn't know we're reading it.
- Ordering is preserved. Events flow through Debezium in the order they were written.

Cons:
- Operational complexity. Debezium is an additional component. It needs Kafka Connect, configuration, monitoring.
- Database specifics. CDC works differently on Postgres (logical replication), MySQL (binlog), Oracle (LogMiner).
- Harder debugging. When something goes wrong, the issue is often in Debezium's state management.

### When to Choose Each

**Use polling** if:
- Your latency requirement is under 500ms.
- You value simplicity and operational ease.
- Your outbox volume is modest (< 10K events/second).

**Use CDC (Debezium)** if:
- You need sub-100ms latency.
- You have tens of thousands of events per second (polling becomes too much DB load).
- You already run Kafka Connect in your infrastructure.
- You're willing to invest in an additional component's operational knowledge.

Most teams start with polling. It's simpler. As you scale, you'll naturally graduate to CDC.

---

## Production Realities

Textbooks don't cover the stuff that keeps you up at night. Here are the real-world considerations.

### Outbox Table Growth

Your outbox table will grow. If you publish 1000 events/second and your events are 1KB each, you're adding 1GB per 1000 seconds (about 17 minutes). Over a day, that's ~50GB.

**Solution**: **Archive and delete old published events**.

```sql
-- Daily batch job
DELETE FROM outbox
WHERE published = TRUE
  AND published_at < DATE_SUB(NOW(), INTERVAL 7 DAYS)
LIMIT 1000000;  -- Delete in chunks to avoid locking
```

This keeps the table size bounded. Adjust the retention window based on your compliance needs. If you need an audit trail, use triggers to move rows to an `outbox_archive` table before deleting.

### Published Lag Monitoring

The most dangerous metric is: **how many unpublished events are sitting in the outbox right now?**

If this number grows over time, it means:
- The poller is slower than events are being produced (you need to scale the poller or batch size).
- Kafka is unhealthy (network, broker, or topic configuration issues).
- The database is slow (queries are taking too long).

**Set up an alert**:

```java
@Component
@Scheduled(fixedDelay = 60000)  // Every minute
public void monitorOutboxLag() {
    long unpublishedCount = outboxRepo.countByPublishedFalse();

    if (unpublishedCount > 10000) {
        metrics.recordOutboxLag(unpublishedCount);
        log.warn("Outbox lag is high: {} unpublished events", unpublishedCount);
        alerting.notify("Outbox lag > 10K");
    }
}
```

### What Happens When the Poller Dies

If your poller process crashes and stays down for an hour:
- Events accumulate in the outbox table. ✓ They're safe.
- Kafka subscribers wait. They might timeout or trigger circuit breakers. ✗
- The lag metric grows. ✓ Operations notice.
- When the poller restarts, it catches up from where it left off. ✓ All events publish.

The key: **your data is never lost**. The lag might be annoying, but nothing is dropped.

### Ordering Guarantees

If you want events for the same order to be processed in order, use the `aggregate_id` as the Kafka partition key. Kafka guarantees that messages with the same key go to the same partition, and partitions maintain order.

But: **if you publish multiple event types (OrderCreated, PaymentProcessed, OrderShipped), they might not process in order** if they go to different topics. If ordering across event types is critical, use a single topic with different event types in the payload.

### Consumer Offset Management

Kafka consumers track their progress using offsets. If a consumer crashes after processing an event but before committing the offset, it will reprocess that event on restart. This is why idempotency isn't optional—it's architectural.

If you want different consumption guarantees (e.g., "exactly once"), configure your consumer group's offset commit strategy. But understand the trade-offs: exactly-once semantics often require distributed transactions, which bring you back to the problems we're solving!

---

## Interview Tip

**Setup**: You're interviewing for a senior or principal engineer role. You explain the Outbox Pattern, and the interviewer asks: "But doesn't polling add latency?"

**Your answer**:

*"Yes, polling does add latency. That's a trade-off, not a bug. Here's the thing: the alternative—trying to coordinate the database and message broker in real time—is actually more complex and less reliable.*

*The Outbox Pattern buys us something more valuable than low latency: it buys us correctness. We get atomic writes. No dual-write inconsistencies. No phantom events. It's simple to understand and operate.*

*Latency depends on the polling interval. If you poll every 100ms, the average latency is 50ms. If you need lower latency—say, under 20ms—then yes, CDC with Debezium makes sense. But that's an optimization you apply after you've validated that polling's latency is actually a bottleneck.*

*I've seen teams reach for CDC early thinking it's more elegant, and then spend six months debugging Debezium state management issues. I've also seen teams poll their outbox at 10K events/second without issues. The choice depends on your SLAs and operational capacity, not on what sounds more sophisticated."*

**Bonus answer if they ask about idempotency**:

*"Duplicates are actually expected. The poller might retry publishing if the ack is slow. Kafka might redeliver if the consumer crashes. Your consumers must be idempotent. I'd implement that using the aggregate ID as a deduplication key—check if you've already processed this ID, and if so, return early. This isn't an edge case; it's the normal operating mode of the system."*

---

## See Also

- [Event-Driven Design](./README.md) — Overview and navigation
- [Saga Pattern](./08-saga-pattern.md) — Distributed transactions using choreography and orchestration
- [Delivery Semantics](./05-delivery-semantics.md) — At-least-once and idempotent consumers
