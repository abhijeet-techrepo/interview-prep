# Outbox Pattern

Ensures reliable event publishing by storing events in a database outbox table, guaranteeing delivery without dual-write problems.

→ Back to [Event-Driven Design](./README.md)

---

## The Problem: Dual Write Dilemma

| Issue | Impact |
|-------|--------|
| Write to DB, then publish event | Event gets lost if publish fails; inconsistent state |
| Publish event, then write to DB | DB fails; subscribers act on invalid state |
| No transactional guarantee | Both operations must succeed together |
| Message broker down | How do we retry? Data already committed. |

**The core challenge**: Two separate systems (database + message broker) have no atomic transaction guarantee.

---

## How It Works (Step by Step)

```
1. Receive request (Order created)
   ↓
2. Write order to DB + outbox event in SAME transaction
   ↓
3. Transaction commits (all or nothing)
   ↓
4. Separate poller/CDC reads outbox table
   ↓
5. Publishes to message broker (Kafka, RabbitMQ, etc.)
   ↓
6. Mark event as published in outbox
```

---

## ASCII Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ Application Service                                             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ├─→ INSERT INTO orders VALUES (...)
                     │
                     ├─→ INSERT INTO outbox (event_type, payload)
                     │   (within SAME transaction)
                     │
                     └─→ COMMIT
                         │
         ┌───────────────┴───────────────┐
         │                               │
         ↓ (Failure)                      ↓ (Success)
    ROLLBACK BOTH              TRANSACTION COMMITTED
   No data changed        Orders + Events both inserted
                                │
                                ↓
                ┌───────────────────────────┐
                │ Outbox Poller             │
                │ (runs every 100ms)        │
                └───────────────┬───────────┘
                                │
                    SELECT * FROM outbox
                    WHERE published = FALSE
                                │
                                ↓
                    ┌───────────────────────┐
                    │ Message Broker        │
                    │ (Kafka/RabbitMQ)      │
                    └───────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │ (on success)                  │ (on failure)
                ↓                               ↓
        UPDATE outbox               Retry with exponential backoff
        SET published = TRUE        (next polling cycle)
        WHERE id = ...
```

---

## Pros & Cons

| Aspect | Pro | Con |
|--------|-----|-----|
| **Atomicity** | Events guaranteed to persist with business data | Requires DB support for additional table |
| **Reliability** | No message loss; retry logic built-in | Added database write overhead |
| **Eventual Consistency** | Subscribers see changes in order | Slight delay before event published |
| **Failure Resilience** | Poller can retry indefinitely | Network/broker failures can still delay delivery |
| **Simplicity** | Standard pattern; no distributed transactions | Extra operational complexity (monitoring poller) |
| **Idempotency** | Events can be replayed from outbox | Consumers must handle duplicates |

---

## Implementation: Spring + Kafka

### Step 1: Create Outbox Table

```sql
CREATE TABLE outbox (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    payload JSON NOT NULL,
    published BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP NULL,
    INDEX idx_published (published),
    INDEX idx_created (created_at)
);
```

### Step 2: Outbox Event Entity

```java
@Entity
@Table(name = "outbox")
@Getter @Setter @NoArgsConstructor
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

    public OutboxEvent(String eventType, String aggregateId, String payload) {
        this.eventType = eventType;
        this.aggregateId = aggregateId;
        this.payload = payload;
    }
}
```

### Step 3: Service Layer (Transactional Write)

```java
@Service
@Transactional
public class OrderService {

    @Autowired private OrderRepository orderRepo;
    @Autowired private OutboxEventRepository outboxRepo;
    @Autowired private ObjectMapper objectMapper;

    public Order createOrder(CreateOrderRequest request) {
        // Save order
        Order order = new Order(request);
        Order savedOrder = orderRepo.save(order);

        // Publish outbox event in SAME transaction
        OrderCreatedEvent event = new OrderCreatedEvent(
            savedOrder.getId(),
            savedOrder.getCustomerId(),
            savedOrder.getTotalAmount()
        );

        String payload = objectMapper.writeValueAsString(event);
        OutboxEvent outboxEvent = new OutboxEvent(
            "OrderCreated",
            savedOrder.getId().toString(),
            payload
        );
        outboxRepo.save(outboxEvent);

        return savedOrder;
        // Transaction commits here — both writes succeed or both rollback
    }
}
```

### Step 4: Outbox Poller (Scheduled)

```java
@Component
@Slf4j
public class OutboxPoller {

    @Autowired private OutboxEventRepository outboxRepo;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 100, timeUnit = TimeUnit.MILLISECONDS)
    public void pollAndPublish() {
        List<OutboxEvent> unpublished = outboxRepo
            .findByPublishedFalse(PageRequest.of(0, 100));

        for (OutboxEvent event : unpublished) {
            try {
                // Publish to Kafka
                kafkaTemplate.send(
                    event.getEventType(),
                    event.getAggregateId(),
                    event.getPayload()
                ).get(5, TimeUnit.SECONDS);

                // Mark as published
                event.setPublished(true);
                event.setPublishedAt(LocalDateTime.now());
                outboxRepo.save(event);

                log.info("Published event: {} (id={})",
                    event.getEventType(), event.getId());

            } catch (Exception e) {
                log.warn("Failed to publish event {}: {}",
                    event.getId(), e.getMessage());
                // Will retry on next poll
            }
        }
    }
}
```

### Step 5: Consumer Handles Duplicates (Idempotency)

```java
@Component
@Slf4j
public class OrderEventListener {

    @Autowired private OrderProcessingService service;

    @KafkaListener(
        topics = "OrderCreated",
        groupId = "order-service",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(
        @Payload String message,
        @Header(KafkaHeaders.RECEIVED_KEY) String key
    ) {
        try {
            OrderCreatedEvent event = objectMapper
                .readValue(message, OrderCreatedEvent.class);

            // Idempotency: use order ID as key
            service.processOrderWithIdempotencyKey(
                event.getOrderId(),
                event
            );
        } catch (Exception e) {
            log.error("Error processing OrderCreated event: {}", e);
            // Dead-letter queue or retry
        }
    }
}
```

---

## Key Considerations

> [!NOTE]
> **Idempotency is Critical**: Consumers may receive the same event multiple times. Use idempotency keys (aggregate ID, request ID) to deduplicate.

> [!IMPORTANT]
> **Monitoring the Poller**: Track published lag. If unpublished events grow, poller is slow or broker is down. Set up alerts.

> [!TIP]
> **CDC Alternative**: Instead of polling, use Change Data Capture (Debezium) to detect outbox inserts and publish automatically. Lower latency, reduced polling overhead.

> [!WARNING]
> **Database Performance**: Outbox table can grow quickly. Archive/delete old published events regularly (retention policy).

---

## Comparison with Alternatives

| Pattern | Use Case | Trade-off |
|---------|----------|-----------|
| **Outbox Pattern** | Database + message broker | Simple, reliable, slight delay |
| **Saga Pattern** | Long-running transactions | Eventual consistency, complexity |
| **Event Sourcing** | Complete audit trail needed | Higher storage, event replay logic |
| **Transactional Outbox + CDC** | Low-latency event publishing | Setup complexity, requires Debezium |

---

## Interview Tip

**Ready-to-say answer for senior/principal level:**

*"The Outbox Pattern solves the dual-write problem by ensuring events are persisted atomically with your business data. Here's the key insight: instead of trying to coordinate a database write and a message broker publish—which have no distributed transaction—we write both the domain entity and an event record to the same database in a single transaction.*

*The actual event publication happens asynchronously. A poller (or CDC tool like Debezium) reads the outbox table and publishes unpublished events to the message broker. If publishing fails, the event stays in the outbox and we retry on the next cycle. If the poller dies, the events aren't lost—they're sitting in the database.*

*The trade-off is a slight delay between when something happens and when subscribers are notified. But we gain strong guarantees: no lost events, no dual-write inconsistencies, and the pattern works with any message broker.*

*In Spring + Kafka, I'd implement it with a scheduled poller that runs every 100ms, checks for unpublished events, publishes them, and marks them published. The consumer needs to be idempotent because events can be published multiple times if the polling cycle retries. Real-world tip: monitor the outbox table size and set up alerts if published lag grows—means your poller is slow or the broker is having issues."*

---

## See Also

- [Event-Driven Architecture](./README.md)
- [Saga Pattern](./05-saga-pattern.md)
- [Event Sourcing](./06-event-sourcing.md)
- [Change Data Capture (Debezium)](./08-change-data-capture.md)
