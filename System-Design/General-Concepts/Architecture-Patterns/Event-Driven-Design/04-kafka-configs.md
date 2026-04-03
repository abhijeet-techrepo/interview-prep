# Kafka Configurations: An Architect's Field Guide

Kafka's defaults are optimized for *getting started*, not for production. An architect's job is knowing which knobs to turn and *why*—the difference between a system that barely limps along and one that handles millions of events per second without losing a single message. This guide walks through every critical configuration decision you'll face, grounded in real production tuning from financial systems and high-volume analytics platforms.

→ Back to [Event-Driven Design](./README.md)

---

## The Config Philosophy

Every Kafka configuration is a tradeoff. You're balancing:
- **Durability** vs. **throughput**: Waiting for all replicas to acknowledge writes (safe) slows you down. Fire-and-forget (fast) loses data.
- **Latency** vs. **efficiency**: Sending small batches immediately (low latency) hammers the network. Batching larger chunks (efficient) adds delay.
- **Consistency** vs. **availability**: Strict offset management catches failures but slows down. Loose management is faster but forgives loss.

The trick is knowing *which* tradeoff matters for *your* system. A financial payment system has zero tolerance for data loss but can accept 100ms latency. A real-time notification system is the opposite: losing an analytics event is fine, but 2-second delays are unacceptable.

---

## Broker/Admin Configurations: The Foundation

Your brokers are the guardians of your data. Every decision here ripples through your entire ecosystem.

### Durability: Protecting Data at Rest

When a producer sends a message, it doesn't disappear into thin air—it gets written to *replicas*. The question is: how many, and how synchronized must they be?

We once ran a payment service at a fintech company. The team set `default.replication.factor=1` to save storage costs. Six months in, we lost a broker in a lightning strike. Every message since the last backup—gone. The compliance audit was brutal. That's when we learned: durability isn't optional, it's table stakes.

**Replication Factor** tells Kafka how many copies to maintain. The default is `1`. That's one copy—if the broker dies, the data dies with it. For production, you want `3`: one leader, two followers. If the leader dies, a follower takes over. If a different broker dies, you still have two copies.

**Min In-Sync Replicas (min.insync.replicas)** is the critical safety valve. When a producer sends a message with `acks=all`, the broker waits for this many replicas to acknowledge *before* confirming to the producer. If you set `min.insync.replicas=1` with `replication.factor=3`, you have three copies on disk but only one needs to acknowledge—if that one copy is the only one that hasn't caught up to the others and it dies, the message is lost.

The formula: set `min.insync.replicas = replication.factor - 1`. This means you can tolerate one broker failure without losing data. For a 3-replica setup, use `min.insync.replicas=2`.

**Unclean Leader Election** is a trap. When a leader dies, Kafka picks a new leader from the in-sync replicas. But what if *all* in-sync replicas are down and an out-of-sync replica is still up? Normally, Kafka blocks until an in-sync replica recovers. If you enable `unclean.leader.election=true`, it promotes the out-of-sync replica anyway. You gain availability but *lose durability*—the out-of-sync replica might be missing messages that the old leader had already acknowledged to producers. For anything remotely important, leave this `false`.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `default.replication.factor` | `1` | `3` | One copy means one failure kills your data. Three copies tolerate two simultaneous failures. Essential for HA. |
| `min.insync.replicas` | `1` | `2` | Set to `replication.factor - 1` so the producer only gets confirmation when the message is safely on disk on at least 2 brokers. |
| `unclean.leader.election.enable` | `true` | `false` | Disables promoting out-of-sync replicas. Sacrifices availability for durability—the right call for systems that can't lose data. |

### Retention & Cleanup: Managing the Data Lifecycle

Kafka doesn't delete messages by default. They sit there forever, accumulating on disk until you explicitly set a retention policy. For some systems, that's intentional—event sourcing requires years of history. For others, it's wasteful.

Retention can work two ways: **time-based** (keep messages for N days) or **size-based** (keep the last N GB). In practice, you almost always use time-based.

We ran an analytics pipeline that ingested clickstream data. The team didn't set any retention, and after three months the broker filled up and crashed. They didn't even realize—retention defaults to *infinite*. Now we always ask: "How long do you need this data?"

**Log Retention Hours** is the main dial. The default is 7 days (168 hours). For an order event stream, 7 days might not be enough if you batch reconcile reports weekly—set it to 30 days. For analytics, you might want 1-2 days to save disk. For event sourcing (replaying your entire domain from events), you want years.

**Log Segment Bytes** is how large Kafka lets a single log file grow before rolling over to a new one. Default is 1 GB. This affects how frequently Kafka checks for retention: the retention cleanup runs per segment, not per message. If you set segment size too large (e.g., 10 GB), retention cleanup runs infrequently. If you set it too small (e.g., 1 MB), you have thousands of tiny files and disk I/O overhead.

**Log Retention Check Interval** is how often the broker looks at old segments and deletes them. Default is 5 minutes. Unless you're cleaning up gigabytes per hour, leave it alone.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `log.retention.hours` | `168` (7 days) | `7` to `720` (depends on use case) | 7 days is a safe default. For audit logs, use 365 (1 year). For real-time analytics, use 1–2. Event sourcing? Use 2160 (90 days) or more. |
| `log.segment.bytes` | `1073741824` (1 GB) | `1073741824` | Bigger segments = fewer files = less cleanup overhead, but slower to delete at retention boundary. 1 GB is reasonable for most workloads. |
| `log.retention.check.interval.ms` | `300000` (5 min) | `300000` to `600000` | Stick with default unless you're managing terabytes per hour. Higher = less frequent cleanup overhead. |

### Governance: Auto-Create Topics and Deletion

By default, Kafka auto-creates topics when a producer tries to publish to one that doesn't exist. Sounds convenient. In production, it's chaos.

A junior engineer misconfigured a service, and it started publishing to topics named `events`, `event`, and `Events` (capitalization matters). Now the Kafka cluster had 47 topics instead of 3, all but 3 were garbage, and debugging took hours.

**Auto Create Topics Enable** should be `false` in production. Force teams to explicitly create topics through a config management system or API. This ensures naming conventions, proper replication factors, and partition counts are set upfront.

**Delete Topic Enable** should usually stay `true`, but pair it with governance: only cluster admins have permission to delete topics, and you maintain a change log of deletions.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `auto.create.topics.enable` | `true` | `false` | Auto-creation is convenient during development but a nightmare in production. Require explicit topic provisioning. |
| `delete.topic.enable` | `true` | `true` (with RBAC) | Deletion is safe as long as you have role-based access control. Confirm your policy before setting. |

---

## Producer Configurations: Sending Messages Safely

When you call `producer.send()`, a chain of decisions fires:
1. Is this message idempotent (can it be safely retried)?
2. How many replicas must acknowledge before you consider it "sent"?
3. Should this batch of messages be compressed?
4. Can it wait for other messages to batch with, or does it send immediately?

Get these wrong and you'll either lose data (silently), duplicate data (silently), or destroy throughput.

### The Durability Trinity: acks, idempotence, and retries

**acks** controls how the producer defines "sent."

- `acks=0` (fire-and-forget): The producer doesn't wait for any acknowledgment. The broker received it? Great. The broker lost it before writing to disk? Producer doesn't know. Use this only for metrics/telemetry where losing 0.1% is acceptable.
- `acks=1` (leader ack): The leader writes to memory (not disk) and tells the producer "got it." If the leader crashes before flushing to disk, the message is lost. If a follower is out of sync and becomes leader, you might see duplicates. This is the default and it's misleading—it feels safe but isn't.
- `acks=all` (all replicas ack): The producer waits until all in-sync replicas have written to disk *and* the leader has confirmed. This is the safe choice. Latency goes up (typically 10-50ms per message), but data loss is nearly impossible unless the entire cluster fails.

We ran a payment service with `acks=1`. In six months, we had 47 transactions that the customer saw as "paid" but never actually cleared. The payment processor thought they were retries, the merchant thought they were missing. The subsequent investigation cost more than the payments were worth.

**enable.idempotence** prevents duplicate messages on retry. Here's the scenario: you send a message with `acks=all`. The broker writes it to all replicas and sends back the confirmation. But the confirmation packet gets lost in the network. The producer times out, assumes failure, and retries. The message is sent *twice*, but the producer thought it was a failure and retry. With idempotence enabled, Kafka tracks a "producer session" and message sequence numbers. If it sees the same message number twice from the same producer, it deduplicates.

Idempotence requires `acks=all` (the producer must wait for replicas to confirm) and `max.in.flight.requests ≤ 5` (to prevent out-of-order issues with retries).

**retries** tells the producer how many times to retry a failed send. Modern Java clients default to `Integer.MAX_VALUE` (retry forever with exponential backoff). That's usually right—if the broker is temporarily down, retry. If it's down for 5 minutes, you'll have a backlog, but no lost messages. If you set `retries=0`, a temporary network glitch loses the message. If you set `retries=3` with a quick timeout, you lose messages when the cluster is under load.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `acks` | `1` | `all` for durability; `1` for high-throughput non-critical data | `acks=1` feels safe but allows silent data loss if the leader crashes. Use `all` for payments, orders, anything a user will ask about. Use `1` for analytics, metrics, logging. |
| `enable.idempotence` | `false` | `true` | Enables exactly-once semantics within a producer session. No downside—turn it on. |
| `retries` | `2147483647` (forever with backoff) | `2147483647` | Retry forever with exponential backoff. Temporary broker issues won't lose messages. Permanent issues fail after the timeout. |
| `retry.backoff.ms` | `100` | `100` to `1000` | Exponential backoff between retries. Higher values reduce load on a struggling broker. |

### Batching: The Throughput Multiplier

Sending one message at a time is slow. Each message = one network round-trip = latency. But if you batch 100 messages together, you make one network round-trip and get 100x better throughput.

The producer batches messages based on **batch.size** (accumulate N bytes) and **linger.ms** (wait up to N milliseconds).

- `batch.size=16KB` (default): Accumulate 16 KB of messages, then send the batch. If you're sending 1 KB messages, that's 16 messages per batch.
- `linger.ms=0` (default): Don't wait. Send as soon as the buffer has *anything*.

The problem: with default settings, you send a 1 KB message immediately without batching. Throughput is terrible.

The fix: increase `linger.ms` to 10–100 ms. This tells the producer: "If a batch isn't full, wait up to 100 ms for more messages to arrive before sending." In a typical system where events are arriving continuously, 100 ms of wait is invisible to users but lets you batch 100–1000 messages together.

We ran a real-time analytics dashboard. The producer was misconfigured with `linger.ms=0`, and each event created a network packet. The NIC on the broker was saturated, and we were only getting 1000 events/sec. When we changed to `linger.ms=100`, we hit 50,000 events/sec on the same hardware.

**compression.type** trades CPU for network I/O. The producer compresses a full batch before sending. Options:
- `none`: No compression. Fast, but uses more network bandwidth.
- `snappy`: Fast compression, good ratio. Sweet spot for most use cases.
- `gzip`: Best compression ratio, slower. Use if bandwidth is the bottleneck.
- `lz4`: Fastest compression. Use if CPU is the bottleneck.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `batch.size` | `16384` (16 KB) | `16384` to `32768` | 16 KB is solid. Increase to 32 KB if you're sending large messages or need max throughput. |
| `linger.ms` | `0` | `10` to `100` | Default sends immediately, destroying batching. Set to 10–100 ms to let the producer wait for a full batch. 10 ms = 100 ms added latency in worst case. |
| `compression.type` | `none` | `snappy` | Use snappy for a good balance of speed and compression. Saves 50–70% bandwidth with minimal CPU cost. |

### Ordering & Concurrency: max.in.flight.requests

Here's a tricky situation: you send message A, then message B. Message A fails (timeout), so the producer retries it. While the retry is in flight, message B is also in flight. What if message B hits the broker first, then message A arrives? You have them out of order.

For most systems, order doesn't matter. But for an order service event stream (Order Created, Payment Captured, Shipment Initiated), order is critical.

`max.in.flight.requests` controls how many messages the producer can have "in flight" (sent to the broker, waiting for ack) before it blocks and waits for an ack.

- `max.in.flight.requests=1`: Only one message in flight at a time. If it fails and retries, no other messages sneak by. Order is guaranteed. But throughput is 1/Nth where N is the number of brokers.
- `max.in.flight.requests=5` (default): Up to 5 messages in flight. Higher throughput, but if message 1 fails and retries, messages 2–5 might already be acked. Messages arrive out of order.

**The key constraint**: If you enable idempotence (`enable.idempotence=true`), you *must* keep `max.in.flight.requests ≤ 5`. This is because Kafka can't track message ordering if you have more than 5 requests in flight—it's a protocol limit. If you need both idempotence and strict ordering, you're forced to set `max.in.flight.requests=1` (and accept the throughput hit).

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `max.in.flight.requests.per.connection` | `5` | `1` (if order matters) or `5` (if throughput matters) | The tradeoff: 1 = strict per-partition ordering but 1/5 throughput. 5 = higher throughput but possible reordering on retry. Use 1 for orders/payments, 5 for events/logs. |

### Advanced: Buffer and Timeout

**buffer.memory** is the total buffer the producer keeps in memory for all pending batches. Default is 32 MB. If the broker is slow or down, batches pile up here. When the buffer fills, `send()` blocks until space opens up.

In a bursty system (lots of events suddenly, then quiet), 32 MB might be too small—batches will fill up and start blocking. In a steady-state system, it's fine.

**request.timeout.ms** is how long the producer waits for an ack before giving up and retrying. Default is 30 seconds. If you set it too low (e.g., 5 seconds), network jitter will cause spurious timeouts and retries. Too high (e.g., 120 seconds) means a broker failure takes 2 minutes to detect.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `buffer.memory` | `33554432` (32 MB) | `33554432` to `67108864` | 32 MB is reasonable for most systems. If you have bursty traffic, increase to 64 MB. |
| `request.timeout.ms` | `30000` | `30000` to `60000` | 30 seconds is standard. Increase only if your brokers are genuinely slow. |

---

## Consumer & Listener Configurations: The Flip Side

A consumer is born, joins a group, gets partitions assigned, polls for messages, processes them, and (if configured right) commits the offset so it can resume from the same place if it crashes.

Every step of this lifecycle is governed by configs.

### The Lifecycle: Joining, Processing, and Timeouts

When a consumer starts:

1. **It joins the consumer group** with a `group.id` (e.g., `order-service-consumer-v1`). The group coordinator (a Kafka broker) assigns partitions to this consumer based on the partitioning strategy.

2. **It sends heartbeats** every `heartbeat.interval.ms` (default 3 seconds) to prove it's alive. If the broker doesn't hear a heartbeat for `session.timeout.ms` (default 45 seconds), it removes the consumer from the group and reassigns its partitions to others.

3. **It calls `poll()`** in a loop, which fetches messages from the broker. Between polls, the consumer must send heartbeats, or it will be considered dead.

4. **If the processing time between polls exceeds `max.poll.interval.ms`** (default 5 minutes), the broker thinks the consumer crashed, even if it's sending heartbeats. It removes it and reassigns partitions.

5. **It commits the offset**, telling Kafka, "I've processed messages up to offset X. If I crash, start me from X+1 next time."

The tricky part is step 4. If you're processing a message for 10 seconds, and `max.poll.interval.ms` is 5 seconds, Kafka will kick you out. The fix: either increase `max.poll.interval.ms` or increase concurrency (process multiple messages in parallel) so you poll more frequently.

### Offset Management: Manual vs. Auto

When a consumer processes a message, it doesn't automatically tell Kafka "I'm done." You have to commit the offset.

With `enable.auto.commit=true` (default), the consumer automatically commits the offset of the last message polled every `auto.commit.interval.ms` (default 5 seconds). Sounds convenient. But what if the consumer crashes after polling message X but before processing it? On restart, it skips to message X+1 and message X is never processed again.

With `enable.auto.commit=false`, you manually commit after you've successfully processed a message. This adds latency (a network round-trip per commit), but guarantees no skipped messages.

In Spring Kafka, the pattern is:

```java
@KafkaListener(topics = "orders", groupId = "order-service")
public void listen(String message, Acknowledgment ack) {
    try {
        processOrder(message);
        ack.acknowledge(); // Only commit if processing succeeded
    } catch (Exception e) {
        // Don't commit; rebalance will replay this message
        throw e;
    }
}
```

We ran an order processing service with auto-commit. A database connection pool exhaustion caused processing to fail on 0.3% of messages. But since they were auto-committed, the orders never got retried. The payments came in, the orders didn't—we lost $40K in a morning before someone noticed.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `enable.auto.commit` | `true` | `false` | Auto-commit is convenient and loses messages on crash. Manual commit is explicit: process, then commit. Always choose manual for anything important. |
| `auto.commit.interval.ms` | `5000` | N/A (use manual commit) | If you must use auto-commit, set to 1000 for more frequent commits. But really, don't use auto-commit. |

### The Heartbeat & Timeout Trinity

**heartbeat.interval.ms** (default 3 seconds): How often the consumer sends "I'm alive" signals. Must be less than `session.timeout.ms`.

**session.timeout.ms** (default 45 seconds): If the broker doesn't hear a heartbeat for this long, it assumes the consumer crashed and reassigns its partitions.

**max.poll.interval.ms** (default 5 minutes): If the consumer doesn't call `poll()` within this time, it's considered dead, even if it's sending heartbeats.

The rule: `heartbeat.interval.ms` should be about 1/3 of `session.timeout.ms`. If `session.timeout.ms=45`, set `heartbeat.interval.ms=15`.

And `max.poll.interval.ms` should be as large as your longest message processing can take. If processing messages takes 2 minutes on average and you're processing them one at a time, set `max.poll.interval.ms=180000` (3 minutes). If you're processing in parallel with concurrency=5, you can process 5 messages at once, so set it lower.

A common mistake: set `session.timeout.ms` too high (e.g., 5 minutes) to avoid rebalances. Now if a consumer crashes, it takes 5 minutes to detect, during which its partitions aren't being processed. Better to set `session.timeout.ms=30000` and detect failures quickly.

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `heartbeat.interval.ms` | `3000` | `3000` | Send heartbeats every 3 seconds. Increase only if network is unreliable. |
| `session.timeout.ms` | `45000` | `30000` to `60000` | 45 seconds is reasonable. Too low (10s) = false evictions on GC pauses. Too high (5m) = slow failure detection. |
| `max.poll.interval.ms` | `300000` (5 min) | Depends on processing time | If processing takes 30 seconds per message, set to 60000 (1 minute). If 2 minutes, set to 240000 (4 minutes). Account for GC pauses. |

### Batching & Fetch Configuration

**max.poll.records** (default 500): How many messages are returned per `poll()` call. Lower values = more latency (you poll more often), but you process fewer at once (less memory, easier to handle timeouts). Higher values = fewer polls but more messages to hold in memory.

The tradeoff: if you process 100 messages/second and `max.poll.records=500`, you're polling every 5 seconds. With `max.poll.interval.ms=5000`, you're right at the edge—any GC pause causes a rebalance. If you drop to `max.poll.records=100`, you poll every second, and you have plenty of buffer.

**fetch.min.bytes** (default 1): The broker waits to accumulate this many bytes before responding to a fetch. Higher values = fewer fetch requests, more efficient, but higher latency if the broker doesn't have many messages waiting.

**fetch.max.wait.ms** (default 500): If `fetch.min.bytes` is 1 KB but only 100 bytes are available, wait up to 500 ms for more to arrive. This balances latency and efficiency.

**auto.offset.reset** (default `latest`): If the consumer's offset is invalid (e.g., the offset doesn't exist anymore because messages were deleted), what do you do?

- `latest`: Skip to the latest offset. New messages will be processed, old ones ignored. Use this for non-critical data (metrics, logs).
- `earliest`: Start from the beginning. Use this if you want to replay history.
- `none`: Throw an exception. Use this if you want to explicitly handle the error (maybe it's a bug in your code, not a normal condition).

| Config | Default | Recommended | Story |
|--------|---------|-------------|-------|
| `max.poll.records` | `500` | `100` to `500` | High = fewer polls but more memory. Low = more polls but easier timeout management. For 50-100KB messages, use 100. For small messages, use 500. |
| `fetch.min.bytes` | `1` | `1` | Broker waits to accumulate at least 1 byte before responding. Changing this is rarely necessary. |
| `fetch.max.wait.ms` | `500` | `500` | Balanced default. Increase to 1000 if you're optimizing for efficiency over latency. |
| `auto.offset.reset` | `latest` | `earliest` or `none` | `latest` = skip unknown offsets. Use for non-critical data. `earliest` = replay from start. `none` = fail explicitly. |

### Spring Kafka Specifics

Spring Kafka wraps Kafka consumers and adds its own layer of configuration. The key ones:

**spring.kafka.listener.ack-mode**: How to commit offsets.

- `BATCH`: (default) After processing a batch of messages (max_poll_records), commit. Fast, but if processing fails mid-batch, some messages are lost.
- `RECORD`: After each message, commit. Safe, but one network round-trip per message (latency).
- `MANUAL`: The application calls `acknowledgment.acknowledge()` to commit. You control exactly when and if to commit.
- `MANUAL_IMMEDIATE`: Like `MANUAL`, but commit synchronously (wait for the broker to confirm).

For most production systems, use `MANUAL` and commit only after successful processing.

**spring.kafka.listener.concurrency**: How many threads to use. Each thread processes messages from assigned partitions in parallel. Max concurrency = number of partitions (you can't have more threads than partitions—some will be idle).

If you have 10 partitions and `concurrency=3`, each thread handles ~3 partitions. This parallelizes message processing across your consumer group.

| Config (Spring) | Default | Recommended | Story |
|-----------------|---------|-------------|-------|
| `spring.kafka.listener.ack-mode` | `BATCH` | `MANUAL` | `BATCH` loses messages on failure. `MANUAL` lets you commit only after successful processing. Use `MANUAL` for anything important. |
| `spring.kafka.listener.concurrency` | `1` | `min(3, partition_count)` | 1 thread = 1 partition processed at a time. 3 threads = 3 partitions in parallel. More threads = higher throughput but more contention. |
| `spring.kafka.consumer.enable-auto-commit` | `true` | `false` | Must be `false` if using manual ack mode. |

---

## Config Recipes: Real-World Architectures

Now that you know the knobs, here's how to set them for four common scenarios.

### Recipe 1: High-Throughput Analytics Pipeline

You're ingesting clickstream events at 500K events/sec, each 500 bytes. Durability isn't critical (if you lose 0.01%, analytics is still accurate). Latency doesn't matter (batch analytics runs hourly). You want to maximize throughput.

**Broker Config:**
```
default.replication.factor=2              # 2 replicas; if one broker dies, no data loss
min.insync.replicas=1                      # Only leader ack required; follower can lag
unclean.leader.election.enable=false       # Stay durable
log.retention.hours=2                      # Keep events for 2 days for replays
```

**Producer Config:**
```
acks=1                                     # Leader ack; some loss acceptable
enable.idempotence=false                   # Idempotence has overhead; not needed
batch.size=32768                           # 32 KB; bigger batches for throughput
linger.ms=100                              # Wait 100ms for full batch
compression.type=snappy                    # Compress batches
max.in.flight.requests=5                   # Max throughput
retry.backoff.ms=100
retries=Integer.MAX_VALUE
```

**Consumer Config:**
```
enable.auto.commit=true                    # Auto-commit; we can lose a few events
auto.commit.interval.ms=1000               # Commit every 1 second
max.poll.records=1000                      # Batch large; we have memory
max.poll.interval.ms=300000                # 5 minutes; processing is bursty
session.timeout.ms=45000
heartbeat.interval.ms=3000
```

**Spring Config:**
```
spring.kafka.listener.ack-mode=BATCH
spring.kafka.listener.concurrency=8        # 8 threads; parallelized processing
```

### Recipe 2: Financial/Payment System (Maximum Safety)

You're processing payments. Every message must be durably stored and processed exactly once. Latency is acceptable (payments take minutes anyway). Data loss is unacceptable (regulatory nightmare).

**Broker Config:**
```
default.replication.factor=3               # 3 replicas; tolerate 1 broker failure
min.insync.replicas=2                      # At least 2 replicas ack; 1 can fail silently
unclean.leader.election.enable=false       # Never promote out-of-sync; lose data
log.retention.hours=730                    # 30 days; audit trail
```

**Producer Config:**
```
acks=all                                   # Wait for all replicas; safe
enable.idempotence=true                    # Exactly-once; deduplicates retries
batch.size=16384                           # Standard batching
linger.ms=50                               # 50ms wait for batch; balances safety and throughput
compression.type=gzip                      # Best compression; bandwidth expensive
max.in.flight.requests=1                   # Strict ordering; required for payments
retry.backoff.ms=1000
retries=Integer.MAX_VALUE
```

**Consumer Config:**
```
enable.auto.commit=false                   # Manual commit; explicit control
max.poll.records=10                        # Small batches; easier to handle failures
max.poll.interval.ms=600000                # 10 minutes; payment processing is slow
session.timeout.ms=60000
heartbeat.interval.ms=10000
auto.offset.reset=none                     # Fail if offset is invalid; debug it
```

**Spring Config:**
```
spring.kafka.listener.ack-mode=MANUAL      # Commit only after successful processing
spring.kafka.listener.concurrency=3
```

With this setup, a payment can fail during processing, the message won't be committed, and it'll be retried. No duplicates (idempotence handles them), no losses (multi-replica).

### Recipe 3: Real-Time Notification System (Low Latency)

You're sending push notifications. 100ms latency is acceptable. 2-second latency is not. Losing 0.1% of notifications is fine (users won't sue). You optimize for latency.

**Broker Config:**
```
default.replication.factor=2               # 2 replicas is enough
min.insync.replicas=1
unclean.leader.election.enable=true        # Promote out-of-sync to stay available
log.retention.hours=24                     # 1 day; notifs don't need long history
```

**Producer Config:**
```
acks=1                                     # Leader ack only; faster
enable.idempotence=false                   # Skip the overhead
batch.size=8192                            # 8 KB; smaller for low latency
linger.ms=5                                # 5ms wait; fast batching
compression.type=lz4                       # Fastest compression
max.in.flight.requests=10                  # High concurrency
```

**Consumer Config:**
```
enable.auto.commit=true
auto.commit.interval.ms=500                # Frequent commits
max.poll.records=100                       # Small batches; low latency per message
max.poll.interval.ms=60000                 # 1 minute; notif processing is fast
session.timeout.ms=20000                   # Detect failures quickly
heartbeat.interval.ms=5000
```

**Spring Config:**
```
spring.kafka.listener.ack-mode=BATCH
spring.kafka.listener.concurrency=2
```

### Recipe 4: Event Sourcing with Long Retention

You're building an event store. The entire state of your domain is the history of events—you replay them to reconstruct state. You never delete events, and you replay from the start often. Durability is critical.

**Broker Config:**
```
default.replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false
log.retention.hours=87600                  # 10 years
log.segment.bytes=536870912                # 512 MB; fewer tiny files
log.retention.check.interval.ms=600000     # Check every 10 minutes; heavy retention cleanup
```

**Producer Config:**
```
acks=all
enable.idempotence=true
batch.size=32768
linger.ms=100
compression.type=snappy
max.in.flight.requests=5
```

**Consumer Config:**
```
enable.auto.commit=false
max.poll.records=500
max.poll.interval.ms=600000
auto.offset.reset=earliest                 # Always replay from the start if offset is missing
```

---

## Interview Tip

**Interviewer**: "Walk me through how you'd configure Kafka for a payment processing system."

**Your answer** (showing you understand tradeoffs):

"First, I'd ensure durability: `default.replication.factor=3` so losing one broker doesn't lose data, and `min.insync.replicas=2` so the producer only gets a confirmation when the message is safely on at least 2 brokers. I'd disable `unclean.leader.election` to prevent data loss if all in-sync replicas are down.

For the producer, I'd set `acks=all` to wait for all replicas, and `enable.idempotence=true` for exactly-once semantics—payments can't be duplicated. I'd set `max.in.flight.requests=1` to preserve order per partition (payments must process in order). Idempotence requires all replicas to ack anyway, so ordering is tight.

For batching, `batch.size=16KB` and `linger.ms=50` let me batch efficiently without huge latency spikes—50ms is invisible to a user's payment experience.

For the consumer, I'd absolutely disable `enable.auto.commit` and use manual acknowledgment. After processing a payment successfully (updating the ledger, sending confirmations), the consumer calls `acknowledge()` to commit. If processing fails, the message isn't committed and gets replayed on restart. No duplicates (idempotence catches them), no losses.

I'd set `max.poll.records=10` because payment processing is slow, and I don't want the polling thread to be starved. I'd set `max.poll.interval.ms=600000` (10 minutes) to give processing plenty of time.

With Spring Kafka, I'd use `ack-mode=MANUAL` and `concurrency=3` to process messages from multiple partitions in parallel while maintaining explicit control over commits.

The tradeoff: this setup is slower than a fire-and-forget system (maybe 50–200ms added latency per message), but every payment is durably stored and processed exactly once. For payments, that tradeoff is non-negotiable."

**Why this impresses:**

- You're not just reciting config values; you're explaining the tradeoffs.
- You're linking configs together (idempotence requires acks=all, which affects latency).
- You're grounded in a real domain (payments) and addressing its constraints.
- You're distinguishing between what's safe (acks=all) and what's merely convenient (auto-commit).
- You understand that an architect's job is choosing constraints that match the system's requirements, not just optimizing for throughput.
