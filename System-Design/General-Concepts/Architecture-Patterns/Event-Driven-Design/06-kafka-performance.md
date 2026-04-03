# Kafka Performance Tuning: An Architect's Incident Guide

Kafka performance isn't about making it fast—it's about understanding what "fast" means for YOUR system and tuning accordingly. After you've responded to three production incidents in a week where lag crept from 5 minutes to 2 hours, you learn to think about this differently.

→ Back to [Event-Driven Design](./README.md)

---

## The Performance Mindset

Here's what I've learned: most Kafka performance problems aren't mysteries. They follow patterns. Your job as an architect is to:

1. **Know your SLA.** Not a guess—actual numbers. Is this system a real-time payment alerting system (latency matters to the microsecond) or an analytics pipeline (throughput is king)?
2. **Measure relentlessly.** Consumer lag in seconds, not messages. Rebalance frequency. Commit failures. GC pauses. The numbers tell the story.
3. **Tune systematically.** Every knob has a purpose. Change one thing. Measure. Move on.

The architects who panic in the incident room are the ones who don't understand their baseline. The ones who stay calm have dashboards already running.

---

## Throughput vs. Latency: The Fundamental Architect's Trade-off

This is where most engineers get stuck. They see the word "Kafka" and think "fast." But fast at what? For whom?

**Here's my mental model:**

If you're building a **payment alerting system**, latency is everything. A fraud alert that arrives 10 minutes late is worthless. You need that notification in 100ms. You're willing to sacrifice throughput to get there.

If you're building an **analytics pipeline**—counting user events, building daily reports, feeding a data warehouse—throughput wins. Processing 100K events/sec with 5-second latency beats processing 10K events/sec with 100ms latency. The data warehouse doesn't care about seconds; it cares about volume.

Most production systems land **somewhere in between**—what I call "throughput-optimized with latency bounds." Process as many messages as you can, but don't let any single message wait more than 500ms.

Here's how those choices cascade through your configuration:

| Scenario | Philosophy | Producer Config | Consumer Config | Result |
|----------|-----------|-----------------|-----------------|--------|
| **Payment alerts (latency obsessed)** | Minimize time-to-consumer | batch.size=1, linger.ms=0, acks=all | fetch.min.bytes=100, max.poll.records=10 | ~50-100ms E2E, ~1K msgs/sec per consumer |
| **Analytics pipeline (throughput obsessed)** | Maximize messages-per-second | batch.size=64KB, linger.ms=100, acks=1, compression=snappy | fetch.min.bytes=100KB, max.poll.records=1000 | ~5K msgs/sec per consumer, 5-10s latency accepted |
| **Most production systems (balanced)** | Handle volume responsibly | batch.size=32KB, linger.ms=10, acks=1, compression=lz4 | fetch.min.bytes=10KB, max.poll.records=500 | ~2-3K msgs/sec per consumer, 200-500ms latency |

Your job in the interview (or on the whiteboard) is to ask: "What's the SLA?" Until you know that, you're just guessing.

---

## Producer Tuning: The Batching Story

Let me walk you through what actually happens when a message hits your producer.

A client calls `send(record)`. That record doesn't immediately fly to Kafka. Instead, it lands in a **buffer**. The producer holds it there, waiting for one of two things:

1. **The batch fills up** (batch.size reached), OR
2. **Time passes** (linger.ms expired)

Whichever comes first. Then—and only then—does the producer compress the batch and send it to the broker.

**Why does this matter?** Because tuning these two knobs defines your latency/throughput tradeoff at the source.

### The Math: What Your Producer Is Actually Doing

Let's say you configure:
- **batch.size = 32KB** (32,768 bytes)
- **linger.ms = 10** (wait up to 10 milliseconds)
- **compression.type = snappy**
- Messages are 1KB each

Here's the timeline:

```
t=0ms: Message 1 arrives (1KB). Buffer has 1KB. Not full. Waiting.
t=2ms: Message 2 arrives (1KB). Buffer has 2KB. Still waiting.
t=4ms: Message 3 arrives (1KB). Buffer has 3KB. Still waiting.
...
t=10ms: Timer fires. 32 messages accumulated (32KB). COMPRESS. SEND.
       OR
t=9ms: Message 32 arrives. Buffer full (32KB). COMPRESS. SEND IMMEDIATELY.
```

If messages arrive faster than they accumulate, you hit the linger.ms timeout. If they arrive slowly, you hit batch.size. Either way, you're grouping messages efficiently.

**With snappy compression**, that 32KB batch (32 messages × 1KB) compresses to roughly 8-10KB on the wire. You've reduced network I/O by 70%.

**The latency cost?** That 10ms linger. If you have an SLA of 100ms end-to-end, 10ms on the producer is acceptable. If your SLA is 10ms total, you're in trouble.

### Producer Tuning by Use Case

**For Maximum Throughput (Analytics, Logging, Events):**
- **batch.size**: 64KB (default 16KB)—every extra KB batched = less overhead
- **linger.ms**: 100 (default 0)—wait longer, batch bigger
- **compression.type**: snappy (CPU fast, compression ratio ~40-50%) or lz4 (fastest)
- **acks**: 1 (leader ack only, not ISR)—don't wait for replicas
- **buffer.memory**: 128MB (default 32MB)—keep more in flight
- **retries**: 2147483647 (infinite, effectively)
- **max.in.flight.requests.per.connection**: 5 (default)

This setup pushes 5-10K msgs/sec/producer easily.

**For Low Latency (Alerts, Real-time Dashboards):**
- **batch.size**: 1 (send immediately)—or leave at 16KB but
- **linger.ms**: 0 (no waiting)—every millisecond counts
- **acks**: all (or acks=1 + in.sync.replicas=2 for durability without the full wait)
- **compression.type**: none (CPU cycles > network savings at low volume)
- **buffer.memory**: 32MB (default)—smaller = less GC pressure
- **retries**: 3
- **max.in.flight.requests.per.connection**: 1 (enforces ordering)

This setup delivers ~100ms E2E latency with modest throughput (1-2K msgs/sec).

**Pro tip:** Don't just increase acks to "all" for durability. That waits for ALL in-sync replicas before returning success. For most systems, acks=1 + proper replication factor (3) is plenty durable and much faster. The replica writes happen async anyway.

---

## Consumer Tuning: The Polling Dance

The consumer has a lifecycle. Understanding it is the difference between a smooth system and one that thrashes.

**Stage 1: Initialize**
Consumer joins the group. Rebalancing happens. Partitions get assigned. You've lost ~5-10 seconds of processing time already.

**Stage 2: The Polling Loop**
```
while (running) {
  records = consumer.poll(fetch.max.wait.ms);
  process(records);
  consumer.commitAsync();
}
```

This loop repeats forever. The speed of this loop—how many messages per loop, how much CPU per message—determines your throughput.

**Stage 3: Session Management**
Every `heartbeat.interval.ms`, the consumer sends a "I'm alive" message to the broker. If the broker doesn't hear from the consumer for `session.timeout.ms`, it assumes the consumer died and rebalances.

### The Knobs That Matter

**fetch.min.bytes** (default 1 byte)
- The broker won't return a poll() response until it has at least this many bytes.
- Set to 1KB or higher: fewer polls, more batching, better throughput
- Set to 100 bytes: more frequent polls, lower latency but more CPU

Example: With 1KB messages, fetch.min.bytes=10KB means you batch at least 10 messages per fetch. That's efficient. But it also means a poll() might wait 100-200ms for 10 messages to arrive.

**fetch.max.wait.ms** (default 500ms)
- If the broker has fewer bytes than fetch.min.bytes, wait this long anyway before returning.
- This is your "give up and return what you have" knob.
- Lower this for lower latency (100-200ms). You send more batches but process faster.

**max.poll.records** (default 500)
- Don't return more than this many records per poll().
- This is a circuit breaker. If you set it to 10,000, you might process 10,000 records in one loop before committing. That's dangerous (long GC pause, if one fails they all fail).
- For throughput: 500-1000. For stability: 100-200.

**The Session Timeout Trinity:**

These three work together:

1. **session.timeout.ms** (default 10s): "How long can the consumer disappear before I assume it's dead?"
2. **heartbeat.interval.ms** (default 3s): "How often do I send a heartbeat?"
3. **max.poll.interval.ms** (default 5min): "How long can a single poll() + processing take before I assume the consumer is stuck?"

**The math you need to remember:**
```
heartbeat.interval.ms < session.timeout.ms / 3

Example: heartbeat=3s, session=10s works (3 < 10/3? No, but 3*3 < 10, so you get 3 heartbeats before session expires—good).
```

If your polling loop takes 30 seconds (because you're processing 1000 records in a for loop), you need:
```
max.poll.interval.ms >= 35 seconds (with headroom)
```

If you don't set this high enough, the broker rebalances while you're still processing. Now your partitions get revoked, offset commits fail, and lag spikes. This is a common production incident.

### Consumer Tuning Checklist

For **throughput**:
- fetch.min.bytes = 100KB
- fetch.max.wait.ms = 500ms
- max.poll.records = 1000
- session.timeout.ms = 10s (default is fine)
- heartbeat.interval.ms = 3s (default is fine)
- isolation.level = read_uncommitted (if you don't need exact-once guarantees)

For **latency with stability**:
- fetch.min.bytes = 1KB
- fetch.max.wait.ms = 100ms
- max.poll.records = 100
- session.timeout.ms = 10s
- heartbeat.interval.ms = 2s
- max.poll.interval.ms = 30s (assume processing takes time)

---

## Partition Math: Capacity Planning in the Real World

Let's do a real capacity planning exercise. Your team comes to you and says:

"We need to handle 500K messages/sec, each roughly 1KB. Current system is crawling. What do we need?"

Here's my playbook:

### Step 1: Calculate Throughput Per Partition

A single partition is single-threaded on the broker. One consumer can read from it at a time. What's the max throughput of one partition?

```
Throughput per partition ≈ network bandwidth of single broker / message size
```

In practice, with modern hardware (10Gbps NIC):
- 1KB messages: ~80K msgs/sec per partition (or ~80 MB/sec)
- 100 byte messages: ~800K msgs/sec per partition
- 10KB messages: ~8K msgs/sec per partition

For your 500K msgs/sec at 1KB each:
```
Required partitions = 500K msgs/sec / 80K msgs/sec per partition ≈ 7 partitions minimum
```

But safety margin: use 10-12 partitions. Brokers will thank you.

### Step 2: Calculate Brokers Needed

You have 10 partitions. Each partition needs a leader + replicas.

With replication factor = 3:
- 10 partitions × 3 replicas = 30 partition-replicas total
- Spread across brokers: 30 / 3 brokers = 10 partitions per broker (leadership-wise)

So you need at least **3 brokers**. But again, headroom: 4-5 brokers lets you:
- Tolerate a broker failure without overload
- Rebalance without hitting 100% CPU

### Step 3: Calculate Disk Space

```
Disk per broker = (throughput in MB/s) × (retention in seconds) × (replication factor)

Example:
- 500K msgs/sec × 1KB = 500 MB/sec cluster-wide
- Per broker (5 brokers): 100 MB/sec
- 7-day retention = 604,800 seconds
- Disk = 100 MB/s × 604,800s × 3 (replication) ≈ 180 TB per broker
```

That's huge. You'd likely compress or reduce retention. Compression reduces it 5-10x. Retention to 1 day: ~25 TB per broker. More reasonable.

### Step 4: Network I/O

Replication means every message written to the leader is replicated to all followers.

```
Network I/O = inbound throughput × (replication_factor - 1)
```

With 500 MB/sec inbound and RF=3:
```
Replication I/O = 500 MB/s × 2 = 1000 MB/sec (1 Gbps)
```

A 10Gbps NIC can handle this, but you're eating 10% of capacity just for replication. This is why multi-datacenter Kafka deployments are expensive.

### The Output

For 500K msgs/sec, 1KB messages, 7-day retention:
- **12 partitions** (headroom above 10 minimum)
- **4-5 brokers** (headroom for failures)
- **180 TB total disk** (post-compression, maybe 25-30TB per broker)
- **Replication factor 3** (standard durability)

---

## Consumer Lag: The Architect's Most Important Metric

Consumer lag is not just "how many messages behind are we." It's *time*. It's *direction*. It's your early warning system.

### What Lag Really Means

At any moment:
```
lag_in_messages = latest_offset_in_partition - committed_offset_of_consumer_group
```

This tells you how many unprocessed messages are sitting on the broker.

But that's **not actionable**. You need to convert it to **seconds**:

```
lag_in_seconds ≈ (lag_in_messages × avg_processing_time_per_message_ms) / 1000
```

If you have 100K messages of lag, but your consumer processes 1K msgs/sec:
```
lag_in_seconds = 100K / 1K = 100 seconds = ~1.5 minutes
```

That might be fine for an analytics pipeline (SLA: 1 hour). But for payment alerts (SLA: 5 minutes), you're already in trouble.

### Healthy vs. Concerning vs. Critical

**Healthy lag:**
- Lag is flat or slowly decreasing
- Lag in seconds < 10% of your SLA
- Payment alerts with SLA=5min: lag should be < 30 seconds

**Concerning lag:**
- Lag is growing
- Lag in seconds = 20-50% of your SLA
- For payment alerts: lag is 1-2 minutes and increasing
- Action: Check consumer health. Is processing slow? Are all partitions being consumed?

**Critical lag:**
- Lag >= 80% of SLA or lag is growing rapidly
- For payment alerts: lag > 4 minutes
- Action: Page on-call. Start incident.

### The Real-Time Metric: Lag Growth Rate

One number I watch obsessively: **is lag growing?**

```
lag_delta = current_lag - lag_5_minutes_ago
```

If lag_delta > 0, your consumer is losing the race. Messages are arriving faster than you can process them.

```
If lag_delta > 0 AND growing at > 1K msgs/min, you have roughly 10-20 minutes before SLA breach.
```

This is what triggers my page before lag actually hits the SLA.

### What to Export to Prometheus

```yaml
# The raw metric
kafka_consumer_lag_messages{topic="payment-events", partition="0", group="payment-events-processor"}
  value: 125000

# Converted to seconds (THIS IS THE ONE TO ALERT ON)
kafka_consumer_lag_seconds{topic="payment-events", partition="0", group="payment-events-processor"}
  value: 125  # 125000 messages / 1000 msgs/sec = 125 seconds

# Is it growing?
kafka_consumer_lag_growing{topic="payment-events", partition="0", group="payment-events-processor"}
  value: 1  # Yes, lag > lag_5min_ago

# Per-consumer metrics
kafka_consumer_processing_time_ms{topic="payment-events", consumer_id="payment-processor-1"}
  value: 45  # How long each message takes

# Rebalancing (silent killer)
kafka_consumer_rebalance_count{group="payment-events-processor"}
  value: 3  # Rebalanced 3 times in last hour—investigate why

# Offset commit failures (silent killer #2)
kafka_consumer_commit_failure_rate{topic="payment-events", group="payment-events-processor"}
  value: 0.02  # 2% of commits are failing
```

**The ones I alert on:**
- lag_seconds > 300 (5 minutes, payment alerts SLA)
- lag_growing == 1 for > 5 minutes straight
- rebalance_count > 2 in 10 minutes (thrashing)
- commit_failure_rate > 0.01 (1%)

---

## Incident Response: High Lag Under SLA

It's **2 PM on a Tuesday**. PagerDuty fires.

```
Alert: kafka_consumer_lag_seconds{topic=payment-events} = 600 seconds
SLA: 300 seconds (5 minutes)
```

You're now 10 minutes behind. You have maybe 20 minutes of window before executive escalation. Here's what you do:

### Minute 1: Triage

**Command:**
```bash
kafka-consumer-groups --bootstrap-server prod-kafka-1:9092 \
  --group payment-events-processor \
  --describe
```

**What you're looking for:**
- LAG column: which partitions are behind?
- CONSUMER-ID: are all partitions assigned? (empty means unassigned)

**Example output:**
```
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
payment-events  0          1000000         1125000         125000  consumer-1
payment-events  1          900000          1125000         225000  consumer-2
payment-events  2          800000          1125000         325000  (none)  <-- RED FLAG
```

Partition 2 is unassigned. That's 1/3 of your partitions doing zero work.

**Action:** If there are unassigned partitions, jump to "Fix #3: Rebalance."

If all are assigned but some lag >> others, jump to "Fix #2: Check Key Distribution."

If lag is even across all partitions, jump to "Fix #1: Scale Consumers."

### Minute 3: Check Broker Health

**Metrics to poll:**
```
- Broker CPU: > 80%? (bottleneck)
- Broker network in: is it spiking or flatlined?
  Flatlined = consumers not pulling
  Spiking = brokers can't keep up
- Broker disk I/O: any recent full GC in Kafka logs?
```

**Command:**
```bash
# Check broker logs for errors
tail -100 /var/log/kafka/server.log | grep -i error
```

### Minute 5: Check Consumer Health

**Are your consumer instances alive?**
```bash
# Check consumer process
ps aux | grep payment-events-processor

# Check application logs
tail -100 /var/log/app/payment-events-processor.log | grep -E "ERROR|Exception|timeout"
```

**Look for:**
- Process is dead (OOMKilled, segfault)
- Exceptions in processing loop
- Network timeouts (to database, external APIs)
- Full GC pauses (JVM is pausing for seconds)

### Minute 7: The Decision Tree

**Branch A: Lag is even across partitions, consumers are healthy.**
→ Consumer count < partition count
→ **Scale: Add more consumer instances**

**Branch B: One or two partitions are way behind others.**
→ Key skew or broker issue
→ **Check: Is one broker at 100% CPU? Is key distribution skewed?**

**Branch C: Lag is spiking on all partitions, but consumers look healthy.**
→ Producer is overloaded OR broker is bottleneck
→ **Check: Is inbound throughput > baseline?**

**Branch D: Lag is growing slowly and steadily, consumers are healthy, load is normal.**
→ Processing is slow (application logic, external API, DB query)
→ **Optimize: Profile the consumer processing code**

### The Emergency Fixes (In Order)

**Fix #1: Increase max.poll.records (takes 5 minutes)**

Edit consumer config:
```properties
max.poll.records=2000  # Was 500
```

Restart consumers. Each poll() now grabs 2000 records instead of 500. If processing is parallelizable, you catch up faster.

**Downside:** If processing fails, you lose 2000 records per retry.

**Fix #2: Add Consumer Instances (takes 10 minutes)**

```bash
# Spin up 2 more consumer instances
for i in {3..4}; do
  docker run -d \
    -e KAFKA_BROKERS=prod-kafka-1:9092 \
    -e CONSUMER_GROUP=payment-events-processor \
    my-payment-processor:latest
done
```

Kafka automatically rebalances. Your partitions get redistributed. Now instead of 2 consumers handling 6 partitions (3 each), you have 4 handling 6 (roughly 1.5 each). Throughput doubles.

**Downside:** Rebalancing takes 30-60 seconds (your lag might spike briefly during rebalance, but then it catches up fast).

**Fix #3: Lower fetch.max.wait.ms (takes 2 minutes to deploy)**

```properties
fetch.max.wait.ms=100  # Was 500
```

Consumers poll more frequently. Less batching, but faster feedback. Helps if the issue is that consumers are waiting for batches.

**Fix #4: Check for Poison Messages (takes 10 minutes)**

If a message causes your consumer to hang for 30 seconds (bad regex, external API timeout), lag spikes on that partition.

```bash
# Look at recent logs for stuck processing
tail -200 /var/log/app/payment-events-processor.log | \
  grep -E "started processing|finished processing" | tail -20
```

If you see a message that started processing 10 minutes ago and never finished, you found your poison pill. Skip it:

```java
try {
  processMessage(record);
} catch (Exception e) {
  logger.error("Failed to process, skipping: " + record.value(), e);
  continue; // Skip this message
}
```

Deploy. Lag starts decreasing.

### Root Cause Patterns (Post-Incident)

After you've put out the fire, here's what I always find:

| Pattern | Evidence | Root Cause | Fix |
|---------|----------|-----------|-----|
| **Slow steady growth** | Lag increases ~5K msgs/min | Processing time creeping up | Profile processing logic; cache external API results; add DB indexes |
| **Sudden spike, then plateau** | Lag jumps 100K messages, stays there | Rebalancing or GC pause | Check for rebalance cycles; increase heap; tune GC settings |
| **Uneven lag across partitions** | One partition 500K behind, others 50K | Key skew or slow broker | Check key distribution; if even, slow broker = add brokers |
| **Lag spikes every 30 minutes** | Regular pattern | Hourly batch job consuming resources | Schedule batch during off-peak; reduce batch concurrency |
| **Lag only at peak hours** | Flat during night, climbs during day | Underdimensioned for peak load | Capacity planning; add brokers/consumers |
| **Lag growing with new code deploy** | Lag stable before deploy, growing after | Processing logic regression | Revert; profile diff; fix; redeploy |

---

## Interview Tip

**How to answer the "Kafka performance" question at a senior/principal level:**

"Kafka performance is about understanding your constraint. Are you latency-bound or throughput-bound?

For **throughput** (analytics, logging): I tune on the producer side first—batch.size=64KB, linger.ms=100, compression=snappy. That pushes 5-10K msgs/sec per producer thread. On the consumer side, fetch.min.bytes=100KB and max.poll.records=1000 to batch work efficiently.

For **latency** (alerts, payments): I flip it—batch.size=1, linger.ms=0 on the producer; fetch.min.bytes=1KB, max.poll.records=100 on the consumer. I watch max.poll.interval.ms carefully: if processing takes 30 seconds, I set it to 60 seconds with headroom.

**The real skill is monitoring.** I export consumer lag in *seconds*, not messages. A consumer 10M messages behind at 1M msgs/sec is fine (10 seconds). A consumer with *growing* lag is a problem. I track rebalance frequency (thrashing = session.timeout.ms too aggressive), commit failures, and GC pauses.

**If lag breaches SLA**, my diagnosis is:
1. Check if all partitions are assigned (if not, rebalance)
2. Check if lag is even (if skewed, key distribution issue)
3. Check consumer processing time (is it slow?)
4. Check broker CPU and network (if maxed, scale brokers)
5. Check for poison messages (one slow message blocking the whole partition)

Then I scale. If it's partition-level skew, I repartition or fix key distribution. If it's consumer-level, I add instances. If it's processing-level, I optimize the code. The order matters: horizontal scale (consumers) is fast; optimization is slow."

---

## Related Topics

- [Kafka Internals](./03-kafka-internals.md) — Topics, partitions, ISR, and cluster architecture
- [Kafka Configs](./04-kafka-configs.md) — The configs that drive performance
- [Delivery Semantics](./05-delivery-semantics.md) — How delivery guarantees affect throughput
