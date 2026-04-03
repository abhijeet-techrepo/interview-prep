# Kafka Performance Tuning

Strategies to optimize Kafka for throughput, latency, and resource efficiency in production systems.

→ Back to [Event-Driven Design](./README.md)

---

## Throughput vs. Latency Tradeoff

| Goal | Configuration | Throughput Impact | Latency Impact | Use Case |
|------|---------------|-------------------|----------------|----------|
| **Maximum Throughput** | batch.size=32KB, linger.ms=10 | ↑↑↑ | ↑ | Bulk analytics, logs, events |
| **Low Latency** | batch.size=1, linger.ms=0 | ↓↓ | ↓↓↓ | Real-time alerts, trades, dashboards |
| **Balanced** | batch.size=16KB, linger.ms=5 | ↑↑ | ↓↓ | Most production systems |
| **High Throughput + Moderate Latency** | batch.size=64KB, linger.ms=100 | ↑↑↑ | ↑↑ | Event streaming, ETL pipelines |

---

## Producer Tuning Knobs

### For Maximum Throughput
- **batch.size**: 32KB or 64KB (default 16KB) — batches more messages before sending
- **linger.ms**: 5–100 (default 0) — waits for batches to fill instead of sending immediately
- **compression.type**: snappy or lz4 (default none) — reduces network I/O
- **acks**: 1 (default all) — only leader ack, faster but less durable
- **buffer.memory**: 64MB (default 32MB) — larger buffer for pending messages

### For Low Latency
- **batch.size**: 1 — sends immediately
- **linger.ms**: 0 — no waiting
- **acks**: 1 or all (with in-sync-replicas=2) — depends on durability requirements
- **buffer.memory**: 32MB (default) — smaller to reduce garbage collection pauses

### General Best Practices
- Set **retries** and **max.in.flight.requests.per.connection** to ensure reliability without blocking
- Use **idempotence=true** (acks=all automatically) to prevent duplicate messages
- Increase **num.network.threads** and **socket.send.buffer.bytes** on broker for network-bound workloads

---

## Consumer Tuning Knobs

| Parameter | Throughput Boost | Latency Reduction | Typical Values |
|-----------|------------------|-------------------|-----------------|
| **fetch.min.bytes** | 1KB (batch more) | 100 bytes (pull faster) | 1KB–100KB |
| **fetch.max.wait.ms** | 500–1000 | 100–200 | 100–500ms |
| **max.poll.records** | 500–1000 | 10–50 | 100–500 |
| **session.timeout.ms** | 10s (default) | 6s (faster rebalance) | 6–30s |
| **heartbeat.interval.ms** | 3s (default) | 1–2s | 1–10s |

---

## Partition Math & Scalability

### Determining Partition Count

**Formula:**
```
target_throughput_MB/s
max_consumer_throughput_MB/s = min(partitions_count, consumers_count)
```

**Example:**
- Target: 1,000 MB/s
- Single consumer throughput: 100 MB/s
- Required partitions: 1,000 ÷ 100 = **10 partitions minimum**

### Throughput Calculation per Partition

```
MB/s per partition = (message_size_bytes × messages_per_sec) / (1024 × 1024)
```

**Example:**
- 1 million messages/sec
- 1 KB message size
- MB/s = (1,000,000 × 1) ÷ (1024 × 1024) ≈ **0.95 MB/s per partition**

### Replication & Durability Cost

```
Broker disk I/O = throughput × replication_factor
Network I/O = throughput × (replication_factor - 1)
```

- **Replication factor 3** → 3x disk writes, 2x network traffic

---

## Consumer Lag Monitoring

### What to Watch

| Metric | Warning Threshold | Critical Threshold | Action |
|--------|-------------------|-------------------|--------|
| **Consumer Lag** | > 10% of max lag | > 50% of max lag OR growing | Increase consumers/partitions |
| **Lag Growth Rate** | +5% over 5 min | +10% per minute | Debug consumer slowness |
| **Max Lag per Consumer** | > 1M messages | > 10M messages | Add more consumer instances |
| **Rebalance Frequency** | > 1 per hour | > 3 per hour | Investigate membership/heartbeat |
| **Commit Offset Lag** | > 1s from real offset | > 5s | Increase max.poll.interval.ms |

### Key Formulas

```
Consumer Lag (messages) = (latest_offset - committed_offset)

Lag in Seconds ≈ lag_messages × avg_message_processing_time_ms / 1000

Max Catchup Time = lag_messages / (throughput_messages_per_sec - message_arrival_rate)
```

### Prometheus Metrics to Export

```
kafka_consumer_lag{topic, partition, group}
kafka_consumer_lag_seconds{topic, partition, group}
kafka_consumer_lag_growing{topic, partition, group} = (current_lag > previous_lag)
rebalance_latency_ms{group}
commit_failure_rate{group, topic}
```

---

## Real-World Scenario: High Lag Under SLA

**Problem:** Consumer group for "payment-events" topic is 2 hours behind; SLA requires < 5 minutes lag.

### Diagnosis Steps

| Step | Command / Check | Expected Finding | If Bad → Action |
|------|-----------------|------------------|-----------------|
| 1. Check lag | `kafka-consumer-groups --bootstrap-server :9092 --group payment-events --describe` | Lag across all partitions and lag sum | → Proceed to step 2 |
| 2. Current throughput | Monitor broker metrics: `bytes-in-per-sec`, `messages-in-per-sec` | Compare to historical baseline | If lower → check network, broker health |
| 3. Consumer count | `kafka-consumer-groups --describe payment-events` | Should have consumers = partitions | If fewer → scale up consumers |
| 4. Consumer processing time | Application logs / APM: latency per message | > 500ms? | → Optimize processing logic, add consumers |
| 5. GC pauses | JVM heap, GC logs on consumer | Full GC > 10s? | → Increase heap, tune GC settings |
| 6. Broker saturation | CPU, disk I/O, network bandwidth | > 80%? | → Add brokers or reduce replication |
| 7. Partition skew | Lag per partition (kafka-consumer-groups describe) | One partition >> others? | → Check key distribution, repartition |

### Quick Fix Checklist

- [ ] Increase **max.poll.records** from 500 to 1000+
- [ ] Increase **num_consumer_threads** or spawn additional consumer instances
- [ ] Lower **fetch.max.wait.ms** from 500 to 100 (pull faster)
- [ ] Increase broker **num.network.threads** (default 8, try 16–32)
- [ ] Check for **consumer rebalancing** (stop/start logs) — tune session.timeout.ms and heartbeat.interval.ms
- [ ] Profile **message processing logic** — is deserialization, DB query, or API call slow?
- [ ] Verify **partition count ≥ number of consumers** (bottleneck if not)
- [ ] Disable **log.cleanup.policy=compact** if not needed (heavy CPU)

### Root Cause by Symptom

| Symptom | Likely Root Cause | Evidence |
|---------|-------------------|----------|
| Lag grows slowly and steadily | Consumer throughput < incoming rate | Processing time stable, lag increases ~linearly |
| Lag spikes suddenly | Rebalancing or GC pause | Consumer logs show "Revoked partitions" or GC > 5s |
| Lag only on 1–2 partitions | Skewed key distribution or slow broker | Topic metrics show uneven partition size |
| Lag across all partitions | Consumer pool too small or network bottleneck | cpu/network at 85%+, partition count < consumer count |

---

## Interview Tip

**Senior/Principal Level Answer:**

"In Kafka performance, you're always balancing throughput, latency, and operational simplicity. Here's my mental model:

**For throughput**: I tune the **batch.size and linger.ms** on the producer—batching 32KB over 10ms turns individual small messages into efficient groups. On the consumer, I increase **max.poll.records** and **fetch.min.bytes** to pull more work per round trip. Compression (snappy) buys you 30–50% network savings if CPU allows.

**For latency**: I flip that—batch.size=1, linger.ms=0, but I **never** go full-async without idempotence enabled; duplicates are worse than slowness. And I always watch **max.in.flight.requests.per.connection** (typically 5) to balance throughput and ordering.

**On consumer lag**: I monitor it as *seconds to catchup*, not just raw message count. If a consumer is 10M messages behind but processes 1M msgs/sec, that's ~10 seconds, not a crisis. But if lag is *growing*, that's the red flag. I look at three things: (1) Is the consumer count less than partition count? (2) Is the processing latency spiking (GC, external API timeouts)? (3) Is one partition's lag way ahead of others (skewed data)?

**For SLA-breaking lag**: Scaling horizontally (add consumers) is the first move, but only if lag distribution is even. If one partition's behind, repartitioning or checking the key distribution is next. After that, it's processing optimization—profiling the consumer logic for slow deserialization, DB queries, or API calls."

---

## Related Topics

- [Kafka Architecture](../README.md#kafka-deep-dive)
- [Consumer Groups & Rebalancing](./04-consumer-groups-rebalancing.md)
- [Exactly-Once Semantics](./05-exactly-once-semantics.md)
- [Monitoring & Observability](../Monitoring-Observability/README.md)
