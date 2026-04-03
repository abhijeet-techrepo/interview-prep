# Kafka Configurations

Essential Kafka configs for production systems: brokers, producers, consumers, and Spring Kafka settings explained.

→ Back to [Event-Driven Design](./README.md)

---

## Admin/Broker Configurations

| Config | Default | Recommended | What It Does |
|--------|---------|-------------|--------------|
| `broker.id` | — | Unique integer per broker (e.g., 0, 1, 2) | Identifies each broker in the cluster; must be unique |
| `log.retention.hours` | `168` (7 days) | `168` to `720` (1–30 days) | How long Kafka stores messages before deletion |
| `log.segment.bytes` | `1073741824` (1 GB) | `1073741824` | Max size of a log segment; larger = fewer files, slower cleanup |
| `log.retention.check.interval.ms` | `300000` (5 min) | `300000` to `600000` | How often the broker checks if segments should be deleted |
| `num.partitions` | `1` | `3` to `12` | Default partitions for auto-created topics (rarely used in modern setups) |
| `default.replication.factor` | `1` | `3` | How many replicas of each partition (min. 3 for HA) |
| `min.insync.replicas` | `1` | `2` | Min replicas that must acknowledge writes; prevents data loss |
| `unclean.leader.election.enable` | `true` | `false` | Allow out-of-sync replicas to become leader (risky; disables durability) |
| `compression.type` | `producer` | `producer` or `snappy` | Broker-side compression: `producer` = use producer's choice, `snappy`/`gzip`/`lz4` override |
| `auto.create.topics.enable` | `true` | `false` | Auto-create topics on publish; disable in production to enforce governance |
| `delete.topic.enable` | `true` | `true` | Allow topic deletion (confirm policy before setting) |

---

## Producer Configurations

| Config | Default | Recommended | What It Does |
|--------|---------|-------------|--------------|
| `acks` | `1` | `all` (or `-1`) | Durability guarantee: `0` = fire & forget, `1` = leader acks, `all` = all replicas ack |
| `retries` | `2147483647` (max int) | `3` to `5` | How many times to retry failed sends (combined with `max.in.flight.requests`); newer Java clients use max. retries with exponential backoff |
| `max.in.flight.requests.per.connection` | `5` | `1` (if ordering required) or `5` (throughput priority) | Concurrent requests per broker; `1` = strict ordering, `5` = higher throughput (may reorder on failure) |
| `enable.idempotence` | `false` | `true` | Enable idempotent producer (deduplicates retries); requires `acks=all` and `max.in.flight≤5` |
| `batch.size` | `16384` (16 KB) | `16384` to `32768` | Accumulate N bytes before sending batch; larger = better throughput, more latency |
| `linger.ms` | `0` | `10` to `100` | Wait N ms before sending batch (even if not full); balances latency & throughput |
| `compression.type` | `none` | `snappy` or `gzip` | Compress batches: `snappy` = good speed/ratio, `gzip` = best compression, `lz4` = fast |
| `buffer.memory` | `33554432` (32 MB) | `33554432` to `67108864` | Total buffer for all pending batches; larger = more throughput, higher memory |
| `request.timeout.ms` | `30000` | `30000` to `60000` | Max time broker waits for an ack; too low = spurious timeouts, too high = slower failure detection |

> [!NOTE]
> **Idempotence & Ordering**: Enable `enable.idempotence=true` for exactly-once semantics (within a single producer session). If you need strict ordering with retries, set `max.in.flight.requests=1` (slower). For high throughput + idempotence, use the default `max.in.flight=5`.

---

## Consumer & Listener Configurations

| Config | Default | Recommended | What It Does |
|--------|---------|-------------|--------------|
| `group.id` | — | Descriptive name (e.g., `order-service-consumer-v1`) | Consumer group ID; partitions distributed among group members |
| `auto.offset.reset` | `latest` | `earliest` or `none` | Action on missing/invalid offsets: `earliest` = start from beginning, `latest` = skip to end, `none` = error |
| `enable.auto.commit` | `true` | `false` | Auto-commit offsets; disable for manual control & better error handling |
| `auto.commit.interval.ms` | `5000` | `5000` to `10000` (if auto-commit enabled) | Interval between auto-commits (if enabled); lower = more frequent, higher overhead |
| `max.poll.records` | `500` | `100` to `500` | Max records returned per `poll()` call; lower = more latency, easier to process, less memory |
| `session.timeout.ms` | `45000` | `45000` to `300000` | Time before broker removes consumer from group (no heartbeat); too low = false evictions, too high = slow rebalance |
| `heartbeat.interval.ms` | `3000` | `3000` | Interval between heartbeats; must be < `session.timeout.ms` (typically `session.timeout / 3`) |
| `max.poll.interval.ms` | `300000` (5 min) | `300000` to `600000` | Max time between polls before consumer is considered dead; increase if processing is slow |
| `fetch.min.bytes` | `1` | `1` | Min bytes broker waits to accumulate before returning fetch; higher = fewer requests, more latency |
| `fetch.max.wait.ms` | `500` | `500` | Max wait time for `fetch.min.bytes` to accumulate; balances latency & throughput |

### Spring Kafka Consumer Configs

| Config (Spring) | Default | Recommended | What It Does |
|-----------------|---------|-------------|--------------|
| `spring.kafka.consumer.group-id` | — | Descriptive name | Consumer group identifier (same as `group.id` in native Kafka) |
| `spring.kafka.listener.ack-mode` | `BATCH` | `MANUAL` or `MANUAL_IMMEDIATE` | Offset commit strategy: `BATCH` = auto after batch, `MANUAL` = app controls, `MANUAL_IMMEDIATE` = commit on success immediately |
| `spring.kafka.listener.concurrency` | `1` | `3` to `10` (depends on partition count) | Number of concurrent listeners; max = number of partitions in topic |
| `spring.kafka.listener.poll-timeout` | `3000` | `3000` to `5000` | Timeout (ms) for `poll()` inside listener container |
| `spring.kafka.consumer.max-poll-records` | `500` | `100` to `500` | Max records per poll (same as native Kafka) |
| `spring.kafka.consumer.enable-auto-commit` | `true` | `false` | Auto-commit offsets; disable for better error handling & manual control |

> [!NOTE]
> **Spring Kafka Best Practice**: Use `ack-mode=MANUAL` with error handling to ensure messages are only marked as consumed after successful processing. Pair with `enable-auto-commit=false` for consistency.

---

## Interview Tip

**"Here's how I'd configure Kafka for a high-throughput, reliable event system:**

*For brokers*: Set `default.replication.factor=3` and `min.insync.replicas=2` to tolerate one broker failure. Disable `unclean.leader.election` to prevent data loss. Set `log.retention.hours=7` unless you need longer (e.g., audit logs = 30 days).

*For producers*: Use `acks=all` + `enable.idempotence=true` for exactly-once semantics within a session. Set `batch.size=16KB`, `linger.ms=10` to batch efficiently without extra latency. Use `compression.type=snappy` to reduce network I/O. If you need strict ordering per partition, drop `max.in.flight` to 1, but this kills throughput.

*For consumers*: Disable `enable.auto.commit` and use manual acknowledgment so failed messages can be retried. Set `auto.offset.reset=earliest` if missing offsets should reprocess history (adjust to `none` if you want to fail hard). With Spring Kafka, use `ack-mode=MANUAL_IMMEDIATE` for each message, or `MANUAL` for batch acks. Tune `max.poll.records` based on message size and processing time—too high causes rebalance timeouts.

The key tradeoff: `acks=all` + `idempotence=true` = durability & deduplication (essential for financial/order events), but costs throughput. For high-volume telemetry, relax to `acks=1` and disable idempotence—acceptable data loss is fine there."**
