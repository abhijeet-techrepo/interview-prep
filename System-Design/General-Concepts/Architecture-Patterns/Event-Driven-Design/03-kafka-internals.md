# Kafka Internals

Deep dive into Kafka's core architecture, cluster coordination, and partition management — essential for system design interviews.

→ Back to [Event-Driven Design](./README.md)

---

## Topics, Partitions, and Offsets

| Concept | Definition | Key Points |
|---------|-----------|-----------|
| **Topic** | Logical feed of records | Immutable; retention policy; replication factor |
| **Partition** | Ordered, durable log within a topic | Each partition lives on exactly one broker; independently ordered |
| **Offset** | Sequential ID for a record in a partition | Starts at 0; used by consumers to track progress; not shared across partitions |

**Why partitions matter:**
- Enable **parallel consumption** — multiple consumers read different partitions simultaneously
- Support **independent replay** — each partition maintains its own offset timeline
- Distribute **write throughput** — producers send to different partitions in round-robin or key-based routing

---

## Brokers and Cluster Basics

| Term | Role |
|------|------|
| **Broker** | Single Kafka server in the cluster |
| **Cluster** | Set of brokers working together; one is **controller** (elected leader) |
| **Controller** | Manages cluster state: partition leaders, replicas, metadata changes |
| **Replication Factor** | Number of copies of each partition (default often 3) |

**Broker responsibilities:**
- Store partition data locally
- Respond to produce/fetch requests
- Participate in leader election
- Sync with coordinator (group management, offsets)

---

## In-Sync Replicas (ISR)

### What is ISR?

A replica is in the **in-sync replica set** if:
- It is **actively replicated** with the leader
- Replication lag is below `replica.lag.time.max.ms` (default 10s)

| ISR State | Meaning |
|-----------|---------|
| **Leader in ISR** | Always true; leader is always in its own ISR |
| **Full ISR** | All replicas caught up with leader |
| **Degraded ISR** | Some replicas lagging; leadership not affected |

### What Removes a Replica from ISR?

A replica is **removed from ISR** when:
- **Network partitioned** — stops sending fetch requests
- **Slow disk** — can't keep up with replication rate
- **GC pause** — broker paused > `replica.lag.time.max.ms`
- **Broker crash** — stops replicating entirely

**Detection mechanism:**
- Leader tracks last fetch request from each replica
- If no fetch for `replica.lag.time.max.ms`, replica removed
- Replica re-joins ISR when it catches up

### Why ISR Matters

| Why | Impact |
|-----|--------|
| **Availability** | Shrinking ISR = fewer replicas available; risk of data loss on leader failure |
| **Durability guarantee** | `acks=all` waits for ISR acknowledgment; larger ISR = safer but slower |
| **ZooKeeper/KRaft** | Both track and update ISR state; critical for recovery |

---

## ZooKeeper vs. KRaft

### ZooKeeper (Traditional - Pre 3.3)

**Architecture:**
- External **ZooKeeper cluster** maintains Kafka metadata
- Brokers register with ZooKeeper; broker failures trigger re-election
- Leader election via ZooKeeper quorum

**What it stores:**
- Cluster membership (which brokers are alive)
- Topic metadata (partitions, replication factor)
- ISR (in-sync replica set)
- Controller election results

**Limitations:**
- Extra operational burden — manage two clusters
- Metadata propagation **lag** — state changes queued then pushed
- Scalability ceiling — ZooKeeper's model limits Kafka cluster size

---

### KRaft (Kafka Raft - Kafka 3.3+)

**Architecture:**
- **Raft consensus** built into Kafka brokers
- No external ZooKeeper dependency
- Designated brokers form a **Quorum Controller** cluster

**What changed:**
- Metadata stored in a replicated log (topic `__cluster_metadata`)
- Brokers commit writes only after quorum acks
- Leader election happens in milliseconds (vs seconds in ZooKeeper)

**Why it matters:**
- **Simpler ops** — single cluster to manage
- **Faster metadata** — sub-100ms propagation
- **Scalability** — tested with 100K+ partitions
- **Backward compatible** — existing producer/consumer code unchanged

| Aspect | ZooKeeper | KRaft |
|--------|-----------|-------|
| External dependency | Yes | No |
| Metadata latency | High (seconds) | Low (< 100ms) |
| Controller failover | Slow | Fast (quorum based) |
| Operational complexity | High | Low |
| Production ready | Yes (mature) | Yes (since 3.3) |

---

## Partition Assignment Rules

### Producer Partition Assignment

**How a producer chooses a partition:**

```
if (key provided):
    partition = hash(key) % num_partitions
else:
    partition = round_robin_or_random_with_batch_awareness()
```

**Key implications:**
- **Keyed messages** guarantee ordering within a partition
- **Keyless messages** distributed for load balancing
- `partitioner` class is customizable

**Example:**
```
Topic: orders, 3 partitions
- Order key="customer_123" → always → partition 0
- Order key="customer_456" → always → partition 1
- Keyed ordering preserved; unkeyed messages spread
```

---

### Consumer Partition Assignment

**Consumer Group coordination:**
- Consumers in a group subscribe to a topic
- **Group Coordinator** (broker) assigns partitions to consumers
- One consumer per partition (no more consumers than partitions)

**Assignment strategies:**

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| **RangeAssignor** (default) | Partition ranges per consumer | Simple; may cause imbalance |
| **RoundRobinAssignor** | Distributes evenly across consumers | Better load balance |
| **StickyAssignor** | Minimize partition movement on rebalance | Reduce overhead; preserve local state |
| **CooperativeStickyAssignor** | No stop-the-world rebalancing | Avoid pause during scaling |

**Rebalancing trigger:**
- New consumer joins group
- Consumer leaves / crashes
- Topic partition count changes

---

## Cluster Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Kafka Cluster (3 Brokers)               │
└─────────────────────────────────────────────────────────────┘

  Broker 0 (Controller)    Broker 1             Broker 2
  ─────────────────────    ──────────           ──────────
  ┌──────────────────┐    ┌──────────┐         ┌──────────┐
  │ Topic A-P0 [L]   │    │ Topic A- │         │ Topic A- │
  │ replica.0        │    │ P0 [ISR] │         │ P0 [ISR] │
  │                  │    │ replica.1│         │ replica.2│
  │ offsets: 0-1042  │    │          │         │          │
  └──────────────────┘    │ offsets: │         │ offsets: │
  ┌──────────────────┐    │ 0-1042   │         │ 0-1042   │
  │ Topic A-P1 [ISR] │    │          │         │          │
  │ replica.0        │    └──────────┘         └──────────┘
  │ offsets: 0-856   │
  └──────────────────┘    ┌──────────┐         ┌──────────┐
  ┌──────────────────┐    │ Topic A- │         │ Topic A- │
  │ Topic A-P2 [ISR] │    │ P1 [L]   │         │ P1 [ISR] │
  │ replica.0        │    │ replica.1│         │ replica.2│
  │ offsets: 0-923   │    │          │         │          │
  │                  │    │ offsets: │         │ offsets: │
  └──────────────────┘    │ 0-856    │         │ 0-856    │
  ┌──────────────────┐    │          │         │          │
  │ __cluster_meta..│    └──────────┘         └──────────┘
  │ (KRaft metadata)│
  │ replicas: 0,1,2 │    ┌──────────┐         ┌──────────┐
  └──────────────────┘    │ Topic A- │         │ Topic A- │
                          │ P2 [ISR] │         │ P2 [L]   │
                          │ replica.1│         │ replica.2│
                          │          │         │          │
                          │ offsets: │         │ offsets: │
                          │ 0-923    │         │ 0-923    │
                          └──────────┘         └──────────┘

[L]   = Leader partition
[ISR] = In-sync replica
Topic A: 3 partitions, replication factor 3
```

**Layout interpretation:**
- **P0 leader** on broker 0; replicas on 0, 1, 2 (all in ISR)
- **P1 leader** on broker 1; replicas on 1, 2, 0 (all in ISR)
- **P2 leader** on broker 2; replicas on 2, 0, 1 (all in ISR)
- Each broker holds **all topic partitions** (distributed across replicas)
- Controller (broker 0) manages cluster state

---

## Key Configuration Deep Dives

| Config | Default | Impact |
|--------|---------|--------|
| `replica.lag.time.max.ms` | 10000 | ISR removal threshold; higher = more tolerance for slow brokers |
| `min.insync.replicas` | 1 | Minimum replicas for `acks=all`; safety valve |
| `unclean.leader.election.enable` | false | If true, allows out-of-sync replicas to become leader (data loss risk) |
| `log.retention.hours` | 168 | How long offsets remain valid |
| `num.partitions` | 1 | Default partitions per new topic |
| `default.replication.factor` | 1 | Default replication factor per new topic |

---

## Common ISR Scenarios

### Scenario 1: Replica Lag
```
Broker 0 (Leader, P0): offset 5000
Broker 1 (Replica, P0): offset 4950 (lagging)
Broker 2 (Replica, P0): offset 5000 (in sync)

After 10 seconds of no fetch from B1:
ISR = [0, 2]  (broker 1 removed)

Acks=all waits for B0 and B2 only.
Min.insync.replicas = 2 → still safe.
```

### Scenario 2: Broker Crash
```
Broker 1 crashes (was leader of P1, replica of P0 and P2)

ISR[P0] = [0, 2]           (B1 removed)
ISR[P1] = [2, 0] new_leader=2  (B1 was leader, B2 takes over)
ISR[P2] = [2, 0]           (B1 removed)

All writes continue; replicas re-elect leaders.
Data is safe if min.insync.replicas ≤ remaining ISR size.
```

### Scenario 3: Slow Broker (Disk I/O Issue)
```
Broker 2 experiences disk latency.

Offset: B0=1000, B1=1000, B2=950
Wait 10s, B2 still at 950 → ISR = [0, 1]
B2 catches up → ISR = [0, 1, 2] again

Temporary reduced durability; auto-recovery.
```

---

## Consumer Offset Management (Post-KRaft)

| Aspect | Details |
|--------|---------|
| **Offset storage** | Internal topic `__consumer_offsets` (replicated, compacted) |
| **Commit modes** | Auto-commit (risky) vs. manual (safe) |
| **Seek behavior** | `earliest`, `latest`, or specific offset |
| **Rebalance listener** | Custom logic on `onPartitionsRevoked` / `onPartitionsAssigned` |

**Best practice:**
- Use **manual commit** in production
- Commit only after processing + storing results
- Use offset reset policy `none` to catch missing offsets early

---

## Interview Tip

**Senior/Principal level answer to "Explain Kafka partition leadership and ISR":**

"Kafka's partition leadership model ensures durability without external dependencies. Each partition has one leader and N-1 replicas. The **in-sync replica set (ISR)** contains only replicas that are actively caught up—defined as having fetched within `replica.lag.time.max.ms`.

When a producer sends with `acks=all`, it waits for acknowledgment from the leader and all replicas in the ISR. If a replica lags (network issue, slow disk), it's removed from ISR automatically. This is crucial: **the ISR shrinks** when replicas fall behind, reducing our safety guarantee until they catch up. If the leader fails, only ISR members can be elected as new leader, preventing data loss.

With KRaft, this coordination moved from external ZooKeeper into Kafka itself using the Raft consensus algorithm on replicated metadata. The key trade-off: larger ISR = safer writes but higher latency; smaller ISR = faster but less durable. That's why `min.insync.replicas` exists—to enforce a minimum safety threshold. In a 3-broker cluster with RF=3, if two replicas are in ISR and min.insync=2, you're still safe even if one broker crashes."

---

## Related Topics

- **[Exactly-Once Semantics](../General-Concepts/Exactly-Once-Semantics.md)** — How ISR + idempotent producers prevent duplicates
- **[Consumer Groups](./02-consumer-groups.md)** — How rebalancing interacts with partition assignment
- **[Monitoring Kafka](./06-monitoring.md)** — Track ISR size, under-replicated partitions
