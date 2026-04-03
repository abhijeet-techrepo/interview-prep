# Kafka Internals: The Architecture That Powers Event-Driven Systems

Understanding Kafka's internals—how it manages partitions, replication, leadership, and coordination—is the difference between building systems that scale gracefully and building systems that fail at 2 AM under load. As an architect, you'll face questions about throughput bottlenecks, durability guarantees, and failure scenarios. This deep dive walks you through the actual mechanics so you can reason about trade-offs with confidence.

→ Back to [Event-Driven Design](./README.md)

---

## Building the Mental Model

To make sound architectural decisions with Kafka, you need to understand five core concepts and how they interact. Each builds on the last.

Imagine you're designing an order processing system. Orders come in at scale—thousands per second. You need **throughput** (many orders processed in parallel), **ordering** (orders from the same customer in sequence), and **durability** (no lost orders, even if servers crash). Kafka solves all three, but only if you understand *why* it's designed the way it is.

Here's the insight: a single log file, on a single machine, is ordered but not scalable. So Kafka splits the log. Not randomly—it *partitions* the log, then *replicates* each partition, then *tracks* which replicas are actually synchronized, then *coordinates* all of it without requiring a separate system. This section walks through that system piece by piece.

---

## Topics and Partitions: The Scalability Breakthrough

Think of a **topic** as a logical channel—all orders flow here. A topic is durable, immutable once written, and respects retention policies. But a single ordered log is a bottleneck.

The real magic is in **partitions**. A topic with N partitions is really N independent logs, each ordered internally but not globally. This unlocks parallelism: your order service sends orders to partition 0, 1, 2... and *ten consumers* can read simultaneously—one per partition, or multiple per partition.

Now here's the architectural trade-off you'll face: **partition count**.

Too few partitions, and you can't saturate your consumers. One partition = one consumer reading at a time. Ten partitions = ten consumers can read in parallel. Sounds like "more is better," right? Not quite.

More partitions means:
- **Cluster metadata explosion** — the controller tracks every partition's leadership, ISR, and replica list. With 100,000 partitions, metadata changes become expensive.
- **Rebalancing storms** — when a consumer crashes or joins, the group rebalances, pausing all processing briefly. More partitions = longer rebalances.
- **Producer routing overhead** — the producer maintains a mental map of which partition lives where. More partitions = more memory, more cache invalidation on broker failure.

I've seen teams provision 100 partitions for a topic with two consumers. Six months later, they wonder why cluster metadata propagation takes seconds instead of milliseconds. Then they wonder why the rebalance takes 30 seconds when a single consumer restarts.

**Here's the rule I use:** Partition count should be roughly `(target_throughput_MB_per_sec) / (single_consumer_throughput_MB_per_sec)`. If one consumer can handle 10 MB/sec and you want 100 MB/sec total, you need ten partitions. Overprovision slightly for growth—maybe 12-15. Revisit quarterly.

Each partition lives on *exactly one broker* at any given moment. That broker is the **leader** for that partition. When a producer sends an order message, it goes to the leader, gets written to disk, and the producer waits (depending on `acks` setting). Only the leader accepts writes. Followers (replicas) only read.

**Why only one leader?** Ordering. If two brokers could accept writes to the same partition simultaneously, you'd have no way to impose a global order. Messages could arrive out of sequence depending on network timing. Kafka chose simplicity and safety: one leader, strict ordering.

---

## Offsets: The Elegance of Position-Based Tracking

Here's where Kafka differs fundamentally from traditional queues (RabbitMQ, ActiveMQ).

In RabbitMQ, when you consume a message, the broker *marks it as consumed* and forgets about it. If your consumer crashes, those messages are gone. You could add acknowledgments, but the broker still manages state.

In Kafka, messages are immutable. Your consumer just tracks a **position**—an **offset**. Offset 0 is the first message ever written to a partition. Offset 1000 is the 1001st message. It's a number, nothing more. Your consumer reads messages 0-999, then crashes. It comes back, seeks to offset 1000, and continues. No broker coordination needed.

This is brilliant for three reasons:

1. **Replay** — Want to reprocess all orders from last Tuesday? Seek to the offset from that time. Want to replay the last hour for debugging? Same operation. The broker doesn't care. This is why Kafka is called an "event store."

2. **Multiple independent consumers** — Ten different services can consume the same partition independently, each at their own pace, each tracking their own offset. No broker state coordination.

3. **Durability without special ops** — if your consumer crashes, on restart it seeks to its last committed offset and continues. No messages are lost, no reprocessing (assuming you commit after processing, not before).

The offset is tracked in an internal topic called `__consumer_offsets`. It's a compacted topic, meaning Kafka keeps only the *latest* offset commit per consumer group. The partitions in this topic are spread across brokers, like any other topic.

**But here's the catch:** your consumer must *choose* when to commit. If you commit at the wrong time—before processing, or in the wrong sequence—you can lose messages or reprocess them. This is why "exactly-once semantics" is a separate, complex topic. For now: offsets are positions, not acknowledgments.

---

## Brokers and the Cluster: The Infrastructure Layer

A **broker** is a single Kafka server. It stores partition data on local disk, organized into immutable segment files. When a producer writes, the broker appends to the current segment, then flushes to disk (depending on `log.flush.interval.ms`). Reads are served from memory-mapped files, which is why Kafka is so fast.

A **cluster** is a set of brokers working together. They share nothing—no shared storage, no shared coordination service (post-KRaft). Each broker is independent. They discover each other via the **controller**.

The **controller** is a designated broker that manages cluster state:
- Which brokers are alive (via heartbeats)
- Which broker is the leader for each partition
- Which replicas are in sync
- Partition reassignments (when you rebalance load)

There's only one controller at a time. When the controller broker crashes, the remaining brokers detect this and elect a new one. This happens via the controller's heartbeat—if a broker stops receiving heartbeats from the controller, it knows something is wrong.

**Broker responsibilities in normal operation:**
- Accept writes from producers (if it's the partition leader)
- Replicate writes to followers (if it's a partition leader)
- Fetch from partition leaders (if it's a replica)
- Respond to consumer fetch requests (any broker can serve reads from any partition)
- Track its own heartbeat to the controller
- Participate in consumer group coordination (some brokers are "group coordinators")

When a broker crashes, the controller detects the heartbeat loss (default 30 seconds), then:
1. Removes that broker from all ISRs
2. Elects new leaders for partitions where this broker was the leader (picks the first replica in the new ISR)
3. Broadcasts metadata changes to all remaining brokers
4. Consumers belonging to groups coordinated by this broker must find a new coordinator

This whole dance—failure detection plus election—takes roughly 30 seconds by default. In high-throughput scenarios, 30 seconds is a long time (millions of messages accumulate in replicas). That's why tuning `socket.timeout.ms` and `replica.socket.receive.buffer.bytes` matters—you're trying to detect failure faster and keep replicas in sync.

---

## ISR Deep Dive: The Heart of Durability

**ISR** stands for "in-sync replica set," but let's be precise about what "in-sync" means, because it's not what you think.

A replica is in the ISR if:
- It is actively replicating from the leader
- The time since its last fetch request is less than `replica.lag.time.max.ms` (default: 10 seconds)

That second bullet is the key. The leader doesn't measure disk lag or message lag directly. It just tracks: "Did this replica send me a fetch request in the last 10 seconds?" If yes, it's in sync. If no, it's out of sync.

Why 10 seconds? It's a heuristic. The assumption is: if a replica is healthy and the network is healthy, it will fetch every few seconds. If it hasn't fetched in 10 seconds, something is wrong—the replica crashed, the network partitioned, or the disk is so slow it can't keep up.

**The timeline of a replica falling behind:**

1. **T=0** — Your broker is fine, fully replicated, ISR = [0, 1, 2]
2. **T=0+** — Disk on broker 2 starts experiencing latency (maybe a full table scan from another process)
3. **T=0+5 seconds** — Broker 2 is still fetching, but slower. Last fetch was 3 seconds ago. Still in ISR.
4. **T=0+12 seconds** — Broker 2 hasn't sent a fetch request in the last 10 seconds. The leader notices. ISR shrinks to [0, 1]. Broker 2 is now a **replica** (not in-sync), still replicating, but laggy.
5. **T=0+30 seconds** — The disk latency clears. Broker 2 sends a fetch request. The leader checks the lag. If it's caught up, ISR expands back to [0, 1, 2].

During step 4, what happens to durability?

**If `acks=all` is set:** The producer waits for acknowledgment from the leader (broker 0) *and all replicas in the ISR* (brokers 1, only). Broker 2's acknowledgment is not required. Why? Because broker 2 is lagging; we can't guarantee it won't lose messages if it crashes.

**If `min.insync.replicas=2` is set:** The producer waits for acks from at least 2 replicas in the ISR. Right now, ISR=[0, 1], so the producer waits for both. If broker 1 crashes, ISR=[0] only, and the producer *cannot write anymore* (only 1 replica in ISR, but min is 2). This is the safety valve—you're saying "I don't care if you're slow, but I need at least 2 replicas available, or I reject writes."

Let me tell you a war story. I once operated a Kafka cluster where we set `min.insync.replicas=1` (the default) and `unclean.leader.election.enable=true` (also default, though dangerous). A data center network partition happened—brokers in DC-A lost contact with brokers in DC-B.

In DC-B, a partition leader crashed. The controller (also in DC-B) elected an out-of-sync replica to be the new leader. That replica was behind by ~100,000 messages. Meanwhile, DC-A still had the old leader, accepting writes. When the partition healed, we had a split-brain scenario: two brokers claiming to be the leader for the same partition, with divergent message histories.

Result: we had to choose which side of the split to keep, and we *lost data*. The recovery took six hours.

**After that:** I never again allow `min.insync.replicas=1`. The minimum is 2, and usually I set 3 in a 5-broker cluster. For any topic where data loss is unacceptable, it's non-negotiable. `min.insync.replicas` is the one config I always validate in architecture reviews.

**Three real-world scenarios:**

### Scenario 1: Replica Lag (Slow Disk)
```
Broker 0 (Leader, partition 5): offset 5000 (just wrote message 5000)
Broker 1 (Replica, partition 5): offset 4950 (lagging)
Broker 2 (Replica, partition 5): offset 5000 (in sync)

ISR = [0, 2]  (broker 1 is not in sync, hasn't fetched in 10+ sec)

Producer sends with acks=all, min.insync=2:
- Broker 0 acknowledges (leader, always acks)
- Broker 2 acknowledges (in ISR)
- Broker 1 doesn't acknowledge (not in ISR)
- Producer gets ack after 2 replicas confirm

Meanwhile, broker 1 continues replicating in the background, catching up.
When it sends a fetch request after catching up, it re-enters ISR.
```

### Scenario 2: Broker Crash
```
Broker 1 crashes (was the leader of partition 1)

Before crash:
ISR[P1] = [1, 2, 0]  (broker 1 is leader)

After crash detected:
Controller removes broker 1 from all ISRs.
ISR[P1] = [2, 0]
Controller elects broker 2 (first in remaining ISR) as new leader.
Consumers are notified of new leader (metadata update).
Producers are notified (will retry writes to new leader).
All writes now go to broker 2 instead of broker 1.

If `min.insync.replicas=2`:
ISR[P1] now has 2 members ([2, 0]), so `acks=all` can still succeed.
If another broker crashes, ISR shrinks to 1, and writes are rejected.
```

### Scenario 3: Network Partition (The Scary One)
```
Cluster: brokers 0, 1, 2
Controller is on broker 0.
Network partition: brokers 0 and 1 can see each other, but broker 2 is isolated.

Broker 2's perspective:
- Lost contact with controller
- Can't send fetch requests (controller elected me to be replica of P1, but can't verify)
- Can serve reads (locally cached data is still valid)
- Cannot serve writes (broker doesn't know if it's still a leader)

Brokers 0 and 1's perspective:
- Broker 2 has stopped sending heartbeats
- After 30 seconds, controller removes broker 2 from ISRs
- Partitions with broker 2 as leader get new leaders (elected from 0 or 1)
- Writes resume with 2-replica ISR

When the partition heals:
- Broker 2 re-joins the cluster
- If it was a leader before, it's now a follower (the new leader is now the source of truth)
- It replays all missed messages
- It re-enters ISRs once caught up
```

The key insight: **ISR is a safety mechanism.** It shrinks when replicas fall behind, reducing your durability guarantee. It expands when replicas catch up. Combined with `min.insync.replicas`, it lets you enforce a minimum safety floor.

---

## ZooKeeper vs. KRaft: An Architectural Evolution

For years, Kafka had a dependency that frustrated every ops team: **ZooKeeper**.

### The Old Way: ZooKeeper (Pre-3.3)

Kafka didn't manage its own cluster state. Instead, it delegated to an external **ZooKeeper cluster** (usually 3-5 nodes). Here's what ZooKeeper stored:

- **Broker membership** — Which brokers are alive (via ephemeral znodes)
- **Topic metadata** — How many partitions, replication factor, where each partition lives
- **ISR state** — Which replicas are in sync for each partition
- **Controller election** — Which broker is the current controller
- **Consumer offsets** — (In older versions) group offsets and group membership

The flow looked like:
1. A broker starts; it registers itself as an ephemeral node in ZooKeeper: `/brokers/ids/0`
2. The controller watches `/brokers/ids`; if a node disappears, the controller detects a broker crash
3. The controller updates ISR state in ZooKeeper: `/config/topics/my_topic/0` contains the ISR list
4. All brokers watch these paths; when they change, brokers update their metadata
5. Consumers similarly track their offset in ZooKeeper: `/consumers/my_group/offsets/my_topic/0`

This works, but it has a fatal flaw: **it's slow**. ZooKeeper is fundamentally ordered, but Kafka's metadata changes are bursty. When a broker crashes:
1. ZooKeeper detects the disappearance (10-30 seconds)
2. The watch fires
3. Kafka brokers react
4. Brokers need to re-elect leaders, update ISRs, and broadcast new metadata

All of this goes through ZooKeeper's coordination model, which is not designed for high-throughput metadata changes. The result: metadata propagation in a large cluster could take seconds.

**Operational pain points:**
- You had to operate ZooKeeper alongside Kafka. Two clusters to monitor, tune, and debug.
- ZooKeeper has scalability limits. With 100,000 partitions, ZooKeeper groans.
- Consumer offset management moved to Kafka's internal topic in 2015 (KIP-0), but ZooKeeper coordination persisted until KRaft.

### The New Way: KRaft (Kafka 3.3+)

Around Kafka 2.8, the Confluent team started KRaft: **Kafka Raft consensus**. The idea: use Raft (the same algorithm that powers etcd, Consul, etc.) but *inside Kafka brokers*. No external ZooKeeper needed.

In KRaft mode:
- A subset of brokers form a **Quorum Controller** cluster (usually 3-5)
- Cluster metadata is stored in a compacted topic called `__cluster_metadata`
- This topic is replicated across quorum controllers using Raft
- Any broker can read metadata; quorum controllers can write it
- When metadata changes (e.g., a broker crashes), the quorum reaches consensus and updates the `__cluster_metadata` log
- All brokers read this log and update their in-memory metadata cache

The implications are profound:

**Speed:** Raft consensus is optimized for low-latency decisions. Leader election happens in milliseconds, not seconds. Metadata propagation is sub-100ms.

**Simplicity:** No external ZooKeeper cluster. One deployment model. One set of operational concerns.

**Scalability:** Tested with 100,000+ partitions. Metadata updates don't bottleneck.

**Reliability:** The quorum controller model is more resilient. If 1 of 3 quorum controllers dies, the other 2 can still reach consensus. ZooKeeper had similar benefits, but KRaft is tightly integrated into Kafka, so failure detection and recovery are faster.

**Backward compatibility:** Producer and consumer code doesn't change. Rebalancing works the same. The change is internal.

| Aspect | ZooKeeper | KRaft |
|--------|-----------|-------|
| External dependency | Yes (3-5 ZK nodes) | No |
| Metadata latency | High (1-5 seconds typical) | Low (< 100ms) |
| Controller failover | Slow (watch fires, brokers react) | Fast (Raft election) |
| Partition scalability | Tested to ~50,000 | Tested to 100,000+ |
| Operational complexity | Manage two clusters | Single cluster |
| Maturity | Stable since 2011 | Production ready since 3.3 |

**Where are we now?** KRaft is the future. New clusters should use KRaft. ZooKeeper clusters are in maintenance mode—Confluent still supports them, but new features go into KRaft first.

That said, if you're operating a ZooKeeper cluster and it's stable, there's no need to migrate immediately. Migration is a one-time effort, not a quick switch. Some teams run both in hybrid mode for a transition period.

---

## Partition Assignment: The Journey of a Message

When your order service writes an order, Kafka doesn't randomly pick a partition. There's logic, and you need to understand it because it affects ordering, load balancing, and exactly-once semantics.

### Producer Partition Assignment

The producer has a message with:
- A topic (e.g., "orders")
- A key (e.g., "customer_123")
- A value (the JSON order data)

The producer runs this logic:

```
if (key provided):
    partition = hash(key) % num_partitions
else:
    partition = round_robin_or_random()
```

If the key is present, the producer hashes it. This is deterministic—same key always goes to the same partition. This guarantees **ordering within a partition**. All of customer_123's orders go to partition 5 (assuming hash(customer_123) % 3 partitions = 5). They arrive in order, so the consumer sees them in order.

If there's no key, the producer distributes messages across partitions for **load balancing**. Each batch of messages might go to different partitions (the logic varies by producer version, but sticky partitioning is now default—batches stick to one partition to minimize broker hops).

**Trade-offs:**
- **Keyed messages:** Ordering guaranteed within customer, but load might become imbalanced (one customer sends tons of orders, and partition 5 is hot).
- **Keyless messages:** Load balanced, but no ordering guarantee across partitions.

**Real-world example:**
Topic: "orders" with 10 partitions.
- Message 1: key="customer_A", value=order_100 → partition 0
- Message 2: key="customer_B", value=order_101 → partition 3
- Message 3: key="customer_A", value=order_102 → partition 0 (same key, same partition)

Consumer 1 reads partition 0: sees order_100, then order_102 (in order).
Consumer 2 reads partition 3: sees order_101.

If you later query the database for customer_A's orders, you see them in timestamp order. Good. But if customer_A sends 1000 orders per second and everyone else sends 1 per second, partition 0 is hot, and the other consumers are idle. This is a real problem in production.

### Consumer Partition Assignment

On the consumption side, a **consumer group** is a set of consumers subscribing to the same topic. Kafka's job is to distribute partitions among consumers so that:
- Each partition is assigned to at most one consumer (no two consumers read the same partition simultaneously)
- Partitions are spread evenly (if there are N partitions and M consumers, each consumer gets roughly N/M partitions)

This is handled by the **group coordinator** (a broker acting as arbitrator for the group). When a consumer joins or leaves, the coordinator triggers a **rebalance**: all consumers stop, re-distribute partitions, and resume. During rebalance, no messages are processed (a pause measured in seconds).

The default assignment strategy is **RangeAssignor**:
- Sort partitions and consumers
- Assign range chunks: consumer 0 gets partitions 0-2, consumer 1 gets 3-5, etc.
- Simple, but if you have many consumers, the last consumer in the range gets fewer partitions (imbalance)

A better choice is **StickyAssignor**:
- Assigns partitions to minimize movement during rebalancing
- If a consumer joins, only some partitions are reassigned (not all)
- Keeps local state hot (consumer maintains disk/memory caches)

Most recent choice is **CooperativeStickyAssignor**:
- Like StickyAssignor, but the rebalance is incremental
- Instead of "stop all, reassign, resume," it's "consumer A yields partition 5, consumer B takes it, all others stay—no stop"
- Zero-downtime rebalancing (other consumers keep processing while rebalance happens)

**When does rebalancing happen?**
- New consumer joins the group (scales up)
- Consumer dies or is removed (scales down)
- Consumer takes too long processing (times out, considered dead)
- Topic partition count changes (expand the topic)

Each rebalance pauses processing. In a group of 100 consumers, a 5-second rebalance means 500 consumer-seconds of wasted compute. This is why StickyAssignor and CooperativeStickyAssignor are preferred.

---

## Cluster Architecture in Pictures

Let me walk you through a concrete cluster. This is what you're actually building:

```
┌───────────────────────────────────────────────────────────────────┐
│                    Kafka Cluster (3 Brokers)                      │
│                                                                   │
│  Each broker stores partition replicas and handles replication    │
└───────────────────────────────────────────────────────────────────┘

  Broker 0 (Controller)      Broker 1               Broker 2
  ─────────────────────      ───────────           ───────────
  ┌──────────────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │ Topic A - P0         │  │ Topic A - P0     │  │ Topic A - P0     │
  │ [LEADER]             │  │ [ISR REPLICA]    │  │ [ISR REPLICA]    │
  │ Offset: 0-5000       │  │ Offset: 0-5000   │  │ Offset: 0-5000   │
  └──────────────────────┘  └──────────────────┘  └──────────────────┘

  ┌──────────────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │ Topic A - P1         │  │ Topic A - P1     │  │ Topic A - P1     │
  │ [ISR REPLICA]        │  │ [LEADER]         │  │ [ISR REPLICA]    │
  │ Offset: 0-4750       │  │ Offset: 0-4750   │  │ Offset: 0-4750   │
  └──────────────────────┘  └──────────────────┘  └──────────────────┘

  ┌──────────────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │ Topic A - P2         │  │ Topic A - P2     │  │ Topic A - P2     │
  │ [ISR REPLICA]        │  │ [ISR REPLICA]    │  │ [LEADER]         │
  │ Offset: 0-5200       │  │ Offset: 0-5200   │  │ Offset: 0-5200   │
  └──────────────────────┘  └──────────────────┘  └──────────────────┘

  ┌──────────────────────┐
  │ __cluster_metadata   │
  │ (KRaft consensus log)│
  │ Replicas: 0, 1, 2    │
  │ (all 3 quorum ctlrs) │
  └──────────────────────┘

Producer sends order to Topic A with key="customer_123":
  1. Producer hashes key: hash(customer_123) % 3 = 0
  2. Producer routes to partition 0
  3. Producer connects to broker 0 (the leader of P0)
  4. Broker 0 writes to disk: offset 5001
  5. Broker 0 replicates to brokers 1 and 2 (their replicas of P0)
  6. Brokers 1 and 2 acknowledge
  7. If min.insync.replicas=2 and acks=all:
     - Broker 0 waits for broker 1 or 2 to ack
     - Once 2 acks received, producer gets success
  8. Producer continues to next message

Consumer in group "order_processor" subscribes to Topic A:
  1. Controller sees 3 consumers, 3 partitions: 1-to-1 assignment
  2. Consumer 1 → Partition 0
  3. Consumer 2 → Partition 1
  4. Consumer 3 → Partition 2
  5. Each consumer can read from any broker (reads are served from any replica)
  6. Consumer 1 asks broker 0, 1, or 2: "Give me messages from partition 0 starting at offset 5001"
  7. Broker returns messages, consumer processes, commits offset 5001
  8. If consumer 1 crashes, controller re-balances:
     - Consumer 2 now owns partitions 0 and 1 (or sticky assigns elsewhere)
     - Consumer 1 restarts, waits for new assignment
     - Re-balance pauses all consumers briefly
```

**Key observations:**

1. **Each partition lives on every broker.** Topic A has 3 partitions, and they're spread across 3 brokers. Broker 0 stores P0 (leader), P1 (replica), P2 (replica).

2. **Leaders are balanced.** P0 leader is on broker 0, P1 leader is on broker 1, P2 leader is on broker 2. This spreads write load.

3. **All ISRs are full.** [0, 1, 2] is the ideal state—all replicas in sync. If broker 2 crashed, ISR[P0] = [0, 1].

4. **Metadata is replicated.** The `__cluster_metadata` topic is on all 3 brokers, and the 3 brokers form a Raft quorum. If one broker crashes, quorum still functions (2 of 3).

5. **Reads can come from any broker.** Consumer doesn't have to read from the leader; it can read from any replica (though reads lag behind the leader by replication time).

---

## Key Configuration Deep Dives

These configs aren't just knobs—they represent architectural trade-offs:

| Config | Default | What It Controls | Architectural Impact |
|--------|---------|-----------------|----------------------|
| `replica.lag.time.max.ms` | 10000 (10s) | Threshold for removing a replica from ISR | Higher = more tolerance for slow brokers, but slower detection of failure. Lower = faster detection, but flaky networks cause ISRs to shrink unnecessarily. |
| `min.insync.replicas` | 1 | Minimum ISR size for `acks=all` | Set to 2+ for safety. Setting to 1 means a single broker failure causes data loss. Non-negotiable in production. |
| `unclean.leader.election.enable` | false | Allow out-of-sync replicas to be elected as leaders | Keep false. Enabling this allows data loss and divergence. I learned this the hard way. |
| `log.retention.hours` | 168 (7 days) | How long messages stay in the log | Longer retention = more replay capability, but more disk. Shorter = less disk, but you can't replay old messages. |
| `num.partitions` | 1 | Default partitions for new topics | Typically set to 3-10 as a cluster default. Should be overridden per-topic. |
| `default.replication.factor` | 1 | Default RF for new topics | Set to 3 in production. RF=1 means no replication (any broker crash loses data). |
| `socket.timeout.ms` | 30000 (30s) | Timeout for broker communication | Affects failure detection speed. Lower = faster detection, but might be too aggressive in WAN scenarios. |
| `offsets.retention.minutes` | 10080 (7 days) | How long consumer offsets are kept | Longer = consumers can be down longer before losing their position. But the `__consumer_offsets` topic grows. |

The config I focus on most in architecture reviews: **`min.insync.replicas`**. It's the safety valve. Too low, and you're trusting a single broker. Too high (e.g., 5 in a 5-broker cluster), and one broker crash stops all writes. I usually recommend `min.insync.replicas = ceil(replication_factor / 2) + 1`, which means:
- RF=3 → min=2 (you can lose 1 broker and still write)
- RF=5 → min=3 (you can lose 2 brokers and still write)

---

## Consumer Offset Management: The Invisible Contract

Your consumer reads messages, processes them, and commits its position. Where does that position live?

**In the internal topic `__consumer_offsets`.**

This topic is:
- **Compacted** — Only the latest offset per consumer group per partition is retained. Old entries are deleted.
- **Replicated** — Default RF=3 (configurable). Your offset commit is durable.
- **Partitioned** — Partitions are assigned based on `hash(group_id) % num_partitions`. This spreads group coordination load.

When your consumer calls `commitOffsets()`, here's what happens:

1. Consumer sends offset commit request to the group coordinator (a broker)
2. Coordinator appends to `__consumer_offsets`: `{group: "order_processor", topic: "orders", partition: 0, offset: 5001}`
3. Coordinator waits for replicas to acknowledge (default: waits for all ISR)
4. Consumer receives confirmation

If the consumer crashes and restarts:
1. It sends a FetchOffsets request to the coordinator
2. Coordinator reads the latest offset from `__consumer_offsets`
3. Consumer seeks to offset 5001 and resumes

**Commit modes:**

- **Auto-commit** (risky) — Consumer automatically commits every 5 seconds (configurable). If the consumer crashes between processing and commit, you reprocess messages. If it crashes *after* commit but *before* persisting results, you lose messages.
- **Manual commit** (safe) — You call commitOffsets() explicitly. Typically after processing and storing results. More code, but predictable.
- **Manual async commit** — You call commitAsync(), which doesn't block. Useful in high-throughput scenarios, but requires careful error handling.

**Best practice:** Manual commit, synchronously, after results are persisted (e.g., order written to database). Code looks like:

```
for message in consumer.poll():
    order_data = parse(message.value)
    write_to_database(order_data)  # Blocking
    consumer.commitSync()           # Block until offset is committed
```

If your application crashes between database write and offset commit, on restart the consumer re-reads that message and re-writes the database. This is idempotent reprocessing, not data loss. Acceptable.

If you flip the order (commit first, write second), a crash between commit and write loses the order. Not acceptable.

---

## Real-World Complexity: When Your Ideal Cluster Breaks

I've painted an idyllic picture: 3 brokers, RF=3, all ISRs full, metadata propagating at light speed. Reality is messier.

**Scenario: Broker 2 experiences a full-disk condition.**

Timeline:
- T=0: Broker 2 is at 95% disk capacity
- T=1: Replication traffic fills the last 5%. Broker 2 disk reaches 100%. Log writing starts failing.
- T=2: Broker 2 can't write new replicas. It stops sending fetch requests to partition leaders.
- T=5: Broker 2 still hasn't sent a fetch in 5 seconds. Leaders start thinking it's slow.
- T=12: Broker 2 hasn't fetched in 12 seconds. ISRs shrink: ISR[P0]=[0,1], ISR[P1]=[1,2], ISR[P2]=[2,0].
- T=15: Ops team gets alerted: "Under-replicated partitions!" They SSH into broker 2, clear some logs, and restart the broker.
- T=20: Broker 2 comes back online. It's 100 GB behind on partition replicas.
- T=25-60: Broker 2 is catching up, replicating messages at max speed.
- T=65: Broker 2 catches up. ISRs expand: all back to [0, 1, 2].

Total incident duration: 65 seconds. During this time:
- Writes were at risk (only 2 replicas in ISR)
- If min.insync.replicas=2, you were fine
- If min.insync.replicas=1, you had no safety net (and I told you not to do this)
- Monitoring should have alerted you at T=12

This scenario is why monitoring is so critical. You need to track:
- **Under-replicated partitions** — count of partitions where ISR size < replication factor
- **ISR size per partition** — alert if it shrinks
- **Broker disk usage** — alert if above 80%
- **Replication lag** — lag of each replica from leader

---

## Interview Tip

**How to answer "Explain Kafka's durability model and trade-offs":**

"Kafka's durability comes from replication and the in-sync replica set (ISR). Each partition has one leader and N-1 replicas. The ISR contains only replicas actively caught up—fetching within `replica.lag.time.max.ms`.

When a producer sends with `acks=all`, it waits for the leader plus all ISR members to acknowledge. This guarantees durability: if the leader fails, a new leader is elected from the ISR, and no committed messages are lost.

The trade-off: larger ISR = higher latency (more replicas to wait for), but better durability. This is why `min.insync.replicas` exists—it enforces a minimum safety floor. I always set it to at least 2, typically `(replication_factor // 2) + 1`.

In KRaft (Kafka 3.3+), cluster coordination moved from external ZooKeeper into Kafka itself using Raft consensus. This improves metadata latency from seconds to <100ms and simplifies operations.

A common mistake: setting `min.insync.replicas=1` and `unclean.leader.election.enable=true`. This allows out-of-sync replicas to become leaders, causing data loss and divergence. I would never allow this in production."

---

## Related Topics

- [Delivery Semantics](./05-delivery-semantics.md) — How idempotent producers and transactional writes prevent duplicates
- [Kafka Configs](./04-kafka-configs.md) — Production config tuning for brokers, producers, and consumers
- [Kafka Performance](./06-kafka-performance.md) — Tuning, lag monitoring, and incident response
