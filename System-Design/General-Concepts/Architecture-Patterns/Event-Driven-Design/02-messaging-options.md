# Messaging Options for Event-Driven Systems

When you choose a messaging system, you're not just picking a technology—you're deciding how your services will fail, how much data you can afford to lose, and whether your team will sleep well at night. This decision cascades through your entire event-driven architecture, affecting latency budgets, operational complexity, and ultimately, whether you can ship features quickly or spend months debugging race conditions.

→ Back to [Event-Driven Design](./README.md)

---

## The Messaging Landscape: Four Questions That Matter

Before you open the comparison table, ask yourself these four fundamental architectural questions. They're the north star that should guide every messaging decision.

**First: What is your throughput ceiling?** Not your average load—your absolute peak, plus 50% headroom for growth and customer surges. Is it thousands of messages per second, or millions? This single number eliminates half your options immediately. You cannot scale Redis Pub/Sub to handle what Kafka handles at 1M msgs/sec without completely redesigning your system. This is not a dial you turn up in production; it's a foundational choice.

**Second: What is your relationship with message loss?** This is where many architects stumble. They say "we can't lose messages" when what they really mean is "we can't lose *financial transactions*." But you can absolutely lose notifications that your friend liked your photo. The business tolerate duplicates? That changes everything. Exactly-once delivery is the siren song of distributed systems—it sounds wonderful until you realize the cost: it requires stateful processing, idempotency keys, and operational complexity that will haunt you for years.

**Third: How far back must you replay?** Do you need to replay the last 7 days of events when a new consumer joins? The last 30 days? Forever? This determines whether you can afford a simple queue (limited retention window) or need something with a commit log. It also determines storage costs: Kafka storing six months of events will cost differently than Redis which won't store them at all.

**Fourth: Where does your data live?** Are you AWS-only, locked into Google Cloud, or running a multi-cloud architecture? Are you on-premises? This is often the deciding factor in architect meetings. If your entire infrastructure is in AWS, fighting against the grain to run Kafka when SNS/SQS exists is premature optimization that burns engineering cycles.

---

## The Four Archetypes: Understanding Messaging Philosophy

Different messaging systems were built with fundamentally different problems in mind. Understanding this genealogy helps you make decisions that stick.

**Message Queues** (the traditional approach) were designed when distributed systems had to be reliable. RabbitMQ, for instance, emerged from financial services where every message was sacred. They use exchange patterns and routing keys—expressive, flexible, but operationally heavy. You manage every queue. You name every binding. They're the Swiss Army knife of messaging, which is exactly why you should avoid using them unless you need that flexibility.

**Kafka** rewrote the rules. Built at LinkedIn to handle the firehose of user activity at billion-scale, it abandoned the "every queue is precious" model and asked: "What if we just append every event to a log forever?" That shift—from queue-as-contract to log-as-truth—is why Kafka became the default for event streaming. You don't route messages in Kafka; you partition them. You don't manage queues per consumer; you manage consumer groups reading the same partition independently. It's a different mental model, and that's its superpower.

**Redis Pub/Sub** comes from the real-time web world—dashboards, notifications, collaborative tools. It's intentionally ephemeral: fire a message, it gets delivered right now to anyone subscribed, and then it's gone. No durability. No replay. That seems reckless until you realize it's actually liberating. You get ultra-low latency, simple operational model, and the business doesn't care if your leaderboard update gets missed.

**Cloud-native services** (AWS SNS/SQS, Google Pub/Sub) collapse the operational burden. You don't manage brokers, replication factors, or ZooKeeper clusters. You pay for scale and get multi-region distribution as a bonus. This comes with trade-offs: you're locked into a cloud vendor, you lose some operational visibility, and you're paying per-message instead of per-instance.

---

## Apache Kafka: The Event Streaming Platform

Kafka is what you reach for when you're ready to build real event-driven systems at scale. I've deployed it across multiple companies handling everything from clickstreams (10M events/day) to IoT sensor data (10B events/day), and it's the only technology in this list that gets *better* as your scale increases.

### The Architectural Philosophy

Kafka fundamentally treats your events as an immutable log. Every message appended to a partition is assigned an offset—its position in the log. Producers write to partitions (usually selected by a key for affinity), and consumers read sequentially, tracking their position. That simplicity is deceptive: it unlocks ordering guarantees within a partition, replay semantics, consumer groups at scale, and stream processing that handles joins and state correctly.

The cost is that you must think about partitioning. A single partition is a bottleneck; ten partitions on high-traffic topics scales linearly until you hit network I/O. In production, I typically start at 10 partitions for most topics, scale to 50-100 for high-volume topics like user events, and keep fewer for low-volume topics (to avoid wasting memory on idle partitions).

### Where Kafka Shines

**Multi-consumer fan-out at scale.** With Kafka consumer groups, you can have five different services reading the same topic independently—each with its own offset tracking. One consumer falls behind by an hour, and it doesn't impact the others. One consumer crashes and replays from its last committed offset. This is elegant. In RabbitMQ, you'd need a separate queue per consumer. In SNS, you'd pay per-delivery to each subscriber.

**Temporal replay and event sourcing.** Because Kafka retains messages, you can replay the entire history of events to reconstruct state. I've used this pattern to audit compliance, debug customer issues ("what was the exact sequence of events that led to this state?"), and onboard new services. A new service starts consuming from offset 0 and builds its state from the beginning of time.

**Stream processing with ordering guarantees.** Kafka Streams (or Flink) can join streams, aggregate, and statefully process events, with the guarantee that if two events have the same key, they're processed in order. This is how you build real-time dashboards, fraud detection, and recommendation engines without descending into eventual consistency hells.

**Operational observability.** Consumer lag—the difference between the latest message offset and the consumer's current position—is a metric you can expose, alert on, and reason about. A consumer slowly falling behind? You see it immediately. A consumer catching up after a deployment? You track it. This visibility is invaluable in production.

### Where Kafka Breaks Down

**Operational complexity.** Kafka clusters require ZooKeeper (or KRaft mode in newer versions), multiple brokers for replication, careful configuration of `log.retention.bytes`, `log.segment.bytes`, and `replica.lag.time.max.ms`. I've spent nights debugging Kafka clusters because someone misconfigured `min.insync.replicas` and didn't realize they were losing durability. Managed Kafka (AWS MSK, Confluent Cloud) solve this, but they cost more and give you less visibility into internals.

**Complexity of exactly-once semantics.** If your business genuinely requires exactly-once delivery (not just "at-least-once with deduplication"), Kafka's transactions feature is powerful but operationally subtle. You're writing transactional state with messages in a way that survives failures. It works, but it requires careful design. Most of the time, "at-least-once with idempotent consumers" is simpler and equally correct.

**Long tail latency.** Kafka prioritizes throughput over latency. A single message might take 100ms to persist and replicate when you have slow followers. For ultra-low-latency systems (sub-millisecond), Kafka isn't the tool. Use Redis Pub/Sub or a custom in-memory queue.

**Wasted capacity on low-volume topics.** Even if a topic gets one message per minute, Kafka keeps all 10 partitions warm, consuming broker resources. This is a minor operational grind, but it matters at scale.

### Concrete Scenario: When and Why You'd Pick Kafka

Imagine you're building a user activity platform. Services need to know when users sign up, log in, click buttons, and make purchases. You have 50M users generating 100K events/second at peak. You need to:

- Send real-time notifications (when a friend comes online)
- Build an audit log (for compliance)
- Recalculate user segments (based on behavior)
- Train ML models (offline, on historical data)

This is Kafka's sweet spot. One `user-events` topic (100 partitions) handles all throughput. The notifications service consumes and filters for relevant events. The audit team runs queries on retained logs. The ML team replays six months of history. Each service operates independently without blocking others. The cost is operational: you run a managed Kafka cluster (or MSK), monitor broker health, and occasionally explain to product why we can't just "delete events to save money."

### Key Configuration Decisions

When you deploy Kafka, these choices have long-term implications:

- **`acks=all` vs. `acks=1`:** Use `acks=all` for financial transactions, compliance events, and anything you can't afford to lose. Expect 10-50% slower writes. Use `acks=1` for user events, analytics, anything where you tolerate 0.01% loss. Your latency budget usually demands this.

- **Compression (`snappy`, `lz4`, `gzip`):** Network bandwidth is expensive at scale. Snappy gives you 2-3x compression with minimal CPU cost. Gzip gives you 5x compression but uses real CPU. Measure your trade-off; I usually pick snappy for most topics.

- **Retention (`log.retention.ms` or `log.retention.bytes`):** More retention = bigger storage bills. Start conservative (7 days), expand only if you have a replay use case. Log compaction (keeping only the latest value per key) is powerful for state topics.

- **Replication factor:** Always 3 in production. Never 2. A factor of 1 means one broker failure loses your data. Three ensures any single failure is survivable. The storage cost is worth the durability guarantee.

---

## RabbitMQ: Complex Routing for Hybrid Clouds

RabbitMQ is the traditional message broker, mature and feature-rich, with one foot in enterprise datacenters and one foot in cloud infrastructure. I've used it for systems where Kafka felt like overkill and where the routing expressiveness mattered more than raw throughput.

### The Architectural Philosophy

RabbitMQ is built around *exchanges* and *queues*. Producers publish messages to exchanges (not directly to queues), and exchanges apply routing rules to decide which queues receive the message. This abstraction is powerful. A single exchange can intelligently fan out messages to multiple queues based on topic patterns, headers, or exact keys. Consumers bind to queues independently, and queues can be temporary (for one client) or durable (surviving broker restarts).

This model is expressive—you can build complex routing topologies without application code. But that expressiveness comes at the cost of operational complexity. You're managing exchanges, declaring bindings, deciding which queues are durable and which are transient. It's easy to build a tangled web of routing that nobody understands six months later.

### Where RabbitMQ Shines

**Complex content-based routing.** Imagine a logistics company routing shipment events. If a package is "fragile," route it to one team's queue. If it's "perishable," route it to the temperature-monitoring team. If it contains "hazardous materials," notify compliance. In RabbitMQ, you use a headers exchange to inspect headers and route accordingly. In Kafka, you'd need your consumer to do all this filtering in application code. Headers-based routing is RabbitMQ's killer feature for middleware scenarios.

**Flexible topic subscriptions.** Topic exchanges support wildcard matching (`logs.*.error`, `user.payment.*`). Services can subscribe to `#` (everything) or narrow patterns. This is more flexible than Kafka's partition-based model, which doesn't support wildcards. If you have hundreds of event types and services only care about a subset, RabbitMQ makes this expressive.

**Hybrid cloud deployments.** RabbitMQ runs on-premises, in Kubernetes, in VMs, across clouds. You can run a cluster spanning your datacenter and AWS, with replication plugins ensuring messages sync. If your architecture is hybrid, RabbitMQ is simpler than running Kafka across cloud and on-prem.

**Dead-letter queues and retry patterns.** RabbitMQ has built-in DLQ support: if a consumer rejects a message N times, it automatically routes to a DLQ. You can then replay from the DLQ with exponential backoff. Kafka requires you to build this pattern explicitly (though it's not hard).

### Where RabbitMQ Breaks Down

**Throughput ceiling.** A single RabbitMQ cluster maxes out around 50K-500K msgs/sec depending on message size and network. If you need 1M+ msgs/sec, RabbitMQ becomes a bottleneck. You'd have to shard: topic A goes to one cluster, topic B to another. Now you have a coordination problem.

**No native consumer groups.** In Kafka, multiple consumers in the same group read the same topic independently with automatic partition rebalancing. In RabbitMQ, you'd create separate queues per consumer, defeating the point. This is a subtle but critical difference when scaling.

**Replay is cumbersome.** RabbitMQ doesn't have Kafka's offset-based replay model. If a consumer crashes, it resumes from its last acknowledged message. But if you want to replay from scratch (after a bug in consumption logic), you typically re-publish messages to a replay queue. It works, but it's a manual operation, not a first-class feature.

**Memory pressure at scale.** RabbitMQ queues live in memory (or lazily spilled to disk). With millions of messages queued, you need a lot of RAM. Kafka, storing messages on disk in a log structure, is dramatically cheaper at scale.

### Concrete Scenario: When and Why You'd Pick RabbitMQ

You're building an order fulfillment system for a retailer. Orders arrive in the main queue. Routing rules depend on the order type: large corporate orders (>$10K) route to an expedited fulfillment team; international orders route to customs compliance; same-day delivery orders route to the fast-pack queue. You also have notification rules: expensive orders trigger a manual review queue.

RabbitMQ shines here. You create a topic exchange, bind queues with routing keys matching order attributes, and add a dead-letter exchange for orders that fail fulfillment five times. Each team owns their queue independently. The system is expressive and self-documenting: one look at the exchange/queue topology tells you how orders are routed.

If you tried this in Kafka, every consumer would read every order from a central topic and filter client-side. It works, but it's less elegant and harder to monitor (you can't see queue depths per queue, only per partition).

---

## Redis Pub/Sub: Real-Time and Fire-and-Forget

Redis Pub/Sub is the inverse of Kafka. Instead of durability-first with eventual replay, it's ephemeral-by-design. A message is published, delivered immediately to all subscribers, and immediately forgotten. No durability. No disk. No history.

This sounds reckless until you realize it's perfectly correct for entire classes of problems where durability doesn't matter.

### The Architectural Philosophy

Redis Pub/Sub is built for real-time systems where latency is the constraint, not reliability. You publish a message, Redis immediately pushes it to all connected subscribers, and returns. No acknowledgment. No durability. If a subscriber is offline, it misses the message. Forever.

This simplicity is its strength. The code is trivial:

```python
# Publisher
redis.publish("chat:room:123", "Hello!")

# Subscriber
pubsub = redis.pubsub()
pubsub.subscribe("chat:room:123")
for message in pubsub.listen():
    print(message["data"])
```

Compare that to Kafka consumer code with offset tracking, rebalancing, error handling. Redis wins on simplicity by a landslide.

### Where Redis Pub/Sub Shines

**Real-time dashboards.** Imagine a monitoring dashboard showing live metrics. Every second, your monitoring system publishes `{cpu: 45%, memory: 72%}` to a Redis channel. The dashboard subscribes and updates the UI in real-time. If a dashboard refresh takes 3 seconds and misses some updates, nobody cares. You're looking at trend, not precision. Redis Pub/Sub delivers the latency (sub-millisecond) that this needs.

**Live chat and collaborative tools.** When a user sends a message, you publish it to `chat:room:123`. All subscribers (the other users in the room) see it immediately. If someone disconnects mid-sentence, they don't care about replay; they'll catch up by asking "what did I miss?" No durability needed.

**Cache invalidation.** Your cache layer depends on you telling it "key X is now stale." You publish an invalidation event to `cache:invalidate`, thousands of cache nodes subscribe, and within milliseconds all caches are consistent. No durability needed; the message is either delivered or it's not, and either way, you'll invalidate the cache on the next request that misses.

**Real-time leaderboards and game state.** Gaming systems need sub-100ms latency for player interactions. Redis Pub/Sub delivers this. If a leaderboard update is lost because a player disconnected, the client re-syncs from the authoritative source (the game backend) on the next request.

**Operational simplicity.** Redis Pub/Sub has zero operational overhead. You already run Redis for caching. You add Pub/Sub channels. Done. No new infrastructure, no cluster coordination, no consumer offset tracking.

### Where Redis Pub/Sub Breaks Down

**Message loss is guaranteed under load.** If a subscriber can't keep up with publisher throughput, Redis will drop messages from that subscriber's buffer. This is documented, but it bites people. You think you've built a reliable system and you're silently losing notifications. Always test this assumption explicitly.

**No persistence.** Messages vanish on Redis restart. For many use cases, this is fine. For others, it's a deal-breaker. If you need persistence, Redis Streams (different from Pub/Sub) add durability with consumer groups, but then you're paying for durability you might not need.

**No replay.** You cannot ask Redis "give me all messages published to this channel in the last hour." Once they're gone, they're gone. This rules out event sourcing, audit logging, and any system requiring temporal access.

**No consumer group abstraction.** In Kafka, ten subscribers to the same topic form a consumer group and partition the work: subscriber A gets messages 0-99, subscriber B gets 100-199. In Redis Pub/Sub, all subscribers receive all messages. If you want load balancing (different subscribers handling different messages), you have to build it yourself.

**Single broker.**Redis Pub/Sub doesn't cluster well. If your Redis master fails, all subscriptions drop until failover completes. This is acceptable for stateless real-time data (a dashboard refresh will reconnect), but not for anything critical.

### Concrete Scenario: When and Why You'd Pick Redis Pub/Sub

You're building a multiplayer collaborative document editor. When one user types, you need to broadcast keystrokes to all other users in the document, ideally sub-100ms. You also publish cursor positions, selection changes, and collaborative awareness events.

This is Redis Pub/Sub's sweet spot. Durability doesn't matter; if a user's keystroke is missed due to a network blip, they'll retransmit or type again. Replay doesn't matter; you're not rebuilding state from history. You need low latency and simple operations. Redis Pub/Sub delivers both.

If you built this with Kafka, you'd have sub-millisecond message latency, full replay, and the operational complexity of managing Kafka clusters for something that doesn't need it. You'd be solving the wrong problem.

---

## AWS SNS/SQS: Vendor-Locked Simplicity

If your entire infrastructure is AWS, SNS and SQS are pragmatic choices that let you avoid operational burden. I've used them for years across multiple companies, and they're reliable. The trade-off is you're locked in, you can't inspect broker internals, and you're paying per-message instead of per-instance.

### SNS: Fan-Out Distribution

SNS (Simple Notification Service) is a topic-based pub/sub system designed to broadcast messages to multiple subscribers. One publish operation sends to all subscribed endpoints (SQS queues, Lambda functions, HTTP webhooks, email).

**Key characteristics:**
- Delivery: At-least-once (with rare duplicates)
- Ordering: No guarantees (messages can arrive out of order)
- Persistence: 0 seconds (immediate delivery only)
- Replay: No

**Where SNS shines:**

Architectural fan-out patterns. You want one event (an order placed) to trigger multiple workflows: inventory system, payment processing, notification service, analytics. SNS fanout is simpler than maintaining separate queues per subscriber. Publish once, SNS routes to all subscribed queues.

Cost-effective for low-frequency events. You pay per publish + per delivery. If you publish 10K events/day and have 5 subscribers, you pay for 50K deliveries. Cheap. If you publish 1M events/day to 5 subscribers, you pay for 5M deliveries. Still cheaper than running Kafka clusters.

**Where SNS breaks down:**

No message ordering. If you publish events in order (user created → user activated → user subscribed), SNS delivers them to each queue independently, and ordering isn't guaranteed. For order-dependent workflows, this is a problem.

No consumer groups. If you have three instances of a service wanting to read SNS messages, you need three SQS queues subscribed to the same SNS topic. You're duplicating the message across three queues. That's inefficient and expensive.

No built-in retry or dead-letter queue semantics. If a subscriber fails, SNS retries (with exponential backoff) but you don't control the strategy cleanly. Failed messages are just abandoned.

### SQS: Point-to-Point Queuing

SQS (Simple Queue Service) is a durable queue. Producers send messages; one or more consumers poll for messages; each message is processed once (after a visibility timeout) and deleted.

**Key characteristics:**
- Delivery: At-least-once
- Ordering: FIFO queues available (`.fifo` suffix), but limited to 300 msgs/sec
- Persistence: 14-day default retention (configurable up to 15 minutes to 14 days)
- Replay: Limited to retention window (no consumer groups)

**Where SQS shines:**

Simple producer-consumer patterns. You have a web server enqueuing jobs (send an email, resize an image) and workers dequeuing and processing. SQS is perfect. Durability is built-in. Visibility timeout means if a worker crashes mid-job, the message reappears after 30 seconds for another worker. Simple and reliable.

Decoupling services from load spikes. Traffic spike causes web servers to enqueue messages faster than workers can process. SQS buffers them, workers process at their own pace, and you've decoupled producer and consumer.

Serverless architectures. Lambda functions can read from SQS, process, and delete. Auto-scaling is automatic (Lambda scales based on queue depth). No infrastructure to manage.

**Where SQS breaks down:**

Throughput limitations of FIFO queues. FIFO guarantees ordering but maxes out at 300 msgs/sec. Standard SQS queues don't guarantee ordering but can handle bursts. If you need both (high throughput + ordering), you're stuck.

No consumer groups. If you have five workers reading from one queue, the queue partitions work: worker A gets messages 0-99, worker B gets 100-199. But there's no Kafka-style offset tracking. If you redeploy, workers re-poll from the queue, and you might reprocess messages if your handling isn't idempotent.

No multi-queue semantics. If you need messages to be routed based on content (different messages to different workers), SNS becomes necessary. But SNS+SQS topology introduces complexity: configure SNS topic, create SQS queues, subscribe queues to topic, configure DLQ for each queue.

No replay. If a bug in your processing logic is discovered, you can't replay messages from 24 hours ago (unless you configured retention to 24 hours and kept them in the queue). You have to manually republish.

### The SNS + SQS Pattern

The most common pattern: SNS topic fans out to multiple SQS queues. The topic ensures all queues get the message; each queue provides durability and independent processing.

```
Order Placed Event → SNS Topic
                   ├→ SQS Queue (Inventory Team)
                   ├→ SQS Queue (Payment Team)
                   └→ SQS Queue (Analytics)
```

This decouples publishers from subscribers (publisher only knows about SNS, not queues). Each team owns their queue independently. If the inventory service crashes, the analytics service continues processing. If you need to add a new subscriber, you create a new queue and subscribe it to the topic.

The downside: cost. If you publish 1M orders and have 3 subscribers, SNS charges for 1M publishes + 3M deliveries. Whereas Kafka would charge based on cluster cost (fixed), SNS charges based on usage.

### Concrete Scenario: When and Why You'd Pick SNS/SQS

You're a startup with three engineers and limited ops budget. You're building an e-commerce platform on AWS. Orders need to trigger multiple workflows: charge the credit card (synchronously, from the web server), send a confirmation email (asynchronously), update inventory (asynchronously), and send to the fulfillment warehouse.

SNS/SQS is perfect. You enqueue the order to SQS immediately (fast, under 100ms). The web request returns. In the background, SQS workers charge cards, send emails, update inventory. Each system owns its queue. If email sending is slow, payment processing isn't blocked. If a worker crashes, SQS re-queues the message.

If you used Kafka, you'd need to run brokers, monitor replication, manage partitions, and explain to your three-person team why you're running infrastructure. SNS/SQS lets you focus on features.

---

## Google Pub/Sub: Managed Cloud-Native Streaming

Google Pub/Sub is the Google Cloud equivalent of Kafka meets SNS/SQS. It's fully managed, scales to 1M+ msgs/sec, supports consumer groups, and offers global distribution across regions.

### The Architectural Philosophy

Pub/Sub borrows Kafka's consumer group model (subscriptions) but layers on cloud-native simplicity. You create topics and subscriptions; publishers write to topics; subscribers pull from subscriptions. Multiple subscriptions to the same topic form consumer groups. But unlike Kafka, Pub/Sub is serverless—you don't manage brokers or replication; Google does.

It also supports push delivery (Pub/Sub pushes messages to HTTP endpoints) and pull delivery (clients pull on demand). This flexibility is powerful. Real-time services pull messages; batch jobs use push delivery.

### Where Pub/Sub Shines

**Global-scale distribution.** Pub/Sub replicates messages across regions automatically. Publish in us-central1, subscribers in europe-west1 get messages with low latency. If a region fails, subscribers automatically fail over. Kafka multi-region replication is possible but requires explicit configuration and monitoring.

**Cloud-native integration.** Pub/Sub natively triggers Cloud Functions, feeds BigQuery, integrates with Dataflow. If your entire stack is Google Cloud, this is seamless. One Pub/Sub topic feeds your real-time dashboard (via Cloud Functions), your data warehouse (via BigQuery streaming insert), and your batch jobs (via Dataflow).

**Serverless operations.** No brokers to manage, no replication factors to tweak, no ZooKeeper clusters to monitor. You set desired throughput and Google scales infrastructure. Costs scale with usage, not with provisioned capacity.

**Push delivery model.** Unlike Kafka, which requires clients to pull, Pub/Sub can push messages to HTTP endpoints. Useful for triggering functions or webhooks without polling.

### Where Pub/Sub Breaks Down

**Vendor lock-in.** If your business is on Google Cloud and you later want to migrate, Pub/Sub knowledge doesn't transfer. Kafka skills are portable across companies and clouds. Pub/Sub skills are Google-specific.

**Ordering guarantees are weaker.** Pub/Sub offers per-subscription ordering (with a performance cost), but it's not as natural as Kafka's per-partition ordering. Enabling ordering reduces throughput significantly.

**Cost transparency.** You pay per-GB transferred. At billion-message scales, this can surprise you. Kafka costs are upfront (broker instance sizes). You know the cost before you scale.

**Limited replay window.** Default retention is 7 days. You can extend to up to 31 days, but Kafka's "unlimited" retention (if you can afford storage) is more flexible for long-term event sourcing.

### Concrete Scenario: When and Why You'd Pick Pub/Sub

You're a data-intensive company entirely on Google Cloud. You're ingesting clickstream data from millions of users, storing in BigQuery for analysis, and updating real-time dashboards. You also train ML models on historical data.

Pub/Sub is the obvious choice. One topic ingests all events. BigQuery subscriptions populate the data warehouse in real-time. Dataflow jobs (Google's Kafka Streams equivalent) process streams. Cloud Functions trigger alerts. Everything is native to Google Cloud. You don't manage a single broker.

The cost is vendor lock-in. But if you're already all-in on Google Cloud, that's not a downside; it's a feature.

---

## Master Comparison Table: Understanding Trade-offs

Before you read the table below, understand how to weight different factors depending on your scenario. For startups, operational simplicity (AWS SNS/SQS) usually wins. For data companies, replay capability (Kafka) matters. For real-time products, latency (Redis) dominates. No system is universally best; the choice depends on your constraints.

| Technology | Throughput | Delivery Guarantee | Ordering | Persistence | Replay | Operational Complexity | Cost Model | Lock-in |
|---|---|---|---|---|---|---|---|---|
| **Message Queues (Generic)** | 10K-100K msgs/s | At-least-once | Per-queue | Disk | Limited window | High | Per-broker | Low |
| **Apache Kafka** | 1M+ msgs/s | Configurable | Per-partition | Disk | Full history | High | Per-broker | Low |
| **Redis Pub/Sub** | 500K+ msgs/s | At-most-once | Best-effort | Memory | None | Low | Per-instance | Low |
| **AWS SNS/SQS** | 100K+ msgs/s (variable) | At-least-once | FIFO available | Disk (14 days) | Limited window | Very Low | Per-message | High |
| **RabbitMQ** | 50K-500K msgs/s | At-least-once | Per-queue | Disk | Limited window | Very High | Per-broker | Low |
| **Google Pub/Sub** | 1M+ msgs/s | At-least-once | Per-subscription | Disk (7 days) | 7-day window | Very Low | Per-GB transferred | High |

**How to read this table:**

- **Throughput:** Maximum sustained messages per second. This is the ceiling; pick a system that exceeds your peak load by at least 50%.

- **Delivery Guarantee:** At-most-once means some messages can be lost. At-least-once means some messages can be duplicated. Exactly-once is theoretically possible (Kafka transactions) but operationally complex.

- **Ordering:** Per-partition (Kafka) means messages with the same key maintain order. Per-queue (SQS, RabbitMQ) means all messages are ordered. Best-effort (Redis, Pub/Sub without ordering enabled) means ordering isn't guaranteed.

- **Persistence:** Memory systems (Redis) lose data on restart. Disk systems survive failures. Persistence is non-negotiable for durability-critical workloads.

- **Replay:** Full history (Kafka) lets you rebuild state. Limited window (SQS, RabbitMQ) limits how far back you can replay. None (Redis) means no replay at all.

- **Operational Complexity:** Very Low (SNS/SQS, Pub/Sub) means almost no management. High (Kafka, RabbitMQ) means brokers, configuration, monitoring, and debugging.

- **Cost Model:** Per-message (AWS) scales with usage but can surprise you at scale. Per-broker (Kafka) is fixed but requires upfront capacity planning.

---

## The Decision Framework: A Narrative Guide

You've got a new event-driven feature to build. Walk through this decision tree, stopping at the first question that's a hard blocker.

**Question 1: What's your absolute throughput ceiling?** If you need less than 10K msgs/sec, congratulations—almost anything works. RabbitMQ, SQS, even a hand-rolled solution. If you need 100K-1M msgs/sec, Kafka and Pub/Sub are your only reliable options. Above 1M msgs/sec, Kafka (self-managed) or cloud-native services (Pub/Sub, AWS MSK) are your only paths.

**Question 2: Do you need to replay messages or reconstruct historical state?** If yes, you need a system with a commit log and offset tracking: Kafka or Pub/Sub. If no, simpler systems (SQS, RabbitMQ) work. Note: "rebuild state after a bug fix" counts as replay.

**Question 3: Is durability critical?** If messages can be lost (notifications, real-time updates), Redis Pub/Sub is viable and fastest. If durability is non-negotiable (financial transactions, audit logs), all others are fine, but skip Redis.

**Question 4: Do you need complex content-based routing?** If you're routing messages to different queues based on message content (headers, payload), RabbitMQ's exchange patterns are purpose-built. SNS/SQS requires topic subscriptions + filtering. Kafka requires consumer-side filtering.

**Question 5: Where is your infrastructure?** If you're all-in on AWS, SNS/SQS are pragmatic. All-in on Google Cloud, use Pub/Sub. Multi-cloud or on-premises, Kafka is the only portable option.

**Question 6: How much operational complexity can you absorb?** If you have a dedicated platform team, Kafka's complexity is acceptable. If you're three engineers, SNS/SQS will let you ship faster.

Start with Question 1. If your throughput requires Kafka, stop debating—use Kafka. If your requirements demand replay, that's Kafka or Pub/Sub. If your cloud vendor is the constraint, that often overrides other considerations. Most decisions are forced by one or two hard constraints, not by weighing ten factors.

---

## The Hard Trade-offs: What Architects Actually Debate

In real architecture meetings, teams don't compare feature matrices. They debate these thorny trade-offs.

### Operational Complexity vs. Feature Richness

Kafka is dramatically more powerful than SQS if you need replay, consumer groups, or stream processing. But it's also dramatically more operationally complex. I've seen teams choose SQS for a year, then switch to Kafka as requirements grew, and then spend three months migrating. I've also seen teams spend six months managing a Kafka cluster when SQS would have been fine forever.

The pragmatic path: Start with SQS (or Pub/Sub if Google Cloud). Migrate to Kafka only when you have a specific use case (replay, stream processing, billion+ event/day volumes) that SQS can't handle. Operational simplicity compounds; every operational complexity is a 24/7 on-call rotation.

### Vendor Lock-in vs. Vendor-Managed Simplicity

AWS SNS/SQS and Google Pub/Sub abstract away operational burden, but they lock you into a vendor. If you later need multi-cloud or on-premises, migration is painful. Kafka is portable; your Kafka knowledge transfers across companies and clouds.

The pragmatic path: If your business is cloud-native (Stripe, Figma, modern SaaS), vendor services are fine; you're already locked in. If your business requires flexibility (consulting firms, platforms used by other companies), Kafka's portability matters.

### Throughput Ceiling vs. Latency Floor

Kafka prioritizes throughput and durability. A single message might take 100ms to persist. Redis Pub/Sub prioritizes latency (sub-millisecond) and sacrifices durability. If you need both (high throughput AND sub-10ms latency), you're in a bind.

The pragmatic path: Few systems need both. Kafka's latency is acceptable for most event streaming (100ms is fine for fraud detection, analytics, updates). Redis Pub/Sub's lack of durability is fine for notifications, dashboards, real-time updates where loss is tolerable. If you genuinely need both, hybrid approach: use Redis Pub/Sub for real-time channels and Kafka for durability, with a replay mechanism that republishes from Kafka to Redis.

### Per-Message Costs vs. Fixed Infrastructure Costs

SNS/SQS charge per-message (and per-delivery for fan-out). At small scale, this is cheap. At billion-message scale, this becomes your biggest cost. Kafka charges per-broker (fixed) and per-storage-GB. At scale, Kafka is cheaper, but you pay upfront.

The pragmatic path: Calculate the cost at your projected scale. If you'll process 1M msgs/day in a year, SQS costs ~$10/month. If you'll process 1B msgs/day, Kafka (self-managed or MSK) is cheaper. For mid-range (100M msgs/day), run the numbers both ways.

### Consumer Group Complexity vs. Decoupling

Kafka consumer groups are elegant: multiple consumers independently read the same topic with automatic load balancing. But they require careful configuration (group IDs, offset commit strategies). SNS/SQS require you to create separate queues per consumer, which is more boilerplate but simpler to reason about.

The pragmatic path: If you have 3-5 different services reading the same event stream, Kafka consumer groups are worth it. If you have 1-2 consumers, SQS simplicity wins.

---

## Interview Tip

**The architect-level answer to "How would you choose a messaging system?"**

> I'd start with non-functional requirements: throughput (msgs/sec at peak), latency (p99), retention window, and whether the business tolerates message loss. Then I'd check delivery semantics—does exactly-once matter, or is at-least-once with idempotent consumers sufficient?
>
> For **high-volume event streaming with replay needs** (billions of events daily), **Kafka wins**. I've scaled it to handle 100M events/day across financial transactions and user activity. Consumer groups enable elegant multi-subscriber patterns without replicated queues. The cost is operational: you need ZooKeeper (or KRaft mode), monitor broker health, tune replication factors, and debug lag. If you can absorb that complexity, Kafka is unbeatable.
>
> For **AWS-native startups**, **SNS + SQS is pragmatic**. You get durability, fan-out patterns, and auto-scaling without managing brokers. The trade-off is you're vendor-locked and you pay per-message at scale. I'd start here, migrate to Kafka only when SQS becomes a bottleneck.
>
> For **real-time, non-durable messaging** (dashboards, notifications, game state), **Redis Pub/Sub** is the fastest and simplest. Message loss is acceptable in these domains.
>
> **RabbitMQ** is my pick for **complex routing scenarios and hybrid deployments**. The exchange topology is elegant if you need content-based routing. But it doesn't scale to Kafka levels, so I only use it when expressiveness matters more than raw throughput.
>
> The critical architectural decision is always **the partition strategy**. In Kafka, one partition per logical stream ensures ordering but is a bottleneck; you scale horizontally by adding partitions (but can't rebalance them later). In RabbitMQ, you create queues per consumer. In SNS/SQS, each team owns a queue. Get this wrong and you'll regret it for years.
>
> I also consider **team expertise and operational budget**. Kafka requires platform engineers who've debugged rebalancing and offset commits. SNS/SQS requires AWS knowledge but less deep systems expertise. For a startup, bet on velocity. For a platform company, bet on scalability.

---

## See Also

- [Kafka Internals](./03-kafka-internals.md) — Deep dive into topics, partitions, ISR
- [Delivery Semantics](./05-delivery-semantics.md) — At-most-once, at-least-once, exactly-once
- [Kafka Configs](./04-kafka-configs.md) — Production tuning for each messaging approach
