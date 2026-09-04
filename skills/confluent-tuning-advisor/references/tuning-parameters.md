# Tuning Parameter Matrix

One row per setting, one column per priority. "Recommended" values are starting points for a
typical workload — reconcile against the user's quantified target and actual cluster size before
finalizing the plan (see SKILL.md Step 4).

> **Client defaults vary by version and implementation.** Consult the current client configuration
> reference and phrase recommendations as a diff from the *actual* current value (Step 3 baseline),
> not from a remembered default.

> **Config-key names are the Java (`kafka-clients`) names.** They apply as-is to the librdkafka
> family too — the Python (`confluent-kafka`), Go, C/C++, and .NET clients — so the recommended
> *values* transfer across languages. Two caveats when the client isn't Java: a few keys are aliased
> (notably `linger.ms` is `queue.buffering.max.ms` in librdkafka, and `batch.size` maps to
> `batch.num.messages`/`batch.size` depending on version), and a few defaults differ. Recommend the
> Java key with its value and note the librdkafka alias when you know the user is on a non-Java
> client. This skill does not cover Kafka Streams, Flink, or Kafka Connect clients (see SKILL.md).

## Table of contents

- [Producer configuration](#producer-configuration)
- [Consumer configuration](#consumer-configuration)
- [Topic / replication configuration](#topic--replication-configuration)
- [Broker / cluster configuration (Confluent Platform only)](#broker--cluster-configuration-confluent-platform-only)
- [Cross-cutting notes](#cross-cutting-notes)

---

## Producer configuration

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `acks` | `all` with idempotence; `1` only if the user explicitly accepts disabling idempotence and losing acknowledged writes on leader failure | `all` with idempotence; `1` only for an explicitly accepted durability trade-off | `all` | `all` | `all` waits for every in-sync replica; `1` only waits for the leader. `acks=all` does not require idempotence; the dependency runs the other way: `enable.idempotence=true` requires `acks=all`. Never recommend `acks=1` plus idempotence. |
| `enable.idempotence` | `true` (requires `acks=all`) | `true` (requires `acks=all`) | `true` | `true` (required for exactly-once semantics) | Prevents duplicate writes on retry and preserves ordering with supported in-flight settings. Disable it only when the user explicitly accepts duplicates/reordering and the durability loss required to use `acks=1`. |
| `compression.type` | `none` or `lz4` | `zstd` (best ratio) or `lz4` (best CPU/latency balance) | `lz4` | `lz4` or `zstd` | Compression trades CPU and a small per-batch latency cost for less network/disk I/O. `gzip` has the best ratio but the worst CPU cost — avoid it for latency-sensitive paths. |
| `linger.ms` | `0` (but see note) | `10`–`100` | `5`–`20` | `5`–`20` | Batching improves throughput and reduces per-message overhead, but every millisecond of linger is added latency for the first message in the batch. **Latency nuance:** at *low per-partition produce rate*, a small `linger.ms` (`5`–`10`) can lower p99 by reducing request volume. So `0` is the safe starting point for latency, but benchmark a small linger before assuming `0` is optimal. |
| `batch.size` | client default | `100000`–`200000` | client default | client default | Larger batches amortize request overhead; only helps if `linger.ms` or produce rate actually fills them. Do not raise it for latency without benchmark evidence. |
| `buffer.memory` | default | increase (e.g. `67108864`+) under sustained high-rate produce | default | default | Prevents `Producer is out of buffer space` errors from stalling the app under backpressure. |
| `max.in.flight.requests.per.connection` | `5` (with idempotence) | `5` (with idempotence) | `5` | `5` with idempotence, or `1` only if idempotence is unavailable and strict ordering on retry matters | With `enable.idempotence=true`, up to 5 in-flight requests preserve ordering safely; without it, `5` risks reordering on retry. |
| `delivery.timeout.ms` / `retries` | lower timeout, fail fast | default/higher | higher (ride out transient issues) | higher (never give up before durability is achieved) | A durability- or availability-first workload should retry through transient broker unavailability rather than surface an error to the caller. |

## Consumer configuration

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `fetch.min.bytes` | `1` (default) | increase (Confluent recommends ~`100000`) | default | default | Higher values make the broker wait for more data before responding — better throughput, worse tail latency for sparse topics. |
| `fetch.max.wait.ms` | low (`50`–`100`) | higher (`500`) | default | default | Caps how long the broker waits to satisfy `fetch.min.bytes`; the latency/throughput lever comes as a pair with the setting above. |
| `max.poll.records` | default or lower | increase (e.g. `1000`+) | default | default | Larger batches per poll reduce per-call overhead but increase the time between polls, which risks session timeout if processing is slow. |
| `isolation.level` | `read_committed` if transactional, else `read_uncommitted` | same | same | `read_committed` for transactional workloads | `read_committed` adds a small latency cost (skips uncommitted records) but is required for correctness with transactional producers — treat this as a durability/correctness setting, not optional. |
| `session.timeout.ms` / `heartbeat.interval.ms` | default | default | lower session timeout for faster failure detection, but not so low it causes false-positive rebalances (start at `10000`/`3000`) | default | Faster failure detection speeds up recovery from a dead consumer, improving availability of the consumer group — at the cost of spurious rebalances if set too aggressively. |
| `partition.assignment.strategy` | n/a | n/a | `cooperative-sticky` | n/a | Cooperative rebalancing avoids a stop-the-world pause across the whole group during a rebalance — meaningfully better availability than the eager range/round-robin assignors. |

**Consumer availability when per-record processing is slow:** if a consumer's processing per poll
is slow, the group can rebalance spuriously (a member misses its poll deadline and is evicted). To
keep the group stable, Confluent recommends *increasing* `session.timeout.ms` and
`max.poll.interval.ms` and *reducing* `max.poll.records` — the opposite direction from the
"faster failure detection" tuning above. Which way to go depends on whether the availability risk
is a *dead* consumer (detect faster: lower timeout) or a *slow* consumer (avoid eviction: raise
timeout, smaller polls). Also prefer static group membership (`group.instance.id`) to skip
rebalances entirely on a rolling restart.

## Topic / replication configuration

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `replication.factor` (topic creation/reassignment) | `3` (standard default) | `3` | `3` (minimum for surviving one broker/AZ loss) | `3` | See [Replication factor is a placement operation](#replication-factor-is-a-placement-operation). Below 3, you cannot both tolerate one failure and keep `min.insync.replicas` at 2. This is rarely worth lowering even for "throughput" — it's a replication-traffic cost, not a client-facing one. |
| `min.insync.replicas` | `1` (fewest acks to wait on, only if `acks=1`) | `2` (with `acks=all` and RF 3) | `1`–`2` — lower survives more simultaneous broker loss without blocking writes, at a durability cost | `2` (with RF 3 and `acks=all`) — never `1` | With RF 3, `min.insync.replicas=2` is the standard balance: tolerates one broker down while still requiring 2 durable copies before ack. Dropping to 1 trades away durability for availability during a double failure. |
| `unclean.leader.election.enable` | n/a | n/a | `true` only if the workload can tolerate silent data loss to stay writable during an all-replicas-down scenario | `false`, always | This is the sharpest availability/durability trade-off in Kafka: `true` lets an out-of-sync replica become leader (cluster stays up, but recent acknowledged writes on the lost leader can vanish). Default to `false` unless the user explicitly accepts the loss risk. |
| `cleanup.policy` | n/a | n/a | n/a | `compact` for keyed state topics that must never lose the latest value per key; `delete` otherwise | Compaction is a durability tool for "latest state" semantics, not a performance lever. |
| `num.partitions` | more partitions = more parallel consumers = can lower end-to-end latency under load, but each partition adds replication/metadata overhead | more partitions = more producer/consumer parallelism, up to the point of diminishing returns (broker file-handle/replication overhead) | fewer partitions = faster leader election/failover per partition during a broker loss | n/a | Partition count is a throughput/parallelism lever first; treat "just add partitions" requests skeptically once past what the workload's actual concurrency needs. |
| `retention.ms` / `retention.bytes` | n/a | n/a | n/a | set generously for replay/recovery needs | Longer retention is a durability/recoverability lever (time to notice and replay a bad write), not a live-traffic performance one. |

### Replication factor is a placement operation

Replication factor is not a topic-config key/value change. For a new topic,
set it in the topic-creation request. For an existing topic on Confluent Platform, use an explicit
replica reassignment plan, confirm sufficient broker capacity and replication bandwidth, and monitor
the reassignment and ISR health until it completes. A reassignment moves data and can temporarily
increase network and disk load. Do not delete and recreate an existing topic solely to change RF;
that unnecessarily disrupts clients and risks data loss. On Confluent Cloud, follow the current Cloud documentation for the
cluster tier's supported replication-factor behavior; do not present broker placement or RF as a
user-managed broker setting.

## Broker / cluster configuration (Confluent Platform only)

Confluent Cloud manages all of these — see
[references/confluent-cloud.md](confluent-cloud.md) for what Cloud exposes instead (quotas,
cluster tier, multi-zone placement).

| Setting | Latency | Throughput | Availability | Durability | Why |
|---|---|---|---|---|---|
| `num.network.threads` / `num.io.threads` | increase if CPU-bound on request handling | increase | default | default | Raise these only if broker CPU/thread-pool metrics show saturation — over-provisioning threads on an already-idle broker does nothing. |
| `socket.send.buffer.bytes` / `socket.receive.buffer.bytes` | default | increase on high-bandwidth-delay-product (cross-region) links | default | default | OS-level TCP buffer sizing matters most when brokers and clients are far apart on the network. |
| `num.replica.fetchers` | n/a | increase if replication is a throughput bottleneck | improves ISR catch-up speed after a broker rejoins | improves | More fetcher threads let a recovering replica catch up to the ISR faster, shrinking the window where `min.insync.replicas` is at risk. |
| `replica.lag.time.max.ms` | n/a | n/a | higher = more tolerant of transient slow replicas (avoids unnecessary ISR shrink, keeping `min.insync.replicas` satisfiable) | lower = stricter about what counts as "in sync", closer to true durability guarantees | This is the availability/durability dial for the ISR mechanism itself — raising it papers over slow replicas rather than fixing them. |
| `broker.rack` | n/a | n/a | set to the physical AZ/rack — lets Kafka spread replicas across failure domains | set alongside `min.insync.replicas` for real fault-domain durability | Without rack awareness, all 3 replicas of a partition can land in the same AZ, silently defeating `replication.factor=3` as an availability guarantee. |
| JVM heap / GC (`-Xmx`, G1GC settings) | tune for low GC pause (moderate heap, G1 with low `MaxGCPauseMillis`) | larger heap tolerable | n/a | n/a | See [references/confluent-platform.md](confluent-platform.md) for the p99/GC diagnostic and baseline. |

## Cross-cutting notes

- **Latency vs. throughput is the most direct trade-off**: batching (`linger.ms`, `batch.size`,
  `fetch.min.bytes`/`fetch.max.wait.ms`) is throughput's main lever and latency's main cost.
- **Availability vs. durability is the second axis**: `min.insync.replicas` and
  `unclean.leader.election.enable` are where this actually gets decided — always state which way
  the recommendation leans and why.
- A workload can optimize at most two axes well at once without additional cost (e.g. more
  hardware, more partitions with more brokers). Say so if a user asks for all four.

## Partition sizing

Partition count is the primary throughput/parallelism lever, but it is not free. Size it, don't
guess it.

- **Lower bound (throughput):** you need at least `max(t/p, t/c)` partitions, where `t` = target
  throughput, `p` = throughput you measure on a *single partition* from one producer, `c` =
  throughput one consumer can sustain on a single partition. `c` is application-dependent — measure
  it, don't assume it.
- **Consumer parallelism is capped by partition count:** Kafka gives each partition to exactly one
  consumer in a group, so max consumer parallelism = partition count. Too few partitions serialize
  consumers regardless of client tuning.
- **Upper bounds:** supported and practical partition ceilings depend on the Confluent Platform or
  Cloud version, cluster type, metadata mode, and workload. Check the current platform limits and
  partition-sizing documentation rather than relying on a fixed ceiling.
- **Costs of *more* partitions:** longer leader-election time on an unclean broker failure, more replication overhead and end-to-end
  latency, more open file handles (two files per segment per partition), and more client memory
  (allocate tens of KB of producer buffer per partition). p99 latency grows roughly linearly with
  partitions-per-broker.
- **Ordering caveat:** messages with the same key always route to the same partition and are
  ordered within it. Changing partition count later re-maps keys and breaks that ordering — so
  over-provision partitions up front for anticipated growth rather than repartitioning a keyed
  topic in place.

## Benchmarking and service goals

The core method behind this whole skill (from Confluent's *Optimizing Your Apache Kafka
Deployment*): **define the service goal first, tune toward it, then benchmark to prove it moved** —
there is no one-size-fits-all config, it depends on hardware, data profile, and enabled features.

- Measure **end-to-end latency** as producer `send()` → consumer `poll()`, and report **p99 (tail)
  latency**, not just the average — tail latency is what SLAs are written against.
- Measure throughput as sustained MB/s and msgs/s during the produce phase.
- Real trade-offs are measurable, not just theoretical. Use the linked Confluent benchmark as an
  illustration, but present the administrator's own before/after results rather than carrying its
  hardware-specific figures into a recommendation.
- A before/after benchmark (e.g. `kafka-producer-perf-test` / `kafka-consumer-perf-test`, or the
  administrator's own load-test harness) produces and consumes records — a write — so it is run by
  the **administrator**, never by this advisory-only skill; see SKILL.md Step 7.

## Reference docs

- [Confluent Platform producer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.md)
- [Confluent Platform consumer configuration reference](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.md)
- [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
- [Optimizing Your Apache Kafka Deployment (white paper / blog)](https://www.confluent.io/blog/optimizing-apache-kafka-deployment/)
- [Tail Latency at Scale / Configure Kafka to Minimize Latency](https://www.confluent.io/blog/configure-kafka-to-minimize-latency/)
- [Optimize and Tune Confluent Cloud Clients — per-goal value tables](https://docs.confluent.io/cloud/current/client-apps/optimizing/overview.md)
  ([durability](https://docs.confluent.io/cloud/current/client-apps/optimizing/durability.md) ·
  [throughput](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.md) ·
  [latency](https://docs.confluent.io/cloud/current/client-apps/optimizing/latency.md) ·
  [availability](https://docs.confluent.io/cloud/current/client-apps/optimizing/availability.md))
- [How to Choose the Number of Topics/Partitions in a Kafka Cluster](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/)
