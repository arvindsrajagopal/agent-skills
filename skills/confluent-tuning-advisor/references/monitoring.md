# Monitoring and Metrics for Tuning

Tuning without measurement is guessing. Read this file when the workflow reaches Step 7 (benchmark
and verify) or whenever a user asks "how do I know if this change helped / what should I watch."
The metric that proves a change worked is the same metric that should have an alert on it.

Metric names below are JMX MBeans on **Confluent Platform** brokers and clients. On **Confluent
Cloud** the broker fleet is managed and these broker MBeans are not exposed — use the Cloud
**Metrics API** / the Confluent Cloud console for the server-side metrics they make available, and
the **client** JMX metrics (producer/consumer sections below) for the parts you run. Do not assume
every broker MBean has a direct Metrics API equivalent; discover the current metric descriptors.
See the [Cloud metrics source of truth](confluent-cloud.md#cloud-metrics-source-of-truth) for the
authoritative discovery, metrics-reference, and query documentation.

**If the target is Confluent Cloud, do not copy any broker MBean name from the sections below into
the recommendation.** Those sections exist for Platform targets. For Cloud, take server-side metric
names only from live descriptor discovery, and use this file solely for the client-metrics sections
(producer, consumer, and rebalance), which apply on both platforms.

## Alert on these at minimum

Confluent's own baseline — alert on all three regardless of workload priority:

| Metric | MBean | Healthy value | Meaning |
|---|---|---|---|
| `ActiveControllerCount` | `kafka.controller:type=KafkaController,name=ActiveControllerCount` | sum across brokers **= 1** | ≠1 means no controller or split brain |
| `OfflinePartitionsCount` | `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | **0** | partitions with no leader — not readable or writable |
| `UncleanLeaderElectionsPerSec` | `kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec` | **0** | a nonzero value signals an unclean election and potential loss of acknowledged writes |

## By priority — what to watch when tuning each axis

### Availability
- `UnderReplicatedPartitions` (`kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions`) — **alert if > 0**; ISR is below replica count, so you are one failure away from unavailability.
- `OfflinePartitionsCount > 0`, `ActiveControllerCount ≠ 1`, `LeaderElectionRateAndTimeMs` (nonzero on broker failures).
- Consumer `records-lag-max` rising over time — the group is falling behind (see consumer section).
- `failed-rebalance-rate-per-hour` and `rebalance-rate-per-hour` — rebalance storms are the usual cause of the "whole consumer group stalls on deploy" symptom that `cooperative-sticky` fixes.

### Durability
- `UncleanLeaderElectionsPerSec` (should be **0**) and `UnderReplicatedPartitions`.
- `IsrShrinksPerSec` / `IsrExpandsPerSec` (`kafka.server:type=ReplicaManager,...`) — steady-state **0**; churn means replicas keep dropping out of ISR, putting `min.insync.replicas` at risk.
- `AtMinIsr` / `InSyncReplicasCount` (`kafka.cluster:type=Partition,topic={t},partition={p},name=...`) — a partition sitting at min ISR has no headroom.
- `MaxLag` (`kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica`) rising = replication can't keep up (consider `num.replica.fetchers`).
- Producer `record-error-rate` (client) — acknowledged-write failures surfacing to the app.

### Latency
- Request-latency breakdown, `kafka.network:type=RequestMetrics,name={X},request={Produce|FetchConsumer|FetchFollower}`:
  `TotalTimeMs` = `RequestQueueTimeMs` + `LocalTimeMs` + `RemoteTimeMs` + `ResponseQueueTimeMs` + `ResponseSendTimeMs`. Use the largest component to choose the next check:
  - High `RequestQueueTimeMs`: correlate `RequestQueueSize`, request-handler idleness, and CPU before changing `num.io.threads`; a full or growing queue can also mean request concurrency exceeds processing capacity.
  - High `LocalTimeMs`: investigate local storage and page-cache pressure; correlate `LogFlushRateAndTimeMs` and host disk latency/IO wait.
  - High `RemoteTimeMs`: for produce with `acks=all`, investigate follower/ISR and inter-broker network delay. For an idle or caught-up consumer/follower fetch, a value near the configured fetch wait can be normal rather than network slowness.
  - High `ResponseQueueTimeMs`: correlate network-processor idleness and host network pressure before changing `num.network.threads`.
  - High `ResponseSendTimeMs`: investigate socket backpressure, NIC/network saturation, and slow disk-to-network transfer.
- `NetworkProcessorAvgIdlePercent` (`kafka.network:type=SocketServer,...`) — low values indicate
  network-thread saturation. Establish the alert from the workload baseline and current Confluent
  monitoring guidance rather than treating one threshold as universal.
- `RequestHandlerAvgIdlePercent` (`kafka.server:type=KafkaRequestHandlerPool,...`) — 0 = saturated I/O threads (the signal to consider `num.io.threads`).
- Producer `request-latency-avg` / `record-queue-time-avg`, consumer `fetch-latency-avg`.
- If p99 is high but p50 is fine, correlate with GC pauses before touching Kafka config — see [references/confluent-platform.md](confluent-platform.md).

### Throughput
- `BytesInPerSec` / `BytesOutPerSec` / `MessagesInPerSec` (`kafka.server:type=BrokerTopicMetrics,name=...,topic={t}`; omit topic for cluster-wide).
- `RequestsPerSec` (`kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower}`) and `RequestQueueSize` (`kafka.network:type=RequestChannel,name=RequestQueueSize`) — evaluate request rate alongside byte throughput. Many small requests tend to consume CPU and queue capacity; high MB/s tends to pressure disk and network. When request rate is high but MB/s is modest, inspect producer `batch-size-avg` and `records-per-request-avg`; if batches are small, recommend client batching as the first lever before adding broker threads.
- `RequestHandlerAvgIdlePercent` / `NetworkProcessorAvgIdlePercent` near 0 = broker is the bottleneck (scale brokers/partitions, not client batching).
- Compare `PartitionCount`, `LeaderCount`, byte rates, request rates, and host utilization across
  brokers before calling the cluster capacity-bound. A single hot broker can indicate a failed
  peer, skewed leadership, or skewed replica placement rather than insufficient total capacity.
  On Confluent Platform, consider
  [Self-Balancing Clusters](https://docs.confluent.io/platform/current/clusters/sbc/index.html)
  for continuous imbalance detection and supported reassignment; otherwise recommend an
  administrator-reviewed preferred-leader election or replica reassignment and monitor ISR health
  while data moves.
- Producer `batch-size-avg`, `records-per-request-avg`, `compression-rate-avg`,
  `buffer-available-bytes` (near zero means the producer is backpressured; consult the current
  client-metrics reference for version availability).
- Consumer `fetch-rate`, `records-consumed-rate`, `fetch-size-avg`.

## Client metrics (apply on both Confluent Platform and Confluent Cloud)

You run the clients, so these JMX metrics are available on both platforms.

**Producer** — `kafka.producer:type=producer-metrics,client-id={id}`:
`record-error-rate`, `record-retry-rate`, `request-latency-avg`/`-max`, `batch-size-avg`,
`records-per-request-avg`, `compression-rate-avg`, `buffer-available-bytes`, `record-queue-time-avg`.

**Consumer** — `kafka.consumer:type=consumer-fetch-manager-metrics,client-id={id}`:
`records-lag-max` (the single best "is the group keeping up" signal), `records-lag`,
`fetch-latency-avg`/`-max`, `fetch-rate`, `records-consumed-rate`, `fetch-throttle-time-avg`
(nonzero means a quota is throttling you — relevant on Confluent Cloud).

**Consumer rebalance** — `kafka.consumer:type=consumer-coordinator-metrics,client-id={id}`:
`rebalance-latency-avg`/`-max`, `rebalance-rate-per-hour`, `failed-rebalance-rate-per-hour`,
`last-rebalance-seconds-ago`, `commit-latency-avg`.

## Reference docs

- [Kafka monitoring / metrics reference (Confluent Platform)](https://docs.confluent.io/platform/current/kafka/monitoring.md)
- [Self-Balancing Clusters](https://docs.confluent.io/platform/current/clusters/sbc/index.html)
- [Broker metrics](https://docs.confluent.io/platform/current/kafka/broker-metrics.md)
- [Log & network request metrics](https://docs.confluent.io/platform/current/kafka/log-network-metrics.md)
- [Producer metrics](https://docs.confluent.io/platform/current/kafka/producer-metrics.md) ·
  [Consumer metrics](https://docs.confluent.io/platform/current/kafka/consumer-metrics.md)
- [Monitor Confluent Cloud clients](https://docs.confluent.io/cloud/current/client-apps/monitoring.md)
- [Confluent Cloud Metrics API](https://docs.confluent.io/cloud/current/monitoring/metrics-api.html)
- [Confluent Cloud Metrics Reference](https://api.telemetry.confluent.cloud/docs/descriptors/)
