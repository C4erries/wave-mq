# Log-Based Message Broker in Go (Single-Node MVP, Cluster-Ready Design)

> Этот документ описывает задачу курсового проекта и требования к одноузловому брокеру сообщений.  
> Он предназначен и для человека-разработчика, и для ИИ-агента, который будет писать код.

---

## 1. Background & Motivation

Modern data-intensive applications (IoT, telemetry, online analytics) require reliable, low-latency streaming pipelines. Log-based message brokers (Kafka-like commit log architecture) have become a de-facto standard for this class of problems.

The goal of this project is **not** to reimplement Kafka, but to build a **small, understandable, single-node log-based broker** in Go that demonstrates:

- append-only WAL storage with segmented logs and indexes;
- consumer groups and offset management on the broker;
- basic streaming guarantees (at-least-once);
- integration with existing tools via **MQTT**.

At the same time, the broker is designed as the **core of a distributed infrastructure** for data processing and analytics:

- multiple producers and consumers run on different nodes and communicate over the network through the broker;
- the internal model (topics, partitions, consumer groups) is chosen so that the broker can be evolved into a multi-node cluster with replication and leader/follower roles.

The result should be a working prototype that is:

- simple enough to read and reason about,
- realistic enough to run performance experiments,
- **architecturally ready** to be extended to clustering and replication in the future.

---

## 2. Problem Statement

Design and implement a **single-node, log-oriented message broker** in Go with:

- **durable commit-log storage** (segmented WAL + index per partition),
- **pull-based consumption** via a custom binary protocol,
- **consumer groups with broker-side offset commits**,
- **minimal MQTT 3.1.1/5.0 frontend** (at least QoS 0/1),
- **CLI client** for manual testing and scripting.

The broker instance itself is **single-node** (replication factor = 1), but:

- the internal data model must treat topics and partitions as **clusterable entities** (with explicit `BrokerID`, `PartitionRole`, etc.);
- configuration must already expose `BrokerID` and `ReplicationFactor` (with `ReplicationFactor = 1` in this project);
- network protocols and APIs should be defined in a way that they can be reused for inter-broker communication later.

The broker should expose basic metrics and support crash recovery without losing acknowledged messages.

Clustering, partition replication and exactly-once semantics are desirable future directions and are explicitly **out of scope** for the core deliverable, but must be considered in the design.

---

## 3. Scope

### 3.1 In Scope (MVP)

**Core model**

- Topics and partitions:
  - `Topic` is a named logical stream.
  - Each topic consists of 1..N partitions.
  - Each partition is a strictly ordered append-only log with monotonically increasing offsets starting from 0.
  - Partition metadata includes fields that make sense in a cluster:
    - `Topic`, `Partition`, `BrokerID`, `Role` (Leader/Follower), `LeaderEpoch`, etc.
    - In this MVP: `ReplicationFactor = 1`, `Role = Leader`, single `BrokerID`.

- Message model:
  - `(offset, timestamp, key?, headers?, value, crc32c)`.
  - Offsets are assigned by the broker per partition.
  - CRC32C is used for integrity checks on read and during recovery.

**Storage (WAL + index)**

- Each partition on a given broker is stored as:
  - segmented WAL files `*.log` (append-only),
  - sparse index files `*.idx` mapping relative offsets → file positions.

- Features:
  - segment rotation by size and/or time,
  - binary search via index + sequential read from WAL,
  - retention policy by size/time (hard-delete old segments),
  - crash-recovery: on restart, logs and indexes are scanned/validated, truncated to last valid record.

**Broker logic**

- Managing topics, partitions and retention.
- Explicit representation of local partition replicas, e.g.:

```go
  type PartitionRole int

  const (
      RoleLeader PartitionRole = iota
      RoleFollower
  )

  type PartitionReplica struct {
      Topic       string
      Partition   int
      BrokerID    int
      Role        PartitionRole // Leader in this MVP
      LeaderEpoch int32         // may be 0 for MVP
  }
```

* Fetch cursors for consumers (offset-based pull).
* **Consumer groups:**

  * group coordinator on the single broker;
  * simple assignment strategy (range or round-robin);
  * offsets stored and retained on the broker.

**Protocols / Interfaces**

1. **Custom binary protocol over TCP** (primary data-plane):

   * Frame format: `len32 | apiKey | apiVer | corrId | flags | payload`.
   * Commands:

     * `CreateTopic`
     * `Produce`
     * `Fetch`
     * `ListOffsets`
     * `CommitOffset`
     * `FetchCommitted`
     * `Metadata`
     * `Ping`
   * Error codes and clear semantics for invalid requests.
   * Design so that the same protocol (or its subset) can later be reused for:

     * client ↔ broker traffic (already in MVP),
     * broker ↔ broker traffic (future replication and metadata sync).

2. **MQTT frontend (TCP)**

   * Minimal support for MQTT 3.1.1/5.0:

     * `CONNECT/CONNACK`
     * `SUBSCRIBE/SUBACK`
     * `PUBLISH` (QoS 0 and 1)
     * `PUBACK`
     * `PINGREQ/PINGRESP`
     * `DISCONNECT`
   * Mapping:

     * MQTT topic → internal `(topic, partitionByKey)`.
     * QoS1 mapped to WAL write: `PUBACK` is sent after message is durably appended (policy may be configurable later).

3. **CLI `mbctl`**

   * Commands (MVP):

     * `mbctl create-topic ...`
     * `mbctl produce ...`
     * `mbctl fetch ...`
   * Uses the custom binary protocol.

**Observability & ops**

* HTTP endpoints:

  * `/metrics` (Prometheus format),
  * `/healthz` (readiness/liveness check).
* CPU/heap/block profiling via `pprof`.
* Structured logging (e.g. `zap`), with clearly tagged events (storage, network, broker).

---

### 3.2 Out of Scope (for this course project)

May be sketched but not required to be implemented:

* Multi-node clustering and metadata replication (controller, Raft, etc.).
* Partition replication (leader/follower, ISR, epochs, HWMs).
* Exactly-once delivery and transactions.
* Full Kafka protocol compatibility.
* Complex MQTT features: persistent sessions, retained messages, will messages (LWT), shared subscriptions, etc.
* Advanced storage optimizations: compression (lz4/zstd/snappy), tiered storage, log compaction by key.

---

## 4. Non-Functional Requirements

Target (approximate) non-functional goals on a typical laptop (no strict guarantees, but used as benchmarks):

* **Throughput**:

  * 100–200k messages/sec for message size 100–500 bytes, no compression.
* **Latency**:

  * p99 fetch latency < 10–20 ms under moderate load.
* **Durability & crash recovery**:

  * No loss of acknowledged appends after a crash.
  * Consistent offsets and index state after restart.
* **Stability**:

  * No data races under `go test -race`.
  * Predictable behaviour under load (no panics, controlled resource usage).

---

## 5. Technology & Constraints

* **Language**: Go ≥ 1.22.
* **Networking**:

  * Standard `net` package for TCP server(s).
  * Design should allow future refactoring to event-loop/reactor style (ideas from `gnet`), but without hard dependency.
* **Storage**:

  * File-based WAL and index.
  * Optional `mmap` for index if it simplifies random access (not mandatory).
* **Dependencies (allowed/preferred)**:

  * Logging: `uber-go/zap`
  * Metrics: `prometheus/client_golang`
  * Hashing/sharding: `cespare/xxhash/v2` and (optionally) `dgryski/go-jump` for partition selection
  * Consistent hashing (future clustering): `buraksezer/consistent`
  * Compression (optional future): `golang/snappy`, `klauspost/compress`
  * Consensus (future clustering): `hashicorp/raft`
* **Testing**:

  * `go test`, `-race`, fuzzing (`go test -fuzz` where applicable).
  * Benchmarks and microbenchmarks where it matters (storage, network path).
  * Scenario-level load generators via CLI or MQTT (k6 or custom scripts).

---

## 6. High-Level Architecture

Directory layout (planned):

```text
/cmd/mbd              # broker binary
/cmd/mbctl            # CLI client
/internal/netproto    # binary protocol: codec, server, handlers
/internal/broker      # topics, partitions, consumer groups, offsets, metadata
/internal/storage     # WAL, index, recovery
/internal/mqtt        # MQTT frontend → broker core
/pkg/api              # public structs/constants (wire formats, config)
```

Conceptual modules:

1. **Storage layer (`internal/storage`)**

   * Append-only WAL and segmented logs.
   * Sparse index.
   * Recovery.

2. **Broker core (`internal/broker`)**

   * Manages topics/partitions and local replicas (with `BrokerID` and `Role`).
   * Applies retention policies.
   * Coordinates consumer groups.
   * Exposes a clean Go API to network frontends.

3. **Network frontends**

   * Binary protocol server (`internal/netproto`).
   * MQTT server (`internal/mqtt`).

4. **Tools & utilities**

   * CLI (`cmd/mbctl`).
   * Observability endpoints (pprof, Prometheus, health).

---

## 7. Milestones & Acceptance Criteria

### Milestone 0 — Design

* Markdown spec (or Go doc) of:

  * segment and index layout,
  * on-disk formats,
  * binary protocol and error codes,
  * `acks` policy and retention behaviour,
  * partition metadata model, including `BrokerID`, `PartitionRole`, `LeaderEpoch`.
* **Acceptance**:

  * Invariants for crash recovery clearly stated.
  * Serialisation formats have example test vectors.
  * Single-node assumptions are explicit (e.g. `ReplicationFactor = 1`), and data structures allow future extension to multi-node.

### Milestone 1 — Single-node core: storage + broker + CLI

* `storage`:

  * WAL append,
  * segmentation & rotation,
  * index structure and binary search,
  * basic retention,
  * crash-recovery procedure.
* `broker`:

  * API to publish/fetch by offsets,
  * topic/partition management,
  * explicit representation of local partition replicas with `BrokerID` and `Role`.
* `netproto`:

  * TCP server, frame codec, request handlers.
* `mbctl`:

  * `create-topic`, `produce`, `fetch`.
* **Acceptance**:

  * Crash-recovery tests (e.g. `kill -9` during writes) pass without losing acknowledged messages.
  * Throughput / latency micro-benchmarks with short report.

### Milestone 2 — Consumer groups & offset commits

* Single-node group coordinator.
* Offset storage and retention on broker.
* **Acceptance**:

  * Two consumers in the same group do not process the same partition concurrently.
  * On consumer exit/failure, remaining consumers get reassigned partitions.

### Milestone 3 — MQTT frontend

* Minimal MQTT server with mapping to broker API.
* QoS1 → WAL write, `PUBACK` after durable append.
* **Acceptance**:

  * MATLAB script (or similar) can publish and receive messages via MQTT port.
  * Simple MQTT client tests for connect/subscribe/publish/disconnect.

### Milestone 4 — Observability & experiments

* `/metrics`, `/healthz`, pprof integration.
* Load scenarios described and executed.
* Short README/report with numeric results.

---

## 8. Future Work: from Single Node to Cluster

Not required for the course deliverable, but the design should make these natural extensions:

* Cluster controller with Raft-based metadata (which broker owns which partitions, leader election).
* Partition replication (leader/follower, ISR, high-watermark, epochs).
* Reusing the binary protocol for inter-broker replication and metadata sync.
* Kafka bridge or connector.
* Exactly-once semantics and idempotent producers.

The current single-node broker should be implemented in such a way that going from `ReplicationFactor = 1` to `ReplicationFactor > 1` is primarily a matter of **adding new components and protocols**, not of rewriting storage or core APIs.
