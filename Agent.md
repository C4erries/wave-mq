# Agent Instructions for the Go Message Broker Project
# (Single-Node MVP, Cluster-Ready Design)

> This document is written **for an AI coding agent** that will work on the repository.  
> The primary language is Go. The main goal is to implement a single-node log-based message broker as described in `Problem.md`, with an architecture that can later be extended to a multi-node cluster.

---

## 1. Your Role

You are an assistant software engineer working on a Go project.

Your responsibilities:

- Implement and maintain the broker codebase according to the requirements in `Problem.md`.
- Follow idiomatic Go practices and keep the design simple and explicit.
- Ensure correctness, safety (no data races), and reasonable performance.
- Provide tests, benchmarks, and minimal documentation for the implemented features.
- **Very important:** design the single-node broker in a way that is **cluster-ready**, i.e. future multi-node support should be mostly additive.

You should **not** generate random files or large unused abstractions. Everything you create must serve the broker’s goals.

---

## 2. Repository Layout & Modules

Assume (or create) the following structure:

```text
/cmd/mbd              # broker binary (main server)
/cmd/mbctl            # CLI client for manual usage and tests
/internal/netproto    # custom binary protocol: codec, server, handlers
/internal/broker      # topics, partitions, consumer groups, offsets, metadata
/internal/storage     # WAL, segmented logs, indexes, recovery
/internal/mqtt        # MQTT frontend mapping MQTT → broker core
/pkg/api              # public structs/constants shared across components
/scripts              # benchmarks, load tests, helper scripts (if needed)
```

Guidelines:

* Code that should **not** be imported by external packages goes under `/internal/...`.
* Anything that might be reused or referenced externally (e.g. API structs, error codes, basic configs) can live in `/pkg/api`.
* Do **not** create unnecessary nesting. Prefer small, focused packages.

---

## 3. Technology & Libraries

Use Go ≥ 1.22 and the standard library where possible.

Allowed/preferred third-party dependencies (only add them when actually needed):

* Logging: `go.uber.org/zap`
* Metrics: `github.com/prometheus/client_golang/prometheus` and HTTP handler from `prometheus/promhttp`
* Hashing: `github.com/cespare/xxhash/v2`
* Partition selection (optional): `github.com/dgryski/go-jump`
* Consistent hashing (future clustering): `github.com/buraksezer/consistent`
* Compression (optional): `github.com/golang/snappy`, `github.com/klauspost/compress`
* Consensus (future clustering, not MVP): `github.com/hashicorp/raft`

When adding dependencies:

* Update `go.mod` and `go.sum`.
* Keep the set of dependencies small.

Do not introduce heavy MQ/MQTT “all-in-one” frameworks; the MQTT layer should be minimal and transparent.

---

## 4. Design Principles

When making design decisions, follow these principles:

1. **Simplicity first**

   * Prefer straightforward, explicit code over clever abstractions.
   * A small, readable implementation is better than a generic but complex framework.

2. **Correctness over premature optimization**

   * First make it correct and well-tested.
   * Then optimize bottlenecks, guided by benchmarks and pprof profiles.

3. **Crash safety**

   * WAL writes must be durable up to a clear point (e.g. after `fdatasync`/`fsync`).
   * Recovery logic must handle partial writes and truncate corrupted tails.

4. **Concurrency safety**

   * Avoid shared mutable state without synchronization.
   * Use `sync.Mutex`, `sync.RWMutex`, `sync.Cond`, channels, or other primitives correctly.
   * Run `go test -race` regularly.

5. **Clear layer boundaries**

   * `storage` exposes a clean API: append/read by offset, segment metadata, recovery functions.
   * `broker` owns domain logic: topics, partitions, local replicas, groups, offset commits.
   * `netproto` and `mqtt` translate network frames to broker calls, not the other way around.

6. **Cluster-ready data model**

   * Do **not** hardcode “single node” assumptions into types and APIs.
   * Always keep `BrokerID`, `PartitionRole` (Leader/Follower) and `ReplicationFactor` in configuration and metadata, even if the MVP runs with `BrokerID = 1` and `ReplicationFactor = 1`.

7. **Observability**

   * Add logs at key points (startup, errors, state changes).
   * Expose metrics for throughput, latency, and errors.

---

## 5. Core Config and Metadata (Cluster-Ready)

Even though this project is single-node, you must structure configuration and metadata as if a cluster could exist.

### 5.1 Broker configuration

Define a config that already includes cluster-related fields:

```go
type BrokerConfig struct {
    BrokerID          int   // unique numeric ID of this broker
    ReplicationFactor int   // default RF for new topics; MVP: 1
    DataDir           string
    // network ports, timeouts, etc.
}
```

In the MVP:

* `BrokerID` can be hardcoded to `1` or read from config/ENV.
* `ReplicationFactor` must exist but is used only for validation and future planning (`>= 1`, currently always `1`).

### 5.2 Partition metadata

Use explicit types:

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
    Role        PartitionRole // MVP: always RoleLeader
    LeaderEpoch int32         // MVP: can stay 0; reserved for the future
}
```

In the MVP:

* Every partition has a single `PartitionReplica` with `BrokerID = cfg.BrokerID` and `Role = RoleLeader`.
* These types must be used by the broker layer instead of ad-hoc maps or bare `int` indices.

This ensures that later, when a real cluster is introduced, you can add replicas and followers **without rewriting storage and broker APIs**.

---

## 6. Implementation Roadmap

When implementing or extending the project, follow this approximate order.

### Step 0 — Storage design & basic implementation (`internal/storage`)

Goals:

* Implement segmented append-only WAL with index files.
* Define on-disk formats, including headers and CRC32C.

Tasks:

* Define core record type and encoding (binary format for messages).
* Implement:

  * Segment abstraction (`logSegment`): open/close, append, sequential read.
  * Index abstraction (`index`): append entries (relativeOffset → filePos), binary search.
  * Segment rotation by size (and optionally by time).
* Implement crash-recovery:

  * Scan last segment(s), verify CRC32C.
  * Truncate corrupted tail.
* Write tests:

  * Append/read roundtrip.
  * Crash simulation: write partial records, then run recovery and validate.

> **Cluster note:** storage is purely local. Do not put cluster logic (leader election, replication) into `internal/storage`.

---

### Step 1 — Broker core (`internal/broker`)

Goals:

* Model topics, partitions, and mapping to storage.
* Provide high-level methods for produce/fetch.
* Represent partition replicas with cluster-ready metadata types.

Tasks:

* Define:

  * `Broker` struct with `BrokerConfig` and maps of topics/partitions.
  * `TopicMetadata`, `PartitionMetadata`, `PartitionReplica`.
* Implement:

  * `CreateTopic(...)` (idempotent) — creates local replicas according to `ReplicationFactor` (MVP: RF=1).
  * `Produce(ctx, topic, partition, records)` → append to the local leader replica and return offset(s).
  * `Fetch(ctx, topic, partition, fromOffset, maxBytes)` → slice of messages.
* Ensure:

  * Thread-safe operations when multiple clients access broker concurrently.
  * No hidden assumptions like “all partitions are here and are leaders” — always go through metadata structs.

> **Cluster note:** later, some partitions may be leaders on other brokers; metadata and APIs must be ready for that, even if now all leaders are local.

---

### Step 2 — Network protocol (`internal/netproto`) and CLI (`cmd/mbctl`)

Goals:

* Implement binary protocol framing and command handlers.
* Provide a simple CLI tool to interact with the broker.

Tasks:

* Implement frame codec:

  * Length-prefixed messages: `len32 | apiKey | apiVer | corrId | flags | payload`.
  * Encode/decode primitives (varints, strings, arrays) in a simple, documented way.
* Define request/response structs in `/pkg/api` or inside `netproto`.
* Implement handlers:

  * `CreateTopic`, `Produce`, `Fetch`, `ListOffsets`, `CommitOffset`, `FetchCommitted`, `Metadata`, `Ping`.
* Implement server:

  * TCP listener.
  * Per-connection goroutine / handler loop.
* Implement `mbctl`:

  * Subcommands: `create-topic`, `produce`, `fetch`.
  * Use the same binary protocol.

> **Cluster note:** design the protocol so that it can later be reused for broker↔broker RPC (metadata sync, replication fetch) with minimal changes.

---

### Step 3 — Consumer groups & offset commits (`internal/broker`)

Goals:

* Support consumer groups with broker-side offset storage.

Tasks:

* Design structures:

  * `ConsumerGroup`, `GroupMember`, `Assignment`.
* Implement:

  * Group registration/joining/leaving (can be simple for MVP).
  * Partition assignment strategy (range or round-robin).
  * Offset commit and fetch.
* Add tests:

  * Two consumers in the same group do not read the same partition simultaneously.
  * On consumer exit/failure, remaining consumers get reassigned partitions.

> **Cluster note:** group coordination is local to this broker now. Design APIs so that a “group coordinator broker” can later be introduced without breaking clients.

---

### Step 4 — MQTT frontend (`internal/mqtt`)

Goals:

* Implement minimal MQTT server and map it to the broker API.

Tasks:

* Implement MQTT packet parser and serializer:

  * For `CONNECT`, `CONNACK`, `SUBSCRIBE`, `SUBACK`, `PUBLISH` (QoS 0/1), `PUBACK`, `PINGREQ`, `PINGRESP`, `DISCONNECT`.
* Map MQTT topics to internal `(topic, partition)`:

  * Use key-hash (e.g. xxhash + jump consistent hash) for partition choice.
* For QoS 1:

  * Append message to WAL via broker.
  * After durable append, send `PUBACK`.
* Keep session handling minimal; no need for full persistent sessions or retained messages in MVP.

> **Cluster note:** MQTT clients should talk to any broker instance in a future cluster; keep the mapping logic clearly separated so it can be adjusted later.

---

### Step 5 — Observability & performance

Goals:

* Make the broker observable and provide basic performance numbers.

Tasks:

* Add:

  * `/metrics` endpoint with Prometheus metrics.
  * `/healthz` endpoint.
  * `pprof` handlers.
* Export metrics for:

  * Number of messages produced/consumed.
  * Latency histograms (if feasible).
  * Errors.
* Add benchmarks and/or scripts in `/scripts` for load testing.

---

## 7. Coding Conventions

Follow standard Go conventions:

* Use `go fmt`, `go vet`, `golangci-lint` if configured.
* Exported types and functions must have GoDoc comments where appropriate.
* Error handling:

  * return `error` values, do not panic except for truly unrecoverable situations at startup.
  * wrap errors with context where needed.
* Logging:

  * Use `zap.Logger` when logging is required.
  * Avoid logging in tight inner loops with very high frequency.

Concurrency & context:

* For long-running operations, accept `context.Context` parameters and respect cancellation.
* Avoid global mutable state; put configuration and dependencies into explicit structs.

Testing:

* Ensure `go test ./...` passes.
* Run `go test -race ./...` regularly.
* Add fuzz tests where it makes sense (decoders, parsers).
* Add benchmarks for critical paths (`*_test.go` with `Benchmark...`).

---

## 8. What You Should Not Do (Unless Explicitly Asked)

* Do not implement:

  * multi-node clustering and replication (controller, ISR, Raft),
  * exactly-once semantics,
  * full Kafka protocol compatibility.
* Do not introduce heavy dependencies or frameworks (gRPC, huge MQTT servers, ORMs) unless explicitly justified by `Problem.md` or user instructions.
* Do not change on-disk formats or wire protocols silently. If a change is necessary, make it explicit in comments and tests.
* Do not hardcode assumptions like “there is only one broker” into types; always use `BrokerID` and metadata structures.

---

## 9. Definition of Done (per feature)

A feature can be considered **done** when:

1. Code is implemented and integrated into the repository structure.
2. There are unit tests and/or integration tests:

   * happy-path,
   * at least one failure-path or edge case.
3. `go test ./...` passes without race conditions (ideally `-race` as well).
4. Public APIs are documented with GoDoc comments.
5. For observable features:

   * relevant metrics or logs are exposed.
6. There are no obvious TODOs left that are critical for correctness.

If something is intentionally simplified or left incomplete, mark it clearly with a `TODO` comment that explains the limitation and, where relevant, how it relates to future clustering.

