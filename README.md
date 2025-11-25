# wave-mq

Single-node log-based message broker in Go, ready to grow into a cluster. Provides a custom binary protocol, basic MQTT 3.1.1/5.0 frontend (QoS0/1), consumer groups with broker-side offsets, and segmented WAL storage with sparse indexes and retention.

## Project Status

- Core single-node broker (storage, binary protocol, MQTT, consumer groups, HTTP UI/API) — implemented and suitable for local experiments and demos.
- Topic metadata persistence (`metadata.log`) — implemented; topics/partitions survive broker restart.
- Cluster metadata layer (controller + `/api/cluster`) — implemented in single-node and static multi-broker form; Raft-based controller exists as an experimental alternative, not wired into the default binary yet.
- Replication path (leader → follower) — binary client and partition replicator implemented as prototypes; **not** enabled in the default runtime, RF effectively remains 1.
- Multi-node / Raft-backed controller quorum — design and scaffolding in place, production wiring and operations are future work.

## Roadmap

### 1. Core (current state)

- Single-node broker with segmented WAL, sparse index, retention by size/age and crash-recovery.
- Binary protocol server (`netproto`), minimal MQTT frontend, CLI client, HTTP admin API + React UI.
- Persistent topic metadata via `metadata.log`; broker and HTTP API recover topics/partitions after restart.
- Cluster metadata controller:
  - SingleNodeController with `ClusterMetadata` and `/api/cluster` snapshot.
  - Static multi-broker layout via `StaticClusterConfig` and round-robin leaders.
- Experimental RaftController:
  - Single-node Raft FSM for `ClusterMetadata` with commands `AssignTopic` and `ReportReplicaProgress`.
  - Snapshots/restore for cluster metadata, not yet used in the default binary.
- Replication scaffolding:
  - BinaryReplicator (uses existing Fetch over binary protocol and returns records + HighWatermark).
  - PartitionReplicator with `Sink` interface and ISR management API (`ReportReplicaProgress`) in the controller.
  - Not wired into the main broker runtime; RF is effectively 1.

### 2. Multi-broker (future evolution)

This stage is split into three major chapters.

#### 2.1 Integrate Raft controller into the main path

- Make RaftController a drop-in replacement for SingleNodeController behind the existing controller interfaces.
- Wire RaftController into `cmd/mbd` and HTTP:
  - configuration/flag to choose between in-memory controller and Raft-based controller;
  - `/api/cluster` and all metadata-changing operations (`CreateTopic`, future admin APIs) go through Raft.
- Stabilize Raft single-node operation operationally:
  - snapshot cadence, restart behaviour, error reporting endpoints (e.g. expose controller status via HTTP);
  - ensure compatibility with existing tests and UI.

#### 2.2 Wire replication to the Broker and controller

- Connect `PartitionReplicator` to storage:
  - implement a `Sink` that appends records into local WAL and updates per-partition HighWatermark;
  - ensure idempotence and correct offset tracking on follower.
- Start replication loops for follower partitions:
  - derive follower assignments from `ClusterMetadata` (roles/replicas);
  - for each follower partition, run a `PartitionReplicator` pointing at the leader’s `BrokerInfo`.
  - Use `ReportReplicaProgress` to maintain ISR:
  - report follower progress (last applied offset + leader HighWatermark) back to the controller;
  - controller updates `ISR` and `Version` accordingly.
- Tests and safety:
  - single-leader + follower in one process, verify replication and ISR updates;
  - feature flags/configuration to enable or disable replication in runtime.

#### 2.3 Multi-node Raft cluster and multi-broker runtime

- Move from single-node Raft to a multi-peer Raft controller cluster:
  - configure Raft peers (servers) from cluster config;
  - define networking/transport between controller nodes.
- Multi-broker deployment:
  - each broker runs with a unique `BrokerID` and shared cluster config;
  - brokers register with the Raft-backed controller and obtain a cluster view (leaders/replicas/ISR).
- Leader-based client routing:
  - clients discover leaders via Metadata/HTTP APIs and route Produce/Fetch to the correct broker;
  - UI shows multiple brokers, their roles, ISR and high-watermarks.
- Operational scenarios:
  - rolling restarts of brokers and controller nodes;
  - leader failover, ISR shrink/expand, degraded modes.
- Hardening:
  - durability and compatibility of Raft snapshots for `ClusterMetadata`;
  - tooling for inspecting cluster state and performing admin operations.

### 3. Testing and validation

#### 3.1 Integration testing

- In-process tests exercising broker + storage + controller + HTTP:
  - topic/partition lifecycle, restart recovery, `/api/topics`, `/api/cluster` consistency.
- Cluster-aware tests for metadata:
  - static multi-broker layouts, RaftController command application, ISR updates.

#### 3.2 End-to-End (E2E) tests

- Full-path scenarios using the real binaries (or docker-compose):
  - start broker+UI, create topics, produce/consume via CLI, HTTP and MQTT;
  - verify metrics, health checks and UI behaviour end-to-end.
- For multi-broker: end-to-end scenarios with multiple brokers and controller, including basic failover cases.

#### 3.3 Large E2E scenario fuzzing

- Scripted “big test” that runs many randomized scenarios to shake out edge cases:
  - random topic/partition creation, consumer group joins/leaves, produces/fetches, restarts;
  - invariants: no lost acknowledged messages, offsets monotonic per partition, ISR never empty, etc.
- Designed to run long and cover many combinations, closer to system-level fuzzing.

#### 3.4 Stress and load testing

- Microbenchmarks (already present) for storage and broker hot paths.
- Load tests for the binary protocol (e.g. `mbbench`) and MQTT:
  - high message rates, varying value sizes, multiple concurrent producers/consumers;
  - measure throughput, latency percentiles, and resource usage.
- Future multi-broker stress scenarios:
  - uneven load distribution, broker/leader failures under load, ISR churn under pressure.

## Build & Run

```sh
go build ./cmd/mbd
go build ./cmd/mbctl
```

Start broker:
```sh
./mbd -data-dir=./data -bind=:7912 -mqtt=:1883 -http=:8090
```

Create topic and produce/fetch via CLI:
```sh
./mbctl create-topic -topic test -partitions 1
./mbctl produce -topic test -partition 0 -value "hello"
./mbctl fetch -topic test -partition 0 -offset 0
```

MQTT usage: connect any MQTT 3.1.1/5.0 client to `:1883`, SUBSCRIBE to a topic, PUBLISH messages (QoS0/1). MQTT topics map directly to broker topics; partitions chosen via hash.

## Observability

HTTP endpoints (default `:8090`):
- `/metrics` — Prometheus metrics.
- `/healthz` — readiness probe.
- `/debug/pprof/*` — pprof handlers.

Example:
```sh
curl http://localhost:8090/metrics
```

## Docker Compose (broker + UI)

В корне репозитория есть `docker-compose.yml`, который поднимает брокер и UI (`wave-ui`):

```sh
docker compose up --build
```

Порты:
- брокер: `7912` (binary), `1883` (MQTT), `8090` (HTTP/metrics)
- UI: `8080` (nginx со статикой Vite)

Данные брокера хранятся в `wave_data` (volume). UI собирается с `VITE_USE_MOCKS=false` и обращается к HTTP API по адресу `http://broker:8090`.

## Benchmarks & Load

Microbenchmarks:
```sh
go test ./internal/storage -bench=.
go test ./internal/broker -bench=.
```

Simple load generator for the binary protocol:
```sh
go run ./cmd/mbbench -broker 127.0.0.1:7912 -topic bench -messages 20000 -value-size 200 -concurrency 8
```
Outputs produced count, avg/max latency, total time, and RPS.

## Docker

Build image:
```sh
docker build -t wavemq:latest .
```

Run broker (single node):
```sh
docker run --rm \
  -p 7912:7912 -p 1883:1883 -p 8090:8090 \
  -v /path/on/host/data:/data \
  wavemq:latest
```

Defaults: `-data-dir=/data -bind=:7912 -mqtt=:1883 -http=:8090`. Override flags via `docker run wavemq:latest <flags>...`.

Access:
- Binary protocol: localhost:7912 (use `mbctl` on host)
- MQTT: localhost:1883
- Observability: http://localhost:8090/metrics and /healthz

## Notes / Limits
- Single node in the default binary; multi-broker/raft controller are experimental and not wired into the main startup path yet.
- Consumer group coordination is local; offsets persisted via offset WAL.
- MQTT support is minimal (QoS0/1, no retained/will/shared subs).
- Storage uses segmented WAL with sparse index and retention by size/age.
