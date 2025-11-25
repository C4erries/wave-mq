# wave-mq

Single-node log-based message broker in Go, ready to grow into a cluster. Provides a custom binary protocol, basic MQTT 3.1.1/5.0 frontend (QoS0/1), consumer groups with broker-side offsets, and segmented WAL storage with sparse indexes and retention.

## Project Status

- Core single-node broker (storage, binary protocol, MQTT, consumer groups, HTTP UI/API) вЂ” implemented and suitable for local experiments and demos.
- Topic metadata persistence (metadata.log) вЂ” implemented; topics/partitions survive broker restart.
- Cluster metadata layer (controller + /api/cluster) вЂ” implemented in single-node and static multi-broker form; Raft-based controller available via -controller=raft (default single) with optional -raft-dir for state (empty=in-memory).
- Replication path (leader в†’ follower) вЂ” binary client and partition replicator implemented as prototypes; **not** enabled in the default runtime, RF effectively remains 1.
- Multi-node / Raft-backed controller quorum вЂ” supported for local multi-broker clusters; production hardening and operational tooling are ongoing.

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
  - for each follower partition, run a `PartitionReplicator` pointing at the leaderвЂ™s `BrokerInfo`.
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

- Scripted вЂњbig testвЂќ that runs many randomized scenarios to shake out edge cases:
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
- `/metrics` вЂ” Prometheus metrics.
- `/healthz` вЂ” readiness probe.
- `/debug/pprof/*` вЂ” pprof handlers.

Example:
```sh
curl http://localhost:8090/metrics
```

### Two-broker Raft example

Run two brokers sharing one Raft controller cluster:

Broker 1:

```sh
./mbd \
  -broker-id=1 \
  -controller=raft \
  -raft-bind=127.0.0.1:9001 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
  -data-dir=./data1 \
  -bind=:7912 -http=:8091
```

Broker 2:

```sh
./mbd \
  -broker-id=2 \
  -controller=raft \
  -raft-bind=127.0.0.1:9002 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
  -data-dir=./data2 \
  -bind=:8912 -http=:8092
```

Expected behavior: one controller becomes leader; `/api/cluster` on both brokers converges to the same `ClusterMetadata`, and partition leaders are spread across broker IDs. The `/api/controller` endpoint shows `mode=raft`, current `raftState`/`term`, peers, clusterID and version.

#### Operating a Raft cluster
- Bootstrap: start the first broker with the full `-raft-peer` list; ensure `/api/controller` reports a leader and correct `clusterID`.
- Join: start additional brokers with the same `-raft-peer` list; verify they appear in `/api/controller` peers and `/api/cluster` brokers.
- Rolling restart: restart brokers one by one, checking `/api/controller` after each restart to ensure a leader exists and versions keep increasing.

## Docker Compose (broker + UI)

Р’ РєРѕСЂРЅРµ СЂРµРїРѕР·РёС‚РѕСЂРёСЏ РµСЃС‚СЊ `docker-compose.yml`, РєРѕС‚РѕСЂС‹Р№ РїРѕРґРЅРёРјР°РµС‚ Р±СЂРѕРєРµСЂ Рё UI (`wave-ui`):

```sh
docker compose up --build
```

РџРѕСЂС‚С‹:
- Р±СЂРѕРєРµСЂ: `7912` (binary), `1883` (MQTT), `8090` (HTTP/metrics)
- UI: `8080` (nginx СЃРѕ СЃС‚Р°С‚РёРєРѕР№ Vite)

Р”Р°РЅРЅС‹Рµ Р±СЂРѕРєРµСЂР° С…СЂР°РЅСЏС‚СЃСЏ РІ `wave_data` (volume). UI СЃРѕР±РёСЂР°РµС‚СЃСЏ СЃ `VITE_USE_MOCKS=false` Рё РѕР±СЂР°С‰Р°РµС‚СЃСЏ Рє HTTP API РїРѕ Р°РґСЂРµСЃСѓ `http://broker:8090`.

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
