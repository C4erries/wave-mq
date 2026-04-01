# wave-mq

Single-node log-based message broker in Go, designed to grow into a small but realistic Kafka-like cluster. It provides a custom binary protocol, a minimal MQTT 3.1.1/5.0 frontend (QoS0/1), consumer groups with broker-side offsets, and segmented WAL storage with sparse indexes and retention.

For a detailed architectural overview (in Russian) see: `docs/architecture.md`.

## Project Status

> ⚠️ Clustering, Raft, and follower replication are **experimental**. Expect breaking changes, resets during development, and behavior suitable only for demos/labs. Run with replication disabled (`-replication=false`) if you need a stable single-node broker.

- Core single-node broker (storage, binary protocol, MQTT, consumer groups, HTTP UI/API) - implemented and suitable for local experiments and demos.
- Topic metadata persistence (`metadata.log`) - implemented; topics and partitions survive broker restart and are recovered on startup. In clustered modes `metadata.log` should be viewed as a **local cache**; the controller's view of the cluster is authoritative.
- Cluster metadata layer (controller + `/api/cluster`) - implemented for single-node and small multi-broker clusters. A Raft-based controller is available via `-controller=raft` (single-node or multi-peer) and is currently an **experimental** clustered mode; `-controller=single` keeps the simpler in-memory controller.
- Replication path (leader <-> follower) - binary client (`BinaryReplicator`) and partition replicator (`PartitionReplicator` + WAL sink + ISR reporting) are implemented and exercised with RF=2 scenarios. RF>1 clustering is **intended for lab and demo use**, not production; clients should write/read to leaders (followers return `ErrNotLeader` / HTTP 409 with `leaderBrokerID`), metadata exposes leader/replica layout for routing, and replication metrics (`wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`) track follower progress.
- Multi-node / Raft-backed controller quorum - available for local multi-broker clusters as an experimental feature. Basic failover and rolling restart scenarios have tests; further operational hardening, durable Raft state on disk, and stronger guarantees are ongoing work.

## Roadmap

### 1. Core (current state)

- Single-node broker with segmented WAL, sparse index, retention by size/age and crash-recovery.
- Default retention deletion is disabled for single-node safety (`-retention-bytes=-1`, `-retention-hours=0`). Log cleanup is opt-in only.
- Binary protocol server (`internal/netproto`), minimal MQTT frontend (`internal/mqtt`), CLI client (`cmd/mbctl`), HTTP admin API + React UI (`wave-ui`).
- Persistent topic metadata via `metadata.log`; broker and HTTP API recover topics/partitions after restart.
- Cluster metadata controller:
  - `SingleNodeController` with `ClusterMetadata` and `/api/cluster` snapshot for simple/single-node setups.
  - `RaftController`: Raft-backed FSM for `ClusterMetadata` (single-node or multi-peer), with commands `AssignTopic`, `RegisterBroker` and `ReportReplicaProgress`, plus snapshots/restore.
- Replication:
  - `BinaryReplicator` (uses existing Fetch over the binary protocol and returns records + HighWatermark).
  - `PartitionReplicator` with `Sink` interface (WAL sink + reporting sink) and ISR management API (`ReportReplicaProgress`) in the controller.
  - Replication is opt-in via `-replication`; by default the broker still behaves as RF=1 leader-only.

### 2. Multi-broker (cluster mode)

This stage is largely implemented and focuses on practical cluster behavior.

#### 2.1 Raft controller in the main path

- RaftController is a drop-in implementation of the controller interfaces; it can be selected via:

  ```sh
  -controller=raft \
  -raft-bind=<host:port> \
  -raft-peer=<host1:port1,host2:port2,...> \
  -raft-dir=<path or empty for in-memory>
  ```

- `/api/cluster` and all metadata-changing operations (`CreateTopic`, controller-side assignments, ISR updates) go through Raft when `controller=raft`.
- Raft single-node and multi-peer operation:
  - in-memory mode for tests/dev;
  - TCP transport for real multi-node clusters, with configurable peers and timeouts.

#### 2.2 Replication wired to Broker and controller

- `PartitionReplicator` is connected to storage via WAL sink:
  - appends records into local WAL for follower partitions;
  - tracks last applied offset.
- Replication manager:
  - derives follower assignments from `ClusterMetadata` (roles/replicas);
  - for each follower partition, runs a `PartitionReplicator` pointing at the leader's `BrokerInfo`
    (via the binary protocol).
  - uses `ReportReplicaProgress` to maintain ISR:
    - reports follower progress (last applied offset + leader HighWatermark) back to the controller;
    - controller updates `ISR` and `Version` accordingly.
- Tests cover single-leader + follower in one process, verifying replication and ISR updates. Replication is controlled by `-replication` and can be enabled per deployment.
- Client-facing APIs enforce leader-only writes: metadata and HTTP `/api/topics` responses surface the leader/replica layout, produce/fetch on followers return `ErrNotLeader` (binary) or HTTP 409 with `leaderBrokerID`, and the RF>1 end-to-end test covers metadata, leader routing, and follower errors.
- Prometheus metrics `wavemq_replication_lag_offsets` and `wavemq_replication_applied_total` report follower lag and applied record counts so monitoring can track replication progress.

#### 2.3 Multi-node Raft cluster and multi-broker runtime

- Multi-peer Raft controller cluster:
  - peers configured via `RaftBindAddr` and `RaftPeers`;
  - RaftController uses TCP transport for production and in-memory transport in tests.
- Multi-broker deployment:
  - each broker runs with a unique `BrokerID` and shared cluster config;
  - brokers register with the Raft-backed controller via `RegisterBroker` and obtain a cluster view (leaders/replicas/ISR) via `ClusterMetadata`.
- Broker/cluster awareness:
  - on startup, a broker opens only the partitions it owns (where it is leader or a replica) according to `ClusterMetadata`;
  - client-facing APIs (`/api/topics`, `/api/topics/:name`, `/api/topics/:name/partitions/:id/messages`) reflect only partitions actually served by that broker.
- Leader-based client routing (implemented):
  - clients can discover leaders via the binary Metadata API and HTTP `/api/topics`/`/api/cluster` (leaders and replica sets are exposed per partition);
  - produce/fetch must target leaders; followers reject requests with `ErrNotLeader` (binary) or HTTP 409 plus `leaderBrokerID` hint;
  - metrics (`wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`) and `/api/controller` help monitor replication state.
- Operational scenarios:
  - rolling restarts of brokers and controller nodes;
  - leader failover (tested: after leader shutdown, a new leader is elected and continues applying commands);
  - ISR shrink/expand and degraded modes.
- Hardening:
  - durability and compatibility of Raft snapshots for `ClusterMetadata`;
  - `/api/controller` exposes controller state (mode, Raft state, term, peers, clusterID, metadata version) for operators;
  - documented bootstrap/join/rolling restart procedures (see below).

### 3. Testing and validation

#### 3.1 Integration testing

- In-process tests exercising broker + storage + controller + HTTP:
  - topic/partition lifecycle, restart recovery, `/api/topics`, `/api/cluster` consistency.
- Cluster-aware tests for metadata:
  - static multi-broker layouts, RaftController command application, ISR updates, broker's awareness of its partitions.

#### 3.2 End-to-End (E2E) tests

- Full-path scenarios using the real binaries (or docker-compose):
  - start broker+UI, create topics, produce/consume via CLI, HTTP and MQTT;
  - verify metrics, health checks and UI behavior end-to-end.
- For multi-broker:
  - scenarios with several brokers and a Raft controller cluster, including basic failover cases.

#### 3.3 Large E2E scenario fuzzing

- Scripted "big test" that runs many randomized scenarios to shake out edge cases:
  - random topic/partition creation, consumer group joins/leaves, produces/fetches, restarts;
  - invariants: no lost acknowledged messages, offsets monotonically increasing per partition, ISR never empty, etc.
- Designed to run for a long time and cover many combinations, closer to system-level fuzzing.

#### 3.4 Stress and load testing

- Microbenchmarks for storage and broker hot paths:

  ```sh
  go test ./internal/storage -bench=.
  go test ./internal/broker -bench=.
  ```

- Load tests for the binary protocol and MQTT:
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

MQTT usage: connect any MQTT 3.1.1/5.0 client to `:1883`, SUBSCRIBE to a topic, PUBLISH messages (QoS0/1). MQTT topics map directly to broker topics; partitions are chosen via hash.

## Cluster / experimental mode

Raft-backed clustering (`-controller=raft`) and RF>1 replication are **experimental** features that shine in labs, demos, and controlled multi-broker testbeds rather than production deployments. The controller stores `ClusterMetadata` on disk (`-raft-dir`), brokers bootstrap partitions strictly from `ClusterMetadata`, and follower replication is opt-in via `-replication=true`. The script `scripts/raft-cluster-demo.sh` automates a 2-broker RF=2 scenario including topic creation, leader writes, follower rejections (HTTP 409 with `leaderBrokerID`), and a follower restart that shows replication catching up.

Quickstart outline:

1. Build the binaries:

   ```sh
   go build ./cmd/mbd
   go build ./cmd/mbctl
   ```

2. Pick Raft peer addresses and directories; for a 2-broker RF=2 demo, you can use:

   - `127.0.0.1:9001`, data `./data1`, HTTP `:8091`, binary `:7912`
   - `127.0.0.1:9002`, data `./data2`, HTTP `:8092`, binary `:8912`

3. Start broker/controller 1:

```sh
./mbd \
  -broker-id=1 \
  -controller=raft \
  -raft-bind=127.0.0.1:9001 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
  -raft-dir=./data1/raft \
  -data-dir=./data1 \
  -bind=:7912 -http=:8091 \
  -replication=true
```

4. Start broker/controller 2:

```sh
./mbd \
  -broker-id=2 \
  -controller=raft \
  -raft-bind=127.0.0.1:9002 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
  -raft-dir=./data2/raft \
  -data-dir=./data2 \
  -bind=:8912 -http=:8092 \
  -replication=true
```

5. Create a topic with `replicationFactor=2` via HTTP or `mbctl`, produce and fetch records through the leader, and then inspect the cluster:

- use `/api/controller` to inspect `mode`, Raft `raftState`, `term`, `clusterID`, and metadata `version`;
- use `/api/cluster` to see per-partition leaders, replica sets, and ISR tracked by the controller;
- hit `/api/topics/<name>` (or the binary metadata API) to discover which broker owns each partition;
- expect followers to reply with `ErrNotLeader` (binary) or HTTP 409 plus `leaderBrokerID`; retry those requests against the reported leader.

On startup each broker waits for the Raft leader exposed by `/api/controller`; the leader applies `RegisterBroker` directly, and followers forward the same info to `POST /api/controller/brokers` so the leader can append the registration. You can watch `raftState`, `term`, `peers`, and `leader` in `/api/controller` to understand elections or diagnose `leader not elected`.

The `scripts/raft-cluster-demo.sh` script orchestrates this flow end-to-end, including a follower restart to demonstrate that replication catches up from `WALSink.NextOffset()` without duplicate writes.

#### Client expectations & restart behavior

- **Broker registration**: on startup each `mbd` instance waits for a Raft leader via `/api/controller`; if it is the leader it applies a `RegisterBroker` command directly through Raft, and if it is a follower it POSTs to the leader’s `POST /api/controller/brokers` endpoint so that the leader appends and replicates the registration in `ClusterMetadata`.
- **Leader-only traffic**: clients should write/read to the leader returned by metadata (`/api/topics` or `/api/cluster`). Followers reject produce/fetch with the standard leader hint, so drivers should retry against `leaderBrokerID`.
- **Durable controller state**: `NewRaftController` persists initial `ClusterMetadata` in `-raft-dir` and reuses it on restart so controller restarts continue from the same view without resetting leaders/replicas.
- **Broker bootstrap**: each broker opens partitions only where it is listed as a replica; the local `metadata.log` is just a cache, so the controller remains the source of truth.
- **Replication resilience**: the replication manager watches `ClusterMetadata`, starts/stops `PartitionReplicator`s, and reports ISR progress back to the controller. After a restart, followers use `WALSink.NextOffset()` to resume exactly where they left off.

#### Operating a Raft cluster

- **Bootstrap**: start the first broker with the full `-raft-peer` list, then confirm `/api/controller` reports a leader and stable `clusterID`.
- **Join**: launch additional brokers with the same peer list, and make sure they appear in `/api/controller` peers and `/api/cluster` brokers before sending traffic.
- **Rolling restart**: restart brokers one by one, watching `/api/controller` metadata `version` to see it keep increasing, and use `/api/cluster` to confirm ISR updates and leader elections.

## Docker Compose (Raft demo cluster)

`docker compose up --build` brings up two brokers (`broker1`, `broker2`) and the UI on a shared network. Each broker runs with `-controller=raft`, `-raft-bind=brokerN:9001`, the shared `-raft-peer` list, `-raft-dir=/data/raft`, `-replication=true`, and the usual `-data-dir`, `-bind`, `-mqtt`, `-http` flags. Inside the cluster each `mbd` waits for the Raft leader via `/api/controller`; the leader applies `RegisterBroker` through Raft, while followers POST to `http://<leader-host>:<http-port>/api/controller/brokers` so the leader can append the registration.

Check `http://localhost:8090/api/controller` or `http://localhost:8091/api/controller` for `mode`, `raftState`, `term`, `peers`, `leader`, `clusterID`, `version`, and `http://localhost:8090/api/cluster` (or `8091`) for the current `ClusterMetadata`.

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

Defaults: `-data-dir=/data -bind=:7912 -mqtt=:1883 -http=:8090`. Override flags via:

```sh
docker run wavemq:latest <flags>...
```

Access:

- Binary protocol: `localhost:7912` (use `mbctl` on host)
- MQTT: `localhost:1883`
- Observability: `http://localhost:8090/metrics` and `/healthz`

## Notes / Limits

- Single-node examples are the default in this README; multi-broker/Raft controller are available via flags (`-controller=raft`, `-raft-bind`, `-raft-peer`) and are documented above.
- Consumer group coordination is local; offsets persisted via offset WAL.
- MQTT support is minimal (QoS0/1, no retained/will/shared subs).
- Storage uses segmented WAL with sparse index and retention by size/age.
