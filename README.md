��# wave-mq

Single-node log-based message broker in Go, designed to grow into a small but realistic Kafka-like cluster. It provides a custom binary protocol, a minimal MQTT 3.1.1/5.0 frontend (QoS0/1), consumer groups with broker-side offsets, and segmented WAL storage with sparse indexes and retention.

## Project Status

> ⚠️ Clustering, Raft, and follower replication are **experimental**. Expect breaking changes, resets during development, and behavior suitable only for demos/labs. Run with replication disabled (`-replication=false`) if you need a stable single-node broker.

- Core single-node broker (storage, binary protocol, MQTT, consumer groups, HTTP UI/API) - implemented and suitable for local experiments and demos.
- Topic metadata persistence (`metadata.log`) - implemented; topics and partitions survive broker restart and are recovered on startup. In clustered modes `metadata.log` should be viewed as a **local cache**; the controller’s view of the cluster is authoritative.
- Cluster metadata layer (controller + `/api/cluster`) - implemented for single-node and small multi-broker clusters. A Raft-based controller is available via `-controller=raft` (single-node or multi-peer) and is currently an **experimental** clustered mode; `-controller=single` keeps the simpler in-memory controller.
- Replication path (leader <-> follower) - binary client (`BinaryReplicator`) and partition replicator (`PartitionReplicator` + WAL sink + ISR reporting) are implemented and exercised with RF=2 scenarios. RF>1 clustering is **intended for lab and demo use**, not production; clients should write/read to leaders (followers return `ErrNotLeader` / HTTP 409 with `leaderBrokerID`), metadata exposes leader/replica layout for routing, and replication metrics (`wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`) track follower progress.
- Multi-node / Raft-backed controller quorum - available for local multi-broker clusters as an experimental feature. Basic failover and rolling restart scenarios have tests; further operational hardening, durable Raft state on disk, and stronger guarantees are ongoing work.

## Roadmap

### 1. Core (current state)

- Single-node broker with segmented WAL, sparse index, retention by size/age and crash-recovery.
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
  - for each follower partition, runs a `PartitionReplicator` pointing at the leaderB)s `BrokerInfo`
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
  - static multi-broker layouts, RaftController command application, ISR updates, brokerB)s awareness of B,itsB- partitions.

#### 3.2 End-to-End (E2E) tests

- Full-path scenarios using the real binaries (or docker-compose):
  - start broker+UI, create topics, produce/consume via CLI, HTTP and MQTT;
  - verify metrics, health checks and UI behavior end-to-end.
- For multi-broker:
  - scenarios with several brokers and a Raft controller cluster, including basic failover cases.

#### 3.3 Large E2E scenario fuzzing

- Scripted B,big testB- that runs many randomized scenarios to shake out edge cases:
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

## Observability

HTTP endpoints (default `:8090`):

- `/metrics` B$ Prometheus metrics.
- `/healthz` B$ readiness probe.
- `/debug/pprof/*` B$ pprof handlers.

Replication metrics:
- `wavemq_replication_lag_offsets` measures the follower lag per topic/partition/broker (leader HWM minus last applied).
- `wavemq_replication_applied_total` counts how many records each follower has applied while replicating.

Example:

```sh
curl http://localhost:8090/metrics
```

### Experimental multi-broker (Raft) quickstart

Run 2–3 brokers with a shared Raft controller quorum. All nodes participate in Raft by default; replication must be enabled explicitly.

1) Pick Raft addresses (example): `127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003`. Use two peers if you only want a 2-node demo.
2) Start broker/controller 1:

```sh
./mbd \
  -broker-id=1 \
  -controller=raft \
  -raft-bind=127.0.0.1:9001 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  -data-dir=./data1 \
  -bind=:7912 -http=:8091 \
  -replication=true
```

3) Start broker/controller 2 (adjust ports/paths):

```sh
./mbd \
  -broker-id=2 \
  -controller=raft \
  -raft-bind=127.0.0.1:9002 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  -data-dir=./data2 \
  -bind=:8912 -http=:8092 \
  -replication=true
```

4) (Optional) Start broker/controller 3:

```sh
./mbd \
  -broker-id=3 \
  -controller=raft \
  -raft-bind=127.0.0.1:9003 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 \
  -data-dir=./data3 \
  -bind=:9912 -http=:8093 \
  -replication=true
```

5) Create a topic with RF=2 or RF=3 (via HTTP or `mbctl`), then check:

- `/api/controller` for `mode`, Raft `raftState`/`term`, and peer list;
- `/api/cluster` for leader/replica/ISR assignments per partition;
- `/api/topics/<name>` to see which node is leader vs follower for each partition.

#### Operating a Raft cluster

- Bootstrap:
  - start the first broker with the full `-raft-peer` list;
  - ensure `/api/controller` reports a leader and a correct `clusterID`.
- Join:
  - start additional brokers with the same `-raft-peer` list;
  - verify they appear in `/api/controller` peers and `/api/cluster` brokers.
- Rolling restart:
  - restart brokers one by one, checking `/api/controller` after each restart to ensure a leader exists and metadata `version` continues to increase.

## Docker Compose (broker + UI)

There is a `docker-compose.yml` in the repo which brings up the broker and the UI (`wave-ui`):

```sh
docker compose up --build
```

Ports:

- broker: `7912` (binary), `1883` (MQTT), `8090` (HTTP/metrics)
- UI: `8080` (nginx with Vite-built static assets)

Broker data is persisted in the `wave_data` volume. The UI can be built with `VITE_USE_MOCKS=false` to talk to the real HTTP API at `http://broker:8090`.

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

