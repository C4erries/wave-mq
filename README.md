# wave-mq

Single-node log-based message broker in Go, ready to grow into a cluster. Provides a custom binary protocol, basic MQTT 3.1.1/5.0 frontend (QoS0/1), consumer groups with broker-side offsets, and segmented WAL storage with sparse indexes and retention.

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

## Notes / Limits
- Single node; replication/cluster controller not implemented yet.
- Consumer group coordination is local; offsets persisted via offset WAL.
- MQTT support is minimal (QoS0/1, no retained/will/shared subs).
- Storage uses segmented WAL with sparse index and retention by size/age.
