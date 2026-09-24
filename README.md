# wave-mq

`wave-mq` is the broker implementation in this repository. For preview work, treat the single-node path as the supported path and the cluster/Raft path as experimental.

## Preview status

- Single-node broker is ready for local demos and preview validation.
- Retention deletion is off by default in the single-node path.
- Binary protocol, MQTT 3.1.1/5.0, HTTP admin APIs, consumer groups, and the UI are available.
- Cluster/Raft and follower replication are still experimental and should be used only for lab or demo scenarios.

## Quick start

Build the broker tools:

```powershell
go build ./cmd/mbd
go build ./cmd/mbctl
```

Run a single-node broker:

```powershell
./mbd -data-dir=./data -bind=:7912 -mqtt=:1883 -http=:8090 -controller=single -replication=false
```

Or bring up the preview stack with Docker Compose from the repo root:

```powershell
docker compose -f .\docker-compose.single.yml up --build
```

That preview stack exposes:

- UI: `http://localhost:8080`
- HTTP API: `http://localhost:8090`
- MQTT: `localhost:1883`
- Binary protocol: `localhost:7912`

## Cluster mode

`docker compose -f .\docker-compose.multi.yml up --build` starts the experimental multi-broker preview. It is useful for manual Raft and replication checks, but it is not the stable preview path yet.

## See also

- Detailed architecture: `docs/architecture.md`
- Russian technical overview: [README.ru.md](README.ru.md)
- Root preview entrypoint: [../README.md](../README.md)
- Root plan: [../plan.md](../plan.md)
