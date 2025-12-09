# wave-mq (русский обзор)

wave-mq — это log-based брокер сообщений на Go. Основной режим — single-node, но есть экспериментальная поддержка кластерного режима с контроллером и репликацией.

Подробности архитектуры описаны в `docs/architecture.md`.

## Статус проекта и кластера

> Важно: режимы **Clustering / Raft / репликация** считаются **экспериментальными** и предназначены для демо и лабораторных сценариев, а не для продакшена. Для стабильной работы используйте single-node конфигурацию (`-replication=false`, `-controller=single`).

- Single-node брокер (хранилище, бинарный протокол, MQTT, consumer groups, HTTP UI/API) реализован и подходит для локальных экспериментов.
- Метаданные топиков (`metadata.log`) хранятся на диске; топики/партиции восстанавливаются после рестарта брокера. В кластерном режиме локальный `metadata.log` — это **кэш**, а source of truth — контроллер.
- Контроллер кластера (`internal/controller`):
  - `SingleNodeController` — простой in‑memory контроллер для single-node и упрощённых сценариев.
  - `RaftController` — контроллер на базе Hashicorp Raft, который хранит `ClusterMetadata` (список брокеров, лидеры/реплики/ISR) в Raft‑журнале; снапшоты и восстановление также реализованы.
- Репликация (`internal/replication`):
  - `BinaryReplicator` использует существующий Fetch по бинарному протоколу.
  - `PartitionReplicator` + WAL‑sink + reporting‑sink реплицируют данные с лидера на follower‑партиции и отправляют прогресс в контроллер (`ReportReplicaProgress`), поддерживая ISR.
  - RF>1 (репликационный фактор >1) рассматривается как лабораторный режим: писать/читать нужно в лидера; followers возвращают `ErrNotLeader` / HTTP 409 с `leaderBrokerID`. Метрики (`wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`) показывают лаг и прогресс репликации.

## Основные компоненты

- `internal/storage` — сегментированный WAL, индексы, ретеншн и recovery.
- `internal/broker` — топики, партиции, локальные реплики, consumer groups, смещения.
- `internal/netproto` + `cmd/mbctl` — бинарный протокол и CLI (`create-topic`, `produce`, `fetch`).
- `internal/mqtt` — минимальный MQTT 3.1.1/5.0 frontend (QoS0/1) поверх брокера.
- `internal/httpapi` + `wave-ui` — HTTP API и UI для администрирования / мониторинга.
- `internal/controller` — SingleNodeController и RaftController, которые управляют `ClusterMetadata` (leaders/replicas/ISR).
- `internal/replication` — клиент репликации, PartitionReplicator, WAL‑sink и reporting‑sink.
- `internal/observability` — Prometheus‑метрики (`/metrics`), `/healthz`, pprof.

Подробности по форматам данных и API — в `docs/architecture.md`.

## Экспериментальный Raft‑кластер (2–3 брокера)

Режим `-controller=raft` и репликация с `-replication=true` позволяют поднять небольшой кластер из нескольких брокеров. Этот режим пока не предназначен для продакшена, но подходит для локальных сценариев RF=2.

### Требования

- Каждый брокер имеет уникальный `-broker-id`.
- Все брокеры используют один и тот же список Raft‑пиров:

```sh
-controller=raft \
-raft-peer=broker1:9001,broker2:9001
```

- Для каждого брокера задаётся свой `-raft-bind` (адрес Raft‑транспорта), попадающий в общий список `-raft-peer`.
- HTTP‑порты обычно одинаковые (например, `-http=:8090`) и доступны по тем же host‑именам, что и Raft‑пиры (в docker‑compose это имена сервисов `broker1`, `broker2`).

### Пример запуска двух брокеров с RF=2

Брокер 1:

```sh
./mbd \
  -broker-id=1 \
  -controller=raft \
  -raft-bind=broker1:9001 \
  -raft-peer=broker1:9001,broker2:9001 \
  -raft-dir=./data1/raft \
  -data-dir=./data1 \
  -bind=:7912 \
  -http=:8090 \
  -replication=true
```

Брокер 2:

```sh
./mbd \
  -broker-id=2 \
  -controller=raft \
  -raft-bind=broker2:9001 \
  -raft-peer=broker1:9001,broker2:9001 \
  -raft-dir=./data2/raft \
  -data-dir=./data2 \
  -bind=:8912 \
  -http=:8090 \
  -replication=true
```

При старте:

- оба брокера поднимают свой RaftController и подключаются к кластеру;
- каждый `mbd` регистрирует себя через `RegisterBroker` (команда идёт через Raft: лидер применяет запись и реплицирует обновлённый `ClusterMetadata`);
- брокер и менеджер репликации подписываются на `WatchClusterMetadata` и поднимают только те партиции/реплики, которые закреплены за данным брокером.

### Диагностика кластера

- `GET /api/controller`:
  - `mode` — режим контроллера (`single` или `raft`);
  - `raftState` — состояние Raft (`leader`, `follower`, `candidate`);
  - `term` — текущий термин;
  - `peers` — список Raft‑пиров;
  - `leader` — адрес текущего лидера;
  - `clusterID`, `version` — идентификатор кластера и версия `ClusterMetadata`.
- `GET /api/cluster` — полный `ClusterMetadata` (список брокеров, топики, партиции, лидеры, реплики, ISR).

Если при запуске видите ошибку `leader not elected` или проблемы с регистрацией брокера:

- убедитесь, что `-raft-peer` на всех узлах совпадает и содержит их `-raft-bind`;
- запросите `/api/controller` на каждом брокере и дождитесь, пока один из них покажет `raftState=leader` и ненулевой `term`;
- проверьте, что с других контейнеров/хостов можно сходить на `http://<broker-name>:<http-port>/api/controller`.

## Single-node запуск

Для одиночного брокера без кластера и репликации достаточно:

```sh
go build ./cmd/mbd

./mbd \
  -data-dir=./data \
  -bind=:7912 \
  -mqtt=:1883 \
  -http=:8090 \
  -controller=single \
  -replication=false
```

Создание топика и produce/fetch через CLI:

```sh
go build ./cmd/mbctl

./mbctl create-topic -topic test -partitions 1
./mbctl produce -topic test -partition 0 -value "hello"
./mbctl fetch -topic test -partition 0 -offset 0
```

Остальные детали (MQTT, HTTP API/UI, observability) совпадают с описанием в основном `README.md`.

