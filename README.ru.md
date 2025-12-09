# wave-mq

wave-mq — это учебный лог-ориентированный (log-based) брокер сообщений на Go, задуманный как маленькое, но реалистичное ядро Kafka‑подобного кластера. Он реализует кастомный бинарный протокол, минимальный MQTT 3.1.1/5.0 фронтенд (QoS0/1), consumer groups с хранением offset’ов на брокере и сегментированное WAL‑хранилище с индексами и retention‑политиками.

Подробный архитектурный обзор (на русском) см. в `docs/architecture.md`.

## Текущее состояние проекта

> ⚠️ Clustering, Raft и репликация фолловеров — **экспериментальные** функции. Они предназначены для демо и лабораторных стендов, а не для production. Для стабильной single-node работы включайте брокер без репликации (`-replication=false`).

- Single-node брокер (storage, бинарный протокол, MQTT, consumer groups, HTTP UI/API) реализован и подходит для локальных экспериментов и демонстраций.
- Персистентность метаданных топиков (`metadata.log`) реализована; топики и партиции переживают рестарт брокера и восстанавливаются при старте. В кластерном режиме `metadata.log` следует считать **локальным кэшем**, а истинное состояние хранится в контроллере.
- Слой метаданных кластера (контроллер + `/api/cluster`) реализован для single-node и небольших multi-broker кластеров. Raft‑контроллер (`-controller=raft`) поддерживает single-node и multi-peer режимы, но остаётся **экспериментальным**; режим `-controller=single` использует упрощённый in-memory контроллер.
- Репликационный путь (leader <-> follower) реализован: бинарный клиент (`BinaryReplicator`), `PartitionReplicator` + WAL‑sink + отчёт в ISR‑слой контроллера. RF>1 рассматривается как режим для лабораторного использования: записывать/читать следует в лидеров (followers возвращают `ErrNotLeader` / HTTP 409 с `leaderBrokerID`), а метрики (`wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`) показывают лаг и прогресс репликации.
- Multi-node / Raft‑контроллер доступен для локальных multi-broker кластеров, но без production‑гарантий; реализованы базовые сценарии failover и rolling‑restart, дальнейший hardening и tooling остаются задачами будущей работы.

## Обзор возможностей

- Сегментированный WAL с индексами и retention‑политикой по размеру/времени.
- Бинарный протокол (`internal/netproto`) и CLI‑клиент (`cmd/mbctl`) для create-topic / produce / fetch и управления метаданными.
- Минимальный MQTT‑фронтенд (`internal/mqtt`): CONNECT/SUBSCRIBE/PUBLISH QoS0/1, маппинг MQTT‑топиков на внутренние топики и партиции.
- HTTP‑админка (`internal/httpapi`) + UI (`wave-ui`): обзор топиков, партиций, потребителей, кластера и контроллера.
- Контроллер кластера (`internal/controller`): SingleNodeController и RaftController, которые хранят и реплицируют `ClusterMetadata` (leaders/replicas/ISR).
- Репликация (`internal/replication`): BinaryReplicator, PartitionReplicator, WAL‑sink, Reporting‑sink и менеджер репликации, который слушает `WatchClusterMetadata` и запускает/останавливает репликаторы.
- Набор метрик Prometheus (`internal/observability`) + `/metrics`, `/healthz`, `pprof`‑эндпоинты.

Для более подробного описания архитектуры см. `docs/architecture.md`.

