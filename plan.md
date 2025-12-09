# План доработки wave-mq

## 1. Single-node ядро (broker, storage, протоколы) — сделано

- Segmented WAL + индекс + recovery (`internal/storage`).
- Broker API: topics/partitions, produce/fetch, consumer groups и offset‑WAL (`internal/broker`).
- Бинарный протокол + `mbctl` + MQTT + HTTP API/UI, метрики, pprof (`internal/netproto`, `internal/mqtt`, `internal/httpapi`, `internal/observability`, `cmd/mbd`, `cmd/mbctl`, `cmd/mbbench`).
- На этом уровне сейчас только поддержка и полировка.

## 2. Контроллер и долговечность метаданных кластера

### 2.1 Raft‑контроллер: персистентность — **частично готово**

- Используется `raftDir` для дисковых `LogStore`/`StableStore`/`SnapshotStore` (через `hashicorp/raft` и `raft-boltdb`) вместо чисто in‑memory — сделано в `internal/controller/raft_controller.go`.
- In‑memory режим по‑прежнему используется для dev/test при пустом `raftDir` — сделано.
- TODO: дожать использование `persistInitialMetadata` / `loadInitialMetadata`, чтобы initial `ClusterMetadata` гарантированно восстанавливался при bootstrap без потери информации.

### 2.2 Стримящий `WatchClusterMetadata` — **сделано**

- Контракт `MetadataStore.WatchClusterMetadata` реализован как поток снапшотов при росте `Version`.
- `SingleNodeController`:
  - хранит подписчиков через `metadataPublisher`;
  - публикует обновлённый `ClusterMetadata` в `AssignTopic` и `ReportReplicaProgress`.
- `RaftController`:
  - `raftMetadataFSM.Apply` и `Restore` вызывают `pub.publish` при изменении метаданных;
  - `WatchClusterMetadata` использует `pub.watch(ctx, sinceVersion, meta)`.

## 3. Брокер: контроллер как source of truth и материализация топиков — **TODO**

### 3.1 Старт брокера от контроллера (cluster‑mode)

- В `cmd/mbd` / `broker.NewBroker`:
  - после инициализации контроллера получать `ClusterMetadata`;
  - на его основе создавать локальные `Topic`/`Partition` для всех `PartitionAssignment`, где `BrokerID` текущего брокера входит в `Replicas`, даже если `metadata.log` пустой;
  - при расхождении локального `metadata.TopicState` и `ClusterMetadata` приводить локальное состояние к виду контроллера (контроллер — источник правды, `metadata.log` — кэш).
- В single‑node режиме можно по‑прежнему начинать с `metadata.log`, но контроллер всё равно должен отражать то же состояние.

### 3.2 Создание топика через контроллер (cluster‑mode)

- Перестроить поток `HTTP /api/topics` → `CreateTopic` так, чтобы в cluster‑mode:
  - сначала вызывать `ctrl.AssignTopic(name, cfg)` и получать распределение реплик;
  - затем материализовывать локальные партиции на текущем брокере (минимум — для лидера; по возможности — и для фолловеров).
- `metadata.Store` на брокере использовать как кэш локальных событий; при расхождении с `ClusterMetadata` — игнорировать локальный лог и восстанавливать состояние по контроллеру.

### 3.3 Фолловеры: bootstrap только по контроллеру

- При получении нового `PartitionAssignment` через `WatchClusterMetadata`, где брокер входит в `Replicas`:
  - при отсутствии локального лога создавать `storage.Log` для `(topic, partition)`;
  - инициализировать локальный `Partition` в `Broker` с ролью `RoleFollower`.

## 4. Репликация: привязка к стримящимся метаданным

### 4.1 Replication Manager поверх стриминга `ClusterMetadata` — **сделано**

- `internal/replication.Manager.Run`:
  - слушает последовательность снапшотов из `WatchClusterMetadata(ctx, 0)`;
  - при каждом новом снапшоте вызывает `applyMetadata`, останавливая/запуская репликаторы при изменении `PartitionAssignment`;
  - чистит `running` при завершении репликатора.

### 4.2 Устойчивый bootstrap репликатора — **TODO**

- Подтверждено тестами, что:
  - `WALSink.NextOffset` корректно даёт стартовую позицию после рестарта;
  - `PartitionReplicator` берёт начальный offset через `OffsetProvider` и догоняет лидера без пропусков/дубликатов подтверждённых сообщений.
- В `internal/replication/partition_replicator_test.go` добавлены:
  - `TestPartitionReplicatorResumesFromNextOffset` — проверяет остановку, рестарт и продолжение репликации только новых записей;
  - `TestPartitionReplicatorDoesNotDuplicateAfterCatchUp` — покрывает сценарий «follower уже догнал лидера и рестартует» и гарантирует отсутствие дубликатов.

## 5. Операционный слой и «экспериментальный» кластерный режим — **частично/TODO**

- Обновить/расширить `/api/controller` и `/api/cluster` (при необходимости) для удобного отображения ролей брокеров (leader/follower, ISR) и режима контроллера (`single`/`raft`, experimental).
- В `README.md`:
  - явно описать, что Raft‑режим с replication — экспериментальный (для демо/лабораторий, не прод);
  - показать типичный сценарий запуска 2–3 брокеров с Raft‑контроллером и включённой репликацией;
  - описать сценарий восстановления после рестартов кластера.
- Подготовить сценарные тесты/скрипты:
  - запуск 2–3 брокеров с RF=2, проверка записи/чтения только на лидера и корректных ошибок на follower’ах;
  - рестарт брокеров и контроллера с проверкой, что `ClusterMetadata` восстанавливается, репликация продолжает работу, а ISR остаётся ненулевым.
