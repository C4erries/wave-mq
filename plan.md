# План доработки wave-mq (итоговое состояние)

## 1. Single-node ядро (broker, storage, протоколы) — сделано

- Segmented WAL + индекс + recovery (`internal/storage`).
- Broker API: topics/partitions, produce/fetch, consumer groups и offset‑WAL (`internal/broker`).
- Бинарный протокол + `mbctl` + MQTT + HTTP API/UI, метрики, pprof (`internal/netproto`, `internal/mqtt`, `internal/httpapi`, `internal/observability`, `cmd/mbd`, `cmd/mbctl`, `cmd/mbbench`).

## 2. Контроллер и долговечность метаданных кластера — сделано

### 2.1 Raft‑контроллер: персистентность

- Используется `raftDir` для дисковых `LogStore`/`StableStore`/`SnapshotStore` (через `hashicorp/raft` и `raft-boltdb`) вместо чисто in‑memory (`internal/controller/raft_controller.go`).
- In‑memory режим по‑прежнему используется для dev/test при пустом `raftDir`.
- Начальная метадата кластера (`ClusterMetadata`) сохраняется в `StableStore` через `persistInitialMetadata` и при рестартах контроллера поднимается через `loadInitialMetadata`, так что все запуски Raft‑контроллера стартуют с одного и того же источника правды.
- Тесты `TestRaftControllerPersistsInitialMetadata` и `TestRaftControllerRestoresInitialMetadataOnRestart` (`internal/controller/raft_controller_test.go`) проверяют, что initial‑метадата действительно сохраняется и используется при рестарте.

### 2.2 Стримящий `WatchClusterMetadata`

- `MetadataStore.WatchClusterMetadata` реализован как поток снапшотов при росте `Version`.
- `SingleNodeController`:
  - хранит подписчиков через `metadataPublisher`;
  - публикует обновлённый `ClusterMetadata` в `AssignTopic` и `ReportReplicaProgress`.
- `RaftController`:
  - `raftMetadataFSM.Apply` и `Restore` вызывают `pub.publish` при изменении метаданных;
  - `WatchClusterMetadata` использует `pub.watch(ctx, sinceVersion, meta)`.

## 3. Брокер: контроллер как source of truth и материализация топиков — сделано

### 3.1 Старт брокера от контроллера (cluster‑mode)

- В `cmd/mbd/main.go` брокер после регистрации в контроллере получает `ClusterMetadata` и передаёт снапшот в `broker.NewBroker`.
- `NewBroker` (`internal/broker/broker.go`) использует `reconcileClusterMetadata` и `bootstrapTopicsFromMetadata`, чтобы:
  - поднять локальные `Topic`/`Partition` для всех `PartitionAssignment`, где текущий `BrokerID` входит в `Replicas`;
  - привести локальные `metadata.TopicState` к виду контроллера, рассматривая `ClusterMetadata` как источник правды, а `metadata.log` — как кэш.

### 3.2 Создание топика через контроллер (cluster‑mode)

- `Broker.CreateTopic` в cluster‑mode делегирует в `createTopicFromClusterAssignments`, который:
  - проверяет существование топика через `topicExistsInCluster`,
  - вызывает `ctrl.AssignTopic`,
  - материализует только те партиции, где текущий брокер присутствует в `Replicas`, через `CreateTopicWithAssignments`.
- HTTP `/api/topics` (`internal/httpapi/api.go`) вызывает `Broker.CreateTopic`, так что все HTTP‑создания топиков проходят через контроллер‑driven путь, а локальный `metadata.log` используется как кэш.

### 3.3 Фолловеры: bootstrap только по контроллеру

- Брокер хранит последнюю версию метаданных (`metaVersion`) и имеет watcher `StartClusterMetadataWatcher`, который слушает `WatchClusterMetadata` и на каждый снапшот:
  - через `topicsFromClusterMetadata` вычисляет актуальный набор топиков/партиций для данного `BrokerID`,
  - вызывает `bootstrapTopicsFromMetadata`, чтобы:
    - создать недостающие локальные `Topic`/`Partition`,
    - открыть `storage.Log` для новых партиций (включая фолловеров),
  - обновляет `PartitionMetadata` (роль leader/follower, `Leader`, `Replicas`, `ISR`) через `updatePartitionMetadata`.
- В результате брокер динамически подхватывает новые назначения (в том числе follower‑реплики) по данным контроллера, без рестарта процесса.

## 4. Репликация: привязка к стримящимся метаданным — сделано

### 4.1 Replication Manager поверх `ClusterMetadata`

- `internal/replication.Manager.Run`:
  - слушает последовательность снапшотов из `WatchClusterMetadata(ctx, 0)`,
  - при каждом новом снапшоте вызывает `applyMetadata`, который:
    - вычисляет желаемый набор follower‑реплик для текущего брокера,
    - останавливает устаревшие `PartitionReplicator`’ы и запускает новые с корректными `PartitionAssignment`,
  - очищает `running` при завершении репликаторов.

### 4.2 Устойчивый bootstrap репликатора

- `WALSink.NextOffset` возвращает `HighWatermark() + 1`, давая корректную стартовую позицию для догонки после рестартов.
- `PartitionReplicator`:
  - при `nextOffset == 0` запрашивает начальный offset через `OffsetProvider.NextOffset`,
  - после каждого успешного `ApplyBatch` сдвигает `nextOffset` на `lastApplied + 1`.
- Тесты в `internal/replication/partition_replicator_test.go`:
  - `TestPartitionReplicatorResumesFromNextOffset` — моделирует остановку и рестарт follower’а: новый запуск читает `NextOffset` и реплицирует только новые записи, репортя корректный watermark;
  - `TestPartitionReplicatorDoesNotDuplicateAfterCatchUp` — моделирует рестарт полностью догнавшего follower’а и проверяет отсутствие дубликатов/лишних записей.

## 5. Операционный слой и документация экспериментального кластерного режима — сделано

- В `README.md`:
  - явно отмечено, что Raft‑кластер (`-controller=raft`) и RF>1 replication — **экспериментальные** возможности для демо/лабораторий, а не production;
  - описан типичный сценарий запуска 2‑брокерного кластера с RF=2 (конкретные команды `mbd`), ожидания по leader‑only операциям для клиентов (followers возвращают `ErrNotLeader` / HTTP 409 с `leaderBrokerID`);
  - кратко описано поведение при рестартах/rolling‑upgrade: сохранение `ClusterMetadata` в `-raft-dir`, контроллер‑driven bootstrap брокеров, догонка replication manager’ом, ошибки follower’ов при обращении не к лидеру;
  - выделены полезные диагностические HTTP‑эндпоинты `/api/controller` и `/api/cluster`.
- В `scripts/raft-cluster-demo.sh`:
  - автоматизирован сценарий: сборка `mbd`, запуск двух Raft‑брокеров с RF=2, создание топика, produce/fetch через лидера, проверка, что follower возвращает HTTP 409/`leaderBrokerID`, рестарт follower’а и ожидание, пока он догонит лидера (валидируя работу replication manager’а и `WALSink.NextOffset`).

---

С точки зрения плана, основные блоки (2, 3, 4, 5) считаются реализованными и покрыты тестами/демо‑сценариями. Дальнейшая работа может быть направлена на углублённые e2e‑тесты, стресс‑тестирование и полировку UX, но для курсовой и базовой кластерной демонстрации текущего объёма достаточно.

