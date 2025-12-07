# План доработки wave-mq

## 1. Single-node ядро (брокер, storage, протоколы) — сделано

- Segmented WAL + индекс + recovery (`internal/storage`).
- Broker API: topics/partitions, produce/fetch, consumer groups и offset‑WAL (`internal/broker`).
- Бинарный протокол + `mbctl` + MQTT + HTTP API/UI, метрики, pprof (`internal/netproto`, `internal/mqtt`, `internal/httpapi`, `internal/observability`, `cmd/mbd`, `cmd/mbctl`, `cmd/mbbench`).
- На этом уровне сейчас только поддержка и полировка.

## 2. Контроллер и долговечность метаданных кластера

### 2.1 Raft‑контроллер: реальная персистентность

- Использовать `raftDir` для дисковых `logStore`/`stableStore`/`snapshotStore` (через `hashicorp/raft`), вместо всегда in‑memory.
- Развести режимы:
  - dev/test: полностью in‑memory (как сейчас),
  - cluster: при указанном `-raft-dir` — устойчивое состояние кластера на диске.
- Тест: остановить и поднять кластер заново, убедиться, что `ClusterMetadata` (topics, partitions, leaders/replicas, ISR) восстановлен.

### 2.2 Единая семантика `WatchClusterMetadata` (стриминг, а не один снапшот)

- Обновить контракт `MetadataStore.WatchClusterMetadata`: канал должен выдавать **последовательность снапшотов** при росте `Version`, пока не отменён `ctx`.
- `SingleNodeController`:
  - хранить список подписчиков;
  - после каждого `AssignTopic`/`ReportReplicaProgress` отправлять обновлённый `ClusterMetadata` всем активным watcher’ам.
- `RaftController`:
  - после применения команды (в `Apply` или через наблюдатель за `fsm.meta.Version`) пушить свежий снапшот в каналы watcher’ов.

## 3. Брокер: контроллер как source of truth и материализация топиков

### 3.1 Старт брокера от контроллера (cluster‑mode)

- В `cmd/mbd` / `broker.NewBroker`:
  - после инициализации контроллера получить `ClusterMetadata`;
  - вместо чистой опоры на `metadata.log` пройти по `meta.Partitions` и:
    - для каждой партиции, где текущий брокер в `Replicas`, открыть/создать локальный WAL через `storage.Manager.OpenLog`;
    - при несоответствии локального `metadata.TopicState` и `ClusterMetadata` приводить локальное состояние к виду контроллера.
- В single‑node режиме сохранить оптимизацию: старт от `metadata.log`, но контроллер всё равно должен отражать то же состояние.

### 3.2 Создание топика через контроллер (cluster‑mode)

- Перестроить поток `HTTP /api/topics` → `CreateTopic`, чтобы:
  - в cluster‑mode сначала вызывать `ctrl.AssignTopic(name, cfg)` и получать распределение реплик;
  - затем на затронутых брокерах материализовать локальные партиции.
- `metadata.Store` на брокере использовать как кэш локальных событий; при расхождении с `ClusterMetadata` — игнорировать локальный лог и восстанавливать состояние по контроллеру.

### 3.3 Фолловеры: bootstrap только по контроллеру

- При приходе нового `PartitionAssignment` через `WatchClusterMetadata`, где брокер в `Replicas`:
  - при отсутствии локального лога создавать `storage.Log` для `(topic, partition)`;
  - инициализировать локальный `Partition` с ролью `RoleFollower`.

## 4. Репликация: привязка к стримящимся метаданным

### 4.1 Replication Manager поверх стриминга `ClusterMetadata`

- `internal/replication.Manager.Run` должен:
  - обрабатывать **последовательность** снапшотов из `WatchClusterMetadata`;
  - при каждом новом снапшоте вызывать `applyMetadata` и корректно останавливать/запускать репликаторы.

### 4.2 Устойчивый bootstrap репликатора

- Убедиться, что:
  - `WALSink.NextOffset` корректно даёт позицию после рестарта брокера;
  - `PartitionReplicator` всегда читает начальный offset через `OffsetProvider` и догоняет лидера.
- Тест: лидер + follower с RF=2, несколько перезапусков follower’а и контроллера без потери подтверждённых сообщений.

## 5. Операционный слой и «экспериментальный» кластерный режим

- Обновить/расширить `/api/controller` и `/api/cluster` (при необходимости) для удобного отображения роли брокера (leader/follower, ISR) и режима контроллера (`single`/`raft`, experimental).
- В `README.md` и учебных материалах явно описать:
  - что Raft‑состояние изначально может быть in‑memory,
  - что кластерный режим и репликация — **экспериментальны**, без строгих гарантий при сетевых разделениях и сложных отказах.
- Подготовить 1–2 сценарных теста/скрипта:
  - запуск 2–3 брокеров с Raft‑контроллером, создание топика с RF=2, проверка чтения/записи только к лидеру;
  - рестарт брокера и контроллера с проверкой восстановления `ClusterMetadata` и продолжения репликации.

