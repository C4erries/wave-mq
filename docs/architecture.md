# wave-mq: архитектурный обзор

Этот документ описывает архитектуру проекта `wave-mq` и предназначен как текстовая основа для курсовой работы по теме разработки распределённой инфраструктуры потоковой обработки и анализа данных.

## 1. Цели и контекст

### 1.1 Что такое wave-mq

`wave-mq` — это лог‑ориентированный брокер сообщений (commit‑log broker) на Go. Он сочетает в себе:

- **Сегментированное WAL‑хранилище** с индексами и recovery‑логикой;
- **Consumer groups** с хранением offset’ов на брокере;
- **Кастомный бинарный протокол** + CLI‑клиент;
- **Минимальный MQTT 3.1.1/5.0 фронтенд** (QoS0/1);
- **HTTP‑админку и метрики**, включая `/metrics`, `/healthz` и `pprof`;
- **Экспериментальный кластерный режим**: контроллер кластера на Raft, распределённые метаданные и репликация follower‑партиций.

Основной сценарий использования — построение учебного стримингового контура: несколько продюсеров и консюмеров обмениваются данными через брокер, поверх которого можно строить пайплайны потоковой обработки и аналитики.

### 1.2 Требования и ограничения

Ключевые требования (см. `Problem.md` и `Agent.md`):

- **Single-node MVP** с архитектурой, готовой к кластеризации.
- **Долговечное хранение** (append‑only WAL + индексы, crash‑recovery без потери подтверждённых сообщений).
- **Pull‑based потребление** по кастомному бинарному протоколу.
- **Consumer groups** с хранением offset’ов на брокере (at‑least‑once).
- **MQTT‑фронтенд** для интеграции с существующими клиентами.
- **Экспериментальный кластерный режим**:
  - Raft‑контроллер с `ClusterMetadata`,
  - распределённые топики/партиции/реплики,
  - репликация follower‑партиций.

Вне рамок проекта: production‑grade отказоустойчивость, сложные сценарии сетевых разделений, exactly‑once и полнофункциональный Kafka‑протокол. Кластерный режим и репликация — **экспериментальны** и предназначены для демо/лабораторных стендов.

## 2. Модель данных

### 2.1 Топики, партиции и записи

Базовые сущности описаны в `pkg/api/api.go`:

- **Topic** — именованный поток сообщений.
- **Partition** — строго упорядоченный лог внутри топика, с монотонно растущими offset’ами.
- **Record**:
  - `Offset` — логическая позиция внутри партиции (`api.Offset int64`);
  - `Timestamp` — время записи;
  - `Key`, `Value` — произвольные байтовые массивы;
  - `Headers` — список `Header{Key string, Value []byte}`;
  - `CRC32C` — контрольная сумма для проверки целостности (на уровне storage).

### 2.2 Кластерная модель

Всё проектируется с учётом будущего кластера (даже в single‑node режиме):

- `PartitionRole`:
  - `RoleLeader` — лидер, принимающий запись;
  - `RoleFollower` — фолловер‑реплика (в MVP — для экспериментов с RF>1).
- `PartitionReplica`:
  - `Topic`, `Partition` — идентификаторы партиции;
  - `BrokerID` — брокер, который хранит реплику;
  - `Role` — лидер/фолловер;
  - `LeaderEpoch` — версия лидерства (зарезервировано под будущие сценарии).
- `PartitionAssignment`:
  - `Replicas []int` — список брокеров‑реплик;
  - `Leader int` — брокер‑лидер;
  - `ISR []int` — in‑sync‑реплики;
  - `LeaderEpoch` — эпоха.
- `ClusterMetadata`:
  - список брокеров (`BrokerInfo`),
  - список `PartitionAssignment` по всем топикам.

Контроллер (`internal/controller`) управляет `ClusterMetadata` и является **единственным источником правды** о том, какие партиции и с какими ролями принадлежат конкретному брокеру. Локальный `metadata.log` у брокера — лишь кэш.

## 3. Хранилище: WAL и индексы (`internal/storage`)

### 3.1 Формат и сегментация

Каждая партиция хранится как набор сегментов:

- каталог: `<DataDir>/<topic>/<partition>/`;
- сегменты: `<baseOffset>.log` (лог) + `<baseOffset>.idx` (индекс).

Формат записи в `.log`:

- `uint32 recordSize` — размер записи (без включения самого поля длины);
- `uint32 crc32c` — CRC по остальной части записи;
- `int64 offset`;
- `int64 timestampUnixNano`;
- `int32 keyLen`, `int32 valueLen` (−1 = `nil`);
- `int32 headersCount` + пары key/value для заголовков;
- `key` / `value` байты.

Индексы (`.idx`) хранят разрежённое сопоставление `relativeOffset -> filePosition` и используются для бинарного поиска при чтении.

### 3.2 Crash‑recovery и retention

`storage.Manager`:

- управляет сегментами и индексами;
- при старте вызывает `Recover`, который сканирует все `.log`, проверяет длины, CRC и offset’ы; при обнаружении повреждённого хвоста сегмент обрезается до последней корректной записи;
- поддерживает ротацию сегментов по размеру/времени и глобальную retention‑политику по размеру (`MaxLogBytes`) и возрасту (`SegmentMaxAge`).

Таким образом обеспечивается инвариант: после рестарта не теряются подтверждённые записи, а лог остаётся консистентным.

## 4. Ядро брокера (`internal/broker`)

### 4.1 Ответственность брокера

`Broker` отвечает за:

- размещение и управление локальными топиками/партициями;
- API публикации и чтения (`Produce`, `Fetch`, `ListOffsets`);
- consumer groups и хранение offset’ов;
- интеграцию с контроллером (`MetadataStore`) и соблюдение лидерства в кластерном режиме.

Внутренние структуры:

- `Topic` — набор `Partition`;
- `Partition` — держит `PartitionMetadata` и `storage.Log`;
- `ConsumerGroup` — информация о группах и их offset’ах.

### 4.2 Bootstrap и контроллер как source of truth

При старте:

1. Брокер поднимает `storage.Manager` и локальный `metadata.Store` (`metadata.log`).
2. В `cmd/mbd/main.go` создаётся контроллер (`SingleNodeController` или `RaftController`), брокер регистрируется в нём (`RegisterBroker`), затем запрашивается `ClusterMetadata`.
3. `broker.NewBroker` принимает:
   - `recoveredTopics` из `metadata.log`,
   - `ClusterMetadata` из контроллера,
   - вызывает `reconcileClusterMetadata`:
     - фильтрует только те партиции, где данный `BrokerID` входит в `Replicas`;
     - строит `TopicState` и таблицу `assignments` по результатам контроллера;
   - выполняет `bootstrapTopicsFromMetadata`:
     - создаёт/обновляет `Topic` и `Partition` в `b.topics`,
     - открывает `storage.Log` для соответствующих партиций.

Локальный `metadata.log` используется как кэш структуры топиков, но при конфликте приоритет у `ClusterMetadata` контроллера.

### 4.3 Создание топиков

`Broker.CreateTopic`:

- В single-node режиме:
  - создаёт `TopicState` локально;
  - пишет событие `CreateTopicEvent` в `metadata.log`;
  - материализует партиции через `loadTopicLocked`.
- В кластерном режиме (есть `cluster`):
  - использует `createTopicFromClusterAssignments`:
    - проверяет отсутствие топика в `ClusterMetadata` (`topicExistsInCluster`);
    - вызывает `ctrl.AssignTopic` (контроллер распределяет реплики);
    - вычисляет назначения для этого брокера (`assignmentsForBroker`);
    - вызывает `CreateTopicWithAssignments`, которая создаёт локальный `TopicState` только для партиций, где брокер находится в `Replicas`.

Таким образом, контроллер остаётся единственным источником раскладки топика, а брокер лишь материализует локальные реплики.

### 4.4 Динамическое обновление по ClusterMetadata

Брокер подписывается на поток `ClusterMetadata`:

- `StartClusterMetadataWatcher` вызывает `cluster.WatchClusterMetadata` с последней обработанной версией и запускает фоновой цикл;
- при каждом новом снапшоте вызывается `handleClusterMetadataUpdate`:
  - через `topicsFromClusterMetadata` строятся `TopicState` и `assignments` для данного `BrokerID`;
  - `bootstrapTopicsFromMetadata` добавляет новые партиции (в том числе follower‑реплики);
  - `updatePartitionMetadata` обновляет `PartitionMetadata` (роль, лидер, список реплик, ISR);
  - `metaVersion` обновляется, чтобы избежать повторной обработки старых версий.

Это позволяет брокеру подхватывать новые назначения и смену лидера без рестарта процесса.

### 4.5 Consumer groups и offset’ы

Файл `internal/broker/offset_store.go` реализует WAL для offset’ов:

- формат: длина + CRC32C + `(group, topic, partition, offset)`;
- recovery считывает все записи, проверяет CRC и строит в памяти карту `group -> topic -> partition -> offset`;
- `AppendCommit` записывает новую запись и синхронно fsync’ит файл;
- `Compact` переписывает лог с единственной записью на комбинацию `group/topic/partition`.

Брокер хранит offset’ы в памяти и на диске, обеспечивая:

- отсутствие регрессии offset’ов (новый offset не может быть меньше текущего);
- безопасность при рестарте (offset’ы восстанавливаются из WAL).

## 5. Контроллер кластера (`internal/controller`)

### 5.1 Общие интерфейсы

Интерфейс `MetadataStore`:

- `GetClusterMetadata` — получить текущий снапшот `ClusterMetadata`;
- `WatchClusterMetadata` — подписка на поток снапшотов при изменении `Version`;
- `RegisterBroker` — регистрация брокера в кластере;
- `AssignTopic` — распределение реплик для нового топика;
- `ReportReplicaProgress` — отчёты follower‑реплик (обновление ISR).

Реализации:

- `SingleNodeController` — упрощённый контроллер для single-node/статических кластеров;
- `RaftController` — распределённый контроллер на базе `hashicorp/raft`.

### 5.2 SingleNodeController

`NewSingleNodeController`:

- принимает `BrokerConfig` и восстановленные `TopicState` из `metadata.log`;
- строит `ClusterMetadata` с одним брокером и лидерскими партициями;
- предоставляет `GetClusterMetadata` и `WatchClusterMetadata` через `metadataPublisher`.

`AssignTopic`:

- распределяет партиции по брокерам (в простом случае — один брокер);
- увеличивает `Version` и публикует новый снапшот.

`ReportReplicaProgress`:

- обновляет ISR для конкретной партиции (добавляет/удаляет follower‑реплики в зависимости от их догнанности до leader HighWatermark);
- повышает `Version` и публикует обновлённый `ClusterMetadata`.

### 5.3 RaftController

`RaftController` реплицирует `ClusterMetadata` с помощью Raft:

- хранит состояние в `raftMetadataFSM.meta` (`api.ClusterMetadata`);
- команды (`raftCommand`) сериализуются в JSON и попадают в Raft‑лог;
- `Apply` в `raftMetadataFSM` применяет команды:
  - `cmdAssignTopic` — распределяет партиции;
  - `cmdRegisterBroker` — добавляет/обновляет брокера;
  - `cmdReportReplicaProgress` — обновляет ISR;
  - после применения обновляет `Version` и публикует снапшот через `metadataPublisher`.

Персистентность:

- `buildStores` создаёт дисковые `LogStore`/`StableStore`/`SnapshotStore` (BoltDB + файловые снапшоты) в `raftDir`;
- `persistInitialMetadata` сохраняет initial `ClusterMetadata` в `StableStore`;
- `loadInitialMetadata` при рестарте извлекает initial‑метадату и подаёт её в FSM, так что контроллер стартует с консистентного исходного состояния.

`WatchClusterMetadata` возвращает канал, на который `metadataPublisher` отправляет все новые версии `ClusterMetadata`.

## 6. Репликация (`internal/replication`)

### 6.1 Компоненты

- `Replicator` (интерфейс) — реализует `FetchFromLeader` (BinaryReplicator);
- `BinaryReplicator` — использует бинарный протокол (`netproto`) и команду Fetch для получения записей и HighWatermark от лидера;
- `Sink` — абстракция приёмника реплицируемых записей:
  - `WALSink` пишет записи в локальный WAL (через `storage.Manager`);
  - `ReportingSink` оборачивает другой sink, обновляет метрики и вызывает `ReportReplicaProgress` у контроллера;
- `PartitionReplicator` — цикл:
  - Fetch у лидера (начиная с локального offset’а);
  - ApplyBatch в sink + обновление `nextOffset`;
  - повтор с заданным интервалом;
- `Manager` — следит за `ClusterMetadata` и запускает/останавливает `PartitionReplicator`’ы для партиций, где брокер — follower.

### 6.2 Устойчивый bootstrap

`WALSink.NextOffset`:

- если лог уже содержит записи, возвращает `HighWatermark() + 1`;
- если партиция пуста, возвращает 0.

`PartitionReplicator.Run`:

- если `nextOffset == 0` и sink реализует `OffsetProvider`, запрашивает стартовый offset через `NextOffset`;
- при каждом Fetch применяет `ApplyBatch` и сдвигает `nextOffset` на `lastApplied + 1`;
- если записей нет, делает паузу и повторяет запрос.

Тесты:

- `TestPartitionReplicatorResumesFromNextOffset` — моделирует рестарт follower’а: новый sink/репликатор продолжает с `NextOffset` и реплицирует только новые записи;
- `TestPartitionReplicatorDoesNotDuplicateAfterCatchUp` — проверяет, что перезапуск догнавшего follower’а не приводит к дубликатам.

## 7. Сетевые протоколы и CLI

### 7.1 Кастомный бинарный протокол (`internal/netproto`)

Фрейм:

- `len32 | apiKey | apiVersion | correlationID | flags | payload` (big‑endian).

Поддерживаемые операции (`api.APIKey`):

- `CreateTopic`, `Produce`, `Fetch`, `ListOffsets`;
- `CommitOffset`, `FetchCommitted`;
- `Metadata`, `Ping`.

`netproto.Server`:

- слушает TCP‑порт;
- декодирует фреймы, вызывает методы брокера (`BrokerAPI`), формирует ответы и пишет обратно в соединение;
- ошибки брокера маппятся на компактные `api.ErrorCode`.

### 7.2 CLI (`cmd/mbctl`)

Команды:

- `create-topic` — создание топика;
- `produce` — отправка сообщений;
- `fetch` — чтение сообщений;
- `metadata`, `list-offsets`, `commit-offset`, `fetch-committed`, `ping`.

CLI использует тот же бинарный протокол, поэтому удобно для скриптов и ручного тестирования.

## 8. MQTT‑фронтенд (`internal/mqtt`)

Поддерживаемые пакеты:

- `CONNECT/CONNACK`;
- `SUBSCRIBE/SUBACK`;
- `PUBLISH` (QoS0/1) + `PUBACK`;
- `PINGREQ/PINGRESP`;
- `DISCONNECT`.

Ключевые особенности:

- MQTT‑клиент отображается на consumer group (group = clientID);
- `SUBSCRIBE` регистрирует клиента в группе и получает назначения партиций через `Broker.JoinGroup`;
- брокер запускает цикл чтения из соответствующих партиций и отправляет MQTT‑сообщения клиенту, коммитя offset’ы;
- `PUBLISH` превращается в `Broker.Produce` в выбранную партицию (по хэш‑функции по топику/клиенту).

Таким образом, MQTT‑клиенты могут публиковать и подписываться на данные, которые хранятся в общем commit‑log’е брокера.

## 9. Наблюдаемость (`internal/observability`, `internal/httpapi`)

### 9.1 Метрики и health

Модуль `observability` регистрирует Prometheus‑метрики:

- `wavemq_messages_produced_total` / `wavemq_messages_consumed_total`;
- `wavemq_request_errors_total` по компонентам и операциям;
- `wavemq_produce_latency_seconds`, `wavemq_fetch_latency_seconds`;
- `wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`.

HTTP‑сервер (в `cmd/mbd`) поднимает эндпоинты:

- `/metrics` — метрики Prometheus;
- `/healthz` — простой health‑чек (готовность зависит от флага `ready`);
- `/debug/pprof/*` — стандартные pprof‑эндпоинты.

### 9.2 HTTP‑админка (`internal/httpapi`)

Основные маршруты:

- `/api/broker` — информация о брокере (ID, адреса, `ReplicationFactor`, режим контроллера);
- `/api/summary` — сводка (количество топиков/партиций, продюсировано/прочитано, количество ошибок);
- `/api/topics` и `/api/topics/...` — список топиков, подробности по топику и сообщения в партиции (для UI);
- `/api/consumers` — snapshot по consumer groups;
- `/api/cluster` — текущее `ClusterMetadata` от контроллера;
- `/api/controller` — состояние контроллера (mode, Raft‑state, term, peers, clusterID, version).

UI (`wave-ui`) использует эти эндпоинты для визуализации состояния брокера и кластера.

## 10. Сценарии запуска и кластерный режим

### 10.1 Single-node

```sh
go build ./cmd/mbd
go build ./cmd/mbctl

./mbd -data-dir=./data -bind=:7912 -mqtt=:1883 -http=:8090
```

### 10.2 Кластерный режим (экспериментальный)

Минимальный пример RF=2 (2 брокера, общий Raft‑контроллер, включена репликация):

1. Собрать бинарники `mbd` и `mbctl`.
2. Выбрать адреса для `-raft-bind` и соответствующие `-raft-peer`.
3. Запустить брокер 1 с `-controller=raft`, `-raft-dir=./data1/raft`, `-data-dir=./data1`, `-replication=true`.
4. Запустить брокер 2 с аналогичными параметрами, но другими `broker-id`, портами и каталогами (`data2`).
5. Создать топик с `replicationFactor=2` и убедиться, что:
   - лидером становится один из брокеров;
   - второй брокер числится как follower;
   - produce/fetch на follower’е возвращает `ErrNotLeader`/HTTP 409 с `leaderBrokerID`.
6. Перезапустить follower и убедиться, что после догонки все подтверждённые записи доступны, а ISR обновлён.

Скрипт `scripts/raft-cluster-demo.sh` автоматизирует аналогичный сценарий.

## 11. Ограничения и направления развития

Ограничения текущей реализации:

- кластерный режим и репликация RF>1 находятся в статусе **experimental**;
- нет детальной обработки split‑brain и сложных отказов сети;
- нет exactly‑once и транзакций;
- упрощённый MQTT (без retained, will, shared subscriptions);
- нет сложных политик балансировки и переразмещения партиций.

Пути развития:

- расширение контроллера (больше команд и инвариантов, улучшенные снапшоты);
- полноценная репликация с high‑watermark’ами, более строгими гарантиями для клиентов;
- улучшенный UI и операционные панели;
- интеграция с внешними системами (коннекторы, bridge к Kafka и т.п.).

---

В таком виде `wave-mq` представляет собой маленькое, но цельное ядро брокера сообщений и кластерной метадаты, пригодное для экспериментов и учебных проектов по потоковой обработке и аналитике.

