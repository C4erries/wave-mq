# План работ по результатам Release-Gate ревью `wave-mq`

## Статус выполнения (на 2026-03-03)

- Маркировка этапов: `[1]` — текущая реализация, `[2]` — следующий функциональный блок, `[3]` — стабилизация/диагностика.
- [x] Усилен multi-broker e2e restart-аудит в `examples/python/blackbox_multi_suite.py`:
  - теперь проверяется pre-restart baseline,
  - проверяется непрерывность offset,
  - проверяется полнота окна сообщений после restart.
- [x] По падению `after restart partition 1: expected 460 messages, got 60` выявлена причина:
  - после restart WAL-сегмент может перезаписываться с начала файла в `internal/storage/log.go` (`openSegment`/`AppendBatch`), что приводит к потере pre-restart части лога в чтении.
- [x] Запущена часть `[1]`: исправлены `openSegment`/`AppendBatch` после reopen, nil-guard в `/api/controller`, и усилены проверки `metrics/summary` в multi blackbox.
- [x] Запущена часть `[2]`: добавлены key-hash роутинг (`/api/topics/{topic}/messages`), агрегированный fetch для `all`-режима, leader-forward для partition/topic messages и topic-level retention в `TopicConfig`/metadata/storage.
- [x] Запущена часть `[3]`: добавлены race/locking/DI/observability/CLI/netproto/mbd-архитектурные фиксы по пунктам 7-18, покрыты unit-тестами и full `go test ./...`.
- [~] Открытые хвосты в плане сведены к точечным пунктам (retention multi-broker propagation, `rollback failed: tx closed`).

## 1. Результаты анализа (кандидаты на фиксы, без описания способов исправления)

### 1.1 Критичность P1

- `go test -race` не проходит в Raft-пути: воспроизводимый `fatal error: checkptr` в связке `raft-boltdb/boltdb` при создании `BoltStore` (`internal/controller/raft_controller.go`, `buildStores`). Статус: [~] частично исправлено.
  - Часть: `[3]`.
  - Текущее состояние: `buildStores` теперь использует race-safe ветку (`raft.NewInmemStore`) при сборке с `-race`; для persistent сценариев Raft-тесты, завязанные на boltdb-state, явно помечены `Skip` под `-race`.
  - Предложение решения: разделить storage для Raft по режимам выполнения, ввести безопасный путь для `-race` (временный in-memory fallback или совместимая с `-race` boltdb/bbolt-реализация), зафиксировать это регрессионным `go test -race` для Raft-теста.
  - Реализация: [~] частично выполнено — добавлен race-detection (`internal/controller/race_*.go`) и fallback в `buildStores`; `go test -race ./internal/controller -count=1` и `go test -race ./internal/broker -count=1` проходят. Открытый хвост: race-safe persistent backend вместо in-memory fallback.
- Multi-broker e2e restart-сценарий проверяет post-restart маркеры и ISR, но не валидирует сохранность pre-restart данных/непрерывность оффсетов; `expected_per_partition` в `blackbox_multi_suite.py` вычисляется, но не участвует в проверке restart. Статус: [x] исправлено в `blackbox_multi_suite.py`.
- Выявлен дефект сохранности лога после restart в storage-пути (`internal/storage/log.go`, `openSegment`/`AppendBatch`): при определённых условиях старое содержимое сегмента затирается, после чего fetch видит только хвостовые сообщения (пример: offsets `400..459` вместо `0..459`). Статус: [x] выявлено, [x] исправлено.
  - Часть: `[1]`.
  - Текущее состояние: `openSegment` вычисляет `goodBytes` через `scanSegment` на `ReadAt`, но не переводит файловый указатель на конец валидной части; `AppendBatch` пишет через `file.Write`, поэтому после restart запись может идти с `offset=0` и затирать начало сегмента.
  - Предложение решения: перед первой записью после открытия сегмента явно синхронизировать позицию файла (`Seek(goodBytes, io.SeekStart)`), дополнительно защитить запись через позиционную модель (`WriteAt` от `active.size`) и добавить регрессионный тест на append после restart без потери pre-restart оффсетов.
  - Реализация: [x] выполнено — добавлен `Seek(goodBytes, io.SeekStart)` в `openSegment`; добавлен регрессионный тест `TestAppendAfterReopenPreservesExistingSegmentData`.

### 1.2 Критичность P2

- Подтвержден флейк `bb-multi-smoke`: в серии 20 прогонов был 1 падение (`19/20`) в сценарии `broker restart` с `blackbox failed: timed out`. Статус: [~] в работе.
  - Часть: `[3]`.
  - Текущее состояние: в `wait_full_isr` убран вложенный `wait_topic_assignments`; используется единый polling-контур с общим дедлайном и расширенным timeout payload (версия кластера + snapshot assignments/ISR/leader).
  - Предложение решения: убрать вложенные ожидания в один polling-контур с общим дедлайном, добавить расширенную диагностику при timeout (cluster snapshot/ISR/leader map) и параметризовать таймауты по профилю нагрузки.
  - Реализация: [~] частично выполнено — обновлен `wait_full_isr` в `examples/python/blackbox_multi_suite.py`; добавлена пост-мортем диагностика по лог-маркеру `rollback failed: tx closed`. Открытый хвост: повторные стресс-прогоны blackbox (`smoke/full`) и калибровка таймаутов под профиль нагрузки.
- В `blackbox_multi_suite.py` проверки метрик/summary слабее, чем в single-suite: есть проверка `consumed < 0` (нулевое потребление считается валидным), а summary проверяется в основном на наличие ключей. Статус: [x] исправлено.
  - Часть: `[1]`.
  - Текущее состояние: multi-suite в `scenario_metrics` валидирует только наличие ключей summary и базовые неотрицательные значения, но не сверяет метрики с ожидаемым объемом трафика из сценариев.
  - Предложение решения: усилить проверки до сопоставления с фактическими ожиданиями (`expected_per_partition`, суммарно произведено/прочитано), минимум `consumed > 0` после fetch-фазы и контроль непротиворечивости summary vs metrics.
  - Реализация: [x] выполнено — в `scenario_metrics` добавлена агрегированная сверка summary/metrics с `expected_per_partition` и обязательное `consumed > 0` на сумме.
- Потенциальный nil-deref в HTTP-обработчике `/api/controller`: вызов `h.ctrl.GetClusterMetadata(...)` выполняется до проверки `h.ctrl == nil` (`internal/httpapi/api.go`). Статус: [x] исправлено.
  - Часть: `[1]`.
  - Текущее состояние: в `handleControllerStatus` вызов `h.ctrl.GetClusterMetadata(...)` стоит до guard-условия `h.ctrl == nil`, что оставляет риск panic при неинициализированном контроллере.
  - Предложение решения: сделать раннюю проверку `h.ctrl == nil` до любых вызовов интерфейса контроллера и возвращать безопасный пустой payload метаданных.
  - Реализация: [x] выполнено — добавлен nil-guard до вызовов контроллера и тест `TestControllerStatusEndpointWithoutController`.
- В тестах есть значимый слой тайминг-зависимых ожиданий через `time.Sleep(...)` (Raft/broker/replication/mqtt/observability), что повышает риск нестабильности при колебаниях окружения. Статус: [x] исправлено.
  - Часть: `[3]`.
  - Текущее состояние: прямые `time.Sleep(...)` в test-коде удалены; ожидания переведены на ticker/deadline polling и condition-based helpers.
  - Предложение решения: заменить фиксированные sleeps на event/poll-based ожидания с дедлайном и едиными helper-функциями, оставляя sleep только как часть poll-интервала.
  - Реализация: [x] выполнено — обновлены ожидания в `internal/controller`, `internal/broker`, `internal/replication`, `internal/mqtt`, `internal/observability`, `internal/storage`; проверка `rg -n "time\\.Sleep\\(" internal -g "*_test.go"` возвращает 0 совпадений.

### 1.3 Дополнительные результаты проверок

- `go test ./... -count=1`: успешно.
- `go vet ./...`: успешно.
- Таргетные integration/e2e-like Go тесты с повторениями (`x10`): успешно.
- Blackbox single: `smoke 20/20`, `full 3/3`.
- Blackbox multi: `smoke 19/20` (1 timeout), дополнительная серия `10/10` успешна, `full 3/3` успешно.
- `golangci-lint run ./...`: функциональных падений не выявлено, но зафиксированы quality-замечания (complexity/hugeParam/gocyclo).

## 2. Фаза принятия решений

- На основе пункта 1 принимаются решения по приоритетам и составу фиксов. Статус: [ ] ожидает решения.
  - Часть: `[2]`.
  - Текущее состояние: список проблем собран, но отсутствует явная матрица приоритетов/владельцев/критериев готовности к фиксу.
  - Предложение решения: оформить triage-таблицу (priority, owner, риск регрессии, обязательные тесты, целевой релиз) и утвердить порядок реализации.
  - Реализация: [на данном этапе не начато].
- Реализация изменений выполняется отдельным этапом после утверждения списка. Статус: [ ] в ожидании.
  - Часть: `[2]`.
  - Текущее состояние: этап реализации формально не открыт, привязка к задачам/PR не заведена.
  - Предложение решения: после утверждения triage открыть для каждого фикса отдельный подэтап с критериями done и ссылками на артефакты прогонов.
  - Реализация: [на данном этапе не начато].

## 3. retention policy
Статус: [~] частично реализовано.
- Часть: `[2]`.
- Текущее состояние: базовая retention уже работает на уровне broker-wide конфигурации (`-retention-bytes`, `-retention-hours`) и применяется в storage (`SegmentMaxAge`, `MaxLogBytes`), покрыта тестами по size/age/start offset.
- Предложение решения: уточнить scope как topic-level retention (политики на топик/партицию, хранение в metadata, управление через API, проверки в multi-broker сценариях).
- Реализация: [~] частично выполнено — добавлены поля retention в `api.TopicConfig`, HTTP `POST /api/topics` (`retentionBytes`/`retentionHours`), metadata event v2 и per-topic overrides в `storage.OpenLog`; покрыто тестом `TestCreateTopicAppliesRetentionOverrides`. Открытый хвост: перенос topic-level retention через controller metadata в multi-broker watcher-путь.

## 4. Маршрутизация сообщений по key-hash (Kafka-подобно), включая тесты детерминизма
Статус: [x] реализовано.
- Часть: `[2]`.
- Текущее состояние: в HTTP/binary data-plane партиция задается строго явно (`/partitions/{id}` или `partition` в протоколе), ключ записи не участвует в выборе партиции; в MQTT есть хеш-выбор, но по `topic+clientID`, а не по message key.
- Предложение решения: перейти на режим по умолчанию "как в Kafka" — выбор партиции по `key` через стабильный hash по актуальному списку партиций, с тестом `same key -> same partition` и smoke-проверкой распределения разных ключей.
- Реализация: [x] выполнено — добавлены `Broker.ProduceByKey`, key-hash выбор партиции и endpoint `POST /api/topics/{topic}/messages`; добавлены тесты `TestProduceByKeyRoutesSameKeyToSamePartition` и `TestTopicProduceByKeyEndpoint`. Совместимость сохранена: явный produce в `/partitions/{id}/messages` не удален.

## 5. Data analysis не работает для мультиброкера
Статус: [x] реализовано.
- Часть: `[2]`.
- Текущее состояние: UI `DataAnalysis` при режиме `all` фактически читает только `partition=0`; запросы чтения/записи не leader-aware и при multi-broker могут получать `409 not_leader`, что ломает сценарий анализа.
- Предложение решения: добавить leader-aware маршрут для чтения сообщений (backend proxy/forward по cluster metadata) и доработать UI для агрегации по всем партициям топика с корректной обработкой not-leader.
- Реализация: [x] выполнено — добавлен `GET /api/topics/{topic}/messages` с агрегацией по партициям и обработкой follower->leader forwarding; добавлен forward для partition produce/fetch; в UI `fetchMessages` для `partition=all` теперь использует агрегированный endpoint. Добавлен тест `TestTopicMessagesEndpointAggregatesAllPartitions`.

## 6. иногда вылетает `rollback failed: tx closed` у мультиброкера
Статус: [~] в работе.
- Часть: `[3]`.
- Текущее состояние: источник строки локализован в dependency `github.com/hashicorp/raft-boltdb` (defer `tx.Rollback()` после `tx.Commit()`), что может давать шум `Rollback failed: tx closed` в логах; в blackbox добавлена выборка этого маркера по сервисным логам.
- Предложение решения: добавить отдельный диагностический этап (воспроизведение + расширенные логи + привязка к операции/узлу/времени), после локализации выбрать точечный фикс и регрессионный сценарий.
- Реализация: [~] частично выполнено — `examples/python/blackbox_multi_suite.py` теперь печатает отдельный diagnostics-блок по маркеру `rollback failed: tx closed`. Открытый хвост: принять продуктовое решение (шумный warning vs функциональная ошибка) и при необходимости заменить boltdb backend/версию.

### 6.1 Локализация `rollback failed: tx closed` (добавлено для снятия блокера)
Статус: [~] в работе.
- Часть: `[3]`.
- Текущее состояние: MRE в blackbox не зафиксирован, но добавлена автоматическая выборка marker-lines из логов `broker1/broker2` при падении и определен upstream-источник сообщения.
- Предложение решения: собрать минимальный воспроизводимый сценарий в multi-broker тесте с обязательным лог-снимком обоих брокеров и контроллера в момент ошибки.
- Реализация: [~] частично выполнено — добавлен этап лог-локализации; полный MRE и регрессионный тест пока не собраны.

## 7. Data race/неконсистентные проверки лидерства в `internal/broker/broker.go`
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: в clustered-пути есть чтение `Partition.Metadata` вне `p.mu` одновременно с обновлениями под `p.mu` в `updatePartitionMetadata` (пример: `Produce`/`Fetch` читают `p.Metadata.Replica.Role` до `p.mu`; snapshot-методы (`TopicAndPartitionCounts`, `TopicsSnapshot`) тоже читают `p.Metadata.Replica.Role` без `p.mu`). Это создает риск data race и неверных решений `leader/follower` в момент смены метаданных.
- Предложение решения: ввести единый accessor для снимка `Partition.Metadata` под `p.mu` и использовать его во всех read-path; в `Produce`/`Fetch` сначала брать `p.mu`/snapshot, затем принимать решение по лидерству; добавить таргетный race/regression тест на параллельные `StartClusterMetadataWatcher` updates + produce/fetch.
- Реализация: [x] выполнено — добавлен `metadataSnapshot()` под `p.mu`, проверка роли перенесена под lock в `Produce`/`Fetch`, snapshot-методы (`Metadata`, `TopicAndPartitionCounts`, `TopicsSnapshot`, `TopicDetail`) переведены на lock-safe чтение; добавлен regression-тест `TestProduceFetchConcurrentWithMetadataUpdates`.

## 8. Костыль в bootstrap-регистрации брокера через Raft leader (HTTP port coupling)
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `postRegisterBrokerToLeader` строит URL лидера из `leaderAddr` (host) и порта, извлеченного из `info.HTTPAddr` регистрируемого брокера, а не из HTTP-адреса самого лидера. Это неявно требует одинаковый HTTP-port на всех брокерах и может ломать регистрацию/старт кластера в heterogenous/NAT-сценариях.
- Предложение решения: резолвить HTTP endpoint лидера по cluster metadata (`BrokerInfo{ControllerAddr->HTTPAddr}`) либо через явный mapping peer->http в конфиге; убрать эвристику "host лидера + локальный порт" и покрыть тестом с разными HTTP-портами у узлов.
- Реализация: [x] выполнено — добавлен `leaderRegisterBrokerURL(...)` с приоритетом metadata-resolve (`ControllerAddr -> HTTPAddr`) и fallback на старую эвристику; регистрация через лидера использует новый резолвер; добавлен тест `TestLeaderRegisterBrokerURLUsesLeaderHTTPAddrFromMetadata`.

## 9. Рефактор HTTP forwarding-клиента в `internal/httpapi` для DI и тестируемости
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: forwarding-запросы в API-обработчиках привязаны к `http.DefaultClient.Do(...)` и статическим timeout/retry-решениям; transport невозможно подменить локально без глобальных side-effect, что ограничивает unit-тесты негативных сценариев (timeout/network split/retry policy).
- Предложение решения: ввести интерфейс `HTTPDoer` (`Do(*http.Request) (*http.Response, error)`) и внедрять его в `Handler` через конструктор (с default fallback на `http.DefaultClient`); вынести retry/redirect policy в отдельный helper с явными параметрами.
- Реализация: [x] выполнено — в `Handler` внедрен `HTTPDoer`, добавлен `NewWithHTTPClient(...)`, forwarding-пути переведены на инжектируемый клиент; добавлен тест `TestForwardToURLUsesInjectedHTTPClient`.

## 10. Фабрика `PartitionReplicator`/`Sink` в `internal/replication/manager.go`
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `Manager.applyMetadata` напрямую создает `NewWALSink` + `NewReportingSink` + `NewPartitionReplicator`; из-за этого в тестах сложно изолированно проверять lifecycle (create/restart/cancel), а также обработку ошибок старта/остановки без интеграционного запуска.
- Предложение решения: внедрить фабрики `SinkFactory` и `ReplicatorFactory` (или единый `ReplicationWorkerFactory`) в `NewManager`; оставить текущую реализацию как production-default, а в тестах использовать fake factories для точной проверки orchestration.
- Реализация: [x] выполнено — добавлены `SinkFactory`/`WorkerFactory`, конструктор `NewManagerWithFactories(...)`, вынесен `startReplicator(...)`, добавлен тест `TestManagerUsesInjectedFactories`.

## 11. Детерминизм MQTT runtime: тайминги/контекст как зависимости
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: в `internal/mqtt/server.go` зашиты magic-intervals (`50ms`, `100ms`, `300ms`) и используется `context.Background()` в `LeaveGroup`/`CommitOffset`-пути; это усложняет детерминированные тесты и делает cancellation менее прозрачной.
- Предложение решения: вынести retry/poll интервалы в конфиг `ServerOptions` и внедрить через `NewServer`; заменить `context.Background()` на производный от client/session context; для тестов добавить fake clock/timer seam либо инкапсулированный sleeper интерфейс.
- Реализация: [x] выполнено — добавлены `ServerOptions` и `NewServerWithOptions(...)`; runtime интервалы вынесены в опции; в `LeaveGroup`/`CommitOffset` использован производный context вместо `context.Background()`.

## 12. Декомпозиция `cmd/mbd/main.go` через app/factory слой
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `main()` содержит в одном месте parsing, wiring, bootstrap, registration, server lifecycle и shutdown; heavy use `os.Exit(...)` и concrete constructors затрудняет unit-тесты порядка инициализации и error-path без запуска реального окружения.
- Предложение решения: выделить `run(ctx, deps, cfg) error`/`App` слой с dependency factories (`StorageFactory`, `ControllerFactory`, `ServerFactory`) и оставить `main` тонким адаптером (`parse flags` + `if err != nil { os.Exit(1) }`); добавить table-driven tests на bootstrap/error sequencing.
- Реализация: [x] выполнено — выделены `parseStartupOptions(...)`, `validateBrokerConfig(...)`, `run(...)`, введен `appFactory` с injectable конструкторами и тонкий `main`-адаптер; добавлены тесты `TestParseStartupOptions`, `TestValidateBrokerConfig`, `TestRunReturnsStorageInitError`.

## 13. Упрощение snapshot-логики в `internal/broker/broker.go` (дубли и скрытые fallback-и)
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `TopicAndPartitionCounts`, `TopicsSnapshot`, `TopicDetail`, `partitionAssignments` повторяют схожую логику получения assignments/leader-only фильтрации и локального fallback; при этом используется `context.Background()` + частичное игнорирование ошибок `clusterAssignments`, что делает поведение неявным и усложняет сопровождение.
- Предложение решения: вынести общий helper уровня `topicView(assignments, brokerID)`/`resolveAssignments(ctx)` и централизовать policy обработки ошибок (явный degraded-mode вместо silent fallback); добавить table-driven тесты на clustered/non-clustered и error-path metadata.
- Реализация: [x] выполнено — добавлены `clusterAssignmentsBestEffort(ctx)`/`partitionAssignments(ctx, ...)`, введен общий helper `leaderPartitionCount(...)` для snapshot-путей, fallback переведен в явный degraded-mode с логированием; добавлены тесты `TestTopicPartitionIDsFallbackOnClusterMetadataError` и `TestTopicPartitionIDsUseClusterAssignmentsWhenAvailable`.

## 14. Декомпозиция `netproto` dispatch в таблицу handlers
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `internal/netproto/server.go` содержит большой `switch apiKey` с дублированием decode/errorResponse/metrics-паттернов; добавление нового API-key требует правок в нескольких местах и увеличивает риск расхождений.
- Предложение решения: перейти к map-based registry `map[APIKey]Handler` c общими обертками (`decode -> call broker -> map error -> encode`), а `errorResponseForKey` и общие счетчики собрать в единый pipeline.
- Реализация: [x] выполнено — внедрен registry `map[APIKey]requestHandler`, `dispatch` переведен на lookup, кейсы вынесены в отдельные handler-методы и общий decode-helper; поведение протокола сохранено, тесты `internal/netproto` проходят.

## 15. Типизированные ключи вместо `fmt.Sprintf("%s:%d")` в `replication.Manager`
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: lifecycle-реестр репликаторов хранится в `map[string]runningReplicator` с ключом `"topic:partition"`, формируемым через `fmt.Sprintf`; это лишняя сериализация/парсинг-модель и потенциальный источник ошибок при рефакторинге ключевого формата.
- Предложение решения: заменить ключ на структурный тип (`type partitionKey struct { topic string; partition int }`) и вынести stop/start transitions в отдельные методы для более читаемого orchestration-кода; добавить unit-тесты переходов `desired -> running`.
- Реализация: [x] выполнено — `running` переведен на `map[partitionKey]runningReplicator`; orchestration вынесен в `startReplicator(...)`; покрыто unit-тестом на инжектируемые фабрики.

## 16. Унификация forwarding helper-ов в `internal/httpapi/api.go`
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `forwardCreateTopicToLeader`, `forwardPartitionMessagesToLeader`, `forwardPartitionProduceToLeader`, `forwardTopicProduceToLeader` реализуют похожий HTTP forwarding-flow (build URL/request, header guard, do/read response) с частичным дублированием.
- Предложение решения: вынести общий `forwardJSONToLeader`/`forwardToBroker` helper с параметрами `method/path/query/body/retryPolicy` и оставить в handlers только бизнес-ветвление; покрыть тестами на not-leader chain, retry и защиту от forwarding-loop.
- Реализация: [x] выполнено — добавлены общие helper-ы `forwardToURL(...)`/`forwardRequest(...)`, все четыре forwarding-пути переведены на них, добавлены тесты на инжектируемый transport и headers/payload passthrough.

## 17. Упрощение observability helpers (`sumCounter` и `StartHTTPServer` callback semantics)
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `sumCounter` использует отдельную goroutine + channel для синхронной операции, что избыточно; в `StartHTTPServer` комментарий про `onStarted` не совпадает с фактическим порядком вызова (callback вызывается до `ListenAndServe`).
- Предложение решения: сделать `sumCounter` синхронным collector-проходом без лишней goroutine; выровнять контракт `onStarted` (либо переименовать в `beforeServe`, либо вызывать после фактического bind/listen через явный `net.Listen` + `srv.Serve`).
- Реализация: [x] выполнено — `sumCounter` сделан синхронным без лишней goroutine; `StartHTTPServer` переведен на `net.Listen + srv.Serve`, callback `onStarted` вызывается после успешного bind/listen.

## 18. Упрощение CLI-архитектуры `cmd/mbctl` (команды/валидация/вывод)
Статус: [x] выполнено.
- Часть: `[3]`.
- Текущее состояние: `main.go` содержит большой switch по командам и много почти одинаковых `handleX`/`sendX` блоков с повторяющимся парсингом, валидацией и error-print + `os.Exit`.
- Предложение решения: перейти на table-driven command registry (`name -> run(args) error`) с общими helper-ами для флагов/валидации/печати ошибок; для транспортного слоя оставить единый typed wrapper вокруг `sendRequest`; добавить unit-тесты command-dispatch/validation.
- Реализация: [x] выполнено — CLI переведен на `commandList` + `runCLI(...)` и command handlers без `os.Exit` внутри; добавлены unit-тесты `cmd/mbctl/main_test.go` на usage/dispatch/validation.
