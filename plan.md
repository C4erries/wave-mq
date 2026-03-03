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
- [~] Запущена часть `[3]`: добавлен race-safe fallback для Raft-store, убраны прямые `time.Sleep(...)` из тестов, усилена timeout-диагностика multi blackbox и добавлен лог-маркер для `rollback failed: tx closed`.
- [ ] Остальные пункты ниже требуют доведения (финализация решений/прогонов) и отдельного этапа фиксов.

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
