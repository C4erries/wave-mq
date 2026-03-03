# Refactor Plan: wave-mq

## Цель
Системно довести кодовую базу до надежного, предсказуемого и тестопригодного состояния для брокера сообщений. План разбит на 7 рабочих запросов (итераций) так, чтобы закрывать подсистемы по слоям зависимостей и минимизировать регрессии.

## Архитектурная карта зависимостей
1. Контракты и данные: `pkg/api`.
2. Надежное хранение и метаданные: `internal/storage`, `internal/metadata`.
3. Координация кластера: `internal/controller`.
4. Ядро брокера: `internal/broker`.
5. Репликация: `internal/replication`.
6. Транспортные интерфейсы и наблюдаемость: `internal/netproto`, `internal/httpapi`, `internal/mqtt`, `internal/observability`.
7. Точки входа и CLI: `cmd/mbd`, `cmd/mbctl`, `cmd/mbbench`.

Критичный поток надежности: `controller -> broker -> storage/offsets -> replication -> transport`.

## Обязательные правила рефакторинга и тестирования
1. Надежность выше удобства: никакого silent fallback без явного логирования/контракта degraded mode.
2. Ошибки не теряются: в production-коде не глотать ошибки; возвращать/оборачивать контекстом.
3. Тесты не игнорируют ошибки: никаких `_, _ = ...`, `_ = ...` для error-returning вызовов (включая setup/cleanup).
4. В тестах использовать `testify` (`require` для preconditions, `assert` для дополнительных проверок).
5. Использовать `t.Run()` для таблиц сценариев; `t.Parallel()` включать на уровне тест-кейсов, где нет shared mutable state.
6. Тестировать black-box поведение функции/модуля (входы/выходы/контракты), а не только внутренние детали.
7. Для конкурентных участков обязательны race-ориентированные сценарии и запуск с `-race` на затронутых пакетах.
8. Для каждой правки: минимум 1 позитивный и 1 негативный сценарий, плюс регресс-тест на исправленный дефект.
9. Любая новая абстракция (интерфейс/фабрика/helper) должна уменьшать связность и улучшать тестопригодность, а не увеличивать ceremony.
10. После каждой итерации: целевые тесты по измененным пакетам, в конце — `go test ./... -count=1`.

## Итерация 1/7: Базовые контракты, WAL и metadata-store

### Зона изменений (файлы)
- `pkg/api/api.go`
- `internal/storage/log.go`
- `internal/storage/log_test.go`
- `internal/storage/log_fuzz_test.go`
- `internal/storage/log_bench_test.go`
- `internal/metadata/store.go`
- `internal/metadata/store_test.go`

### Зависимости и смысл
- Это фундамент для всех вышестоящих подсистем.
- Любой дефект здесь масштабируется в broker/controller/replication.

### Что делаем
- Укрепить инварианты WAL (append/reopen/retention/index consistency).
- Свести к минимуму неявные состояния в metadata event encode/decode.
- Уточнить/документировать контракты `api` структур, где есть неоднозначность.

### Проверки
- `go test ./internal/storage -count=1`
- `go test ./internal/metadata -count=1`
- при необходимости: `go test -race ./internal/storage -count=1`

## Итерация 2/7: Controller и кластерная координация

### Зона изменений (файлы)
- `internal/controller/controller.go`
- `internal/controller/controller_test.go`
- `internal/controller/raft_controller.go`
- `internal/controller/raft_controller_test.go`
- `internal/controller/metadata_publisher.go`
- `internal/controller/factory.go`
- `internal/controller/peers.go`
- `internal/controller/errors.go`
- `internal/controller/race_enabled.go`
- `internal/controller/race_disabled.go`

### Зависимости и смысл
- Controller определяет корректность лидерства, assignments, версий метаданных.
- Ошибки здесь ломают routing, replication и consistency.

### Что делаем
- Привести paths single/raft к единым контрактам ошибок и lifecycle.
- Убрать скрытые зависимости, усилить тестируемость через seam/factory.
- Проверить race-safe режимы и корректность fallback-веток.

### Проверки
- `go test ./internal/controller -count=1`
- `go test -race ./internal/controller -count=1`

## Итерация 3/7: Core broker (topics/partitions/groups/offsets)

### Зона изменений (файлы)
- `internal/broker/broker.go`
- `internal/broker/offset_store.go`
- `internal/broker/broker_test.go`
- `internal/broker/broker_cluster_test.go`
- `internal/broker/broker_cluster_partitions_test.go`
- `internal/broker/multibroker_integration_test.go`
- `internal/broker/multibroker_rf_test.go`
- `internal/broker/recovery_test.go`
- `internal/broker/replication_restart_test.go`
- `internal/broker/offset_store_test.go`
- `internal/broker/race_enabled_test.go`
- `internal/broker/race_disabled_test.go`
- `internal/broker/broker_bench_test.go`

### Зависимости и смысл
- Это главный доменный слой брокера сообщений.
- Здесь критичны корректность лидерства, offset semantics, потокобезопасность.

### Что делаем
- Финализировать lock discipline, убрать дубли read-path логики.
- Проверить not-leader/leader-forward контракты и metadata snapshot consistency.
- Усилить тесты на recovery/restart/cluster edge-cases.

### Проверки
- `go test ./internal/broker -count=1`
- `go test -race ./internal/broker -count=1`

## Итерация 4/7: Replication pipeline

### Зона изменений (файлы)
- `internal/replication/replication.go`
- `internal/replication/client.go`
- `internal/replication/client_test.go`
- `internal/replication/partition_replicator.go`
- `internal/replication/partition_replicator_test.go`
- `internal/replication/wal_sink.go`
- `internal/replication/wal_sink_test.go`
- `internal/replication/reporting_sink.go`
- `internal/replication/reporting_sink_test.go`
- `internal/replication/manager.go`
- `internal/replication/manager_test.go`

### Зависимости и смысл
- Репликация связывает controller assignments и storage durability.
- Любой дефект ведет к lag, diverged state или data loss.

### Что делаем
- Вычистить orchestration start/stop/restart worker lifecycle.
- Укрепить контракты sink/client и backoff/cancel semantics.
- Добавить black-box тесты на сбои сети/переизбрание лидера.

### Проверки
- `go test ./internal/replication -count=1`
- `go test -race ./internal/replication -count=1`

## Итерация 5/7: Binary/HTTP transport layer

### Зона изменений (файлы)
- `internal/netproto/codec.go`
- `internal/netproto/messages.go`
- `internal/netproto/server.go`
- `internal/netproto/codec_test.go`
- `internal/netproto/codec_fuzz_test.go`
- `internal/netproto/server_test.go`
- `internal/httpapi/api.go`
- `internal/httpapi/api_test.go`

### Зависимости и смысл
- Это пользовательский вход в систему; ошибки здесь бьют по UX и надежности API-контрактов.

### Что делаем
- Упростить и унифицировать request/response pipeline и error mapping.
- Проверить защиту от forwarding loops и корректность retry/timeout.
- Доусилить black-box тесты API на негативные и edge сценарии.

### Проверки
- `go test ./internal/netproto -count=1`
- `go test ./internal/httpapi -count=1`

## Итерация 6/7: MQTT + observability

### Зона изменений (файлы)
- `internal/mqtt/packets.go`
- `internal/mqtt/codec.go`
- `internal/mqtt/server.go`
- `internal/mqtt/codec_test.go`
- `internal/mqtt/codec_fuzz_test.go`
- `internal/mqtt/server_test.go`
- `internal/mqtt/server_exactly_once_test.go`
- `internal/observability/metrics.go`
- `internal/observability/http.go`
- `internal/observability/http_test.go`

### Зависимости и смысл
- MQTT runtime и observability напрямую влияют на операционную надежность продакшена.

### Что делаем
- Убрать magic behavior и неявные тайминги, сделать runtime-параметры явными.
- Проверить exactly-once/QoS-контракты и cancellation semantics.
- Проверить корректность health/ready/metrics lifecycle.

### Проверки
- `go test ./internal/mqtt -count=1`
- `go test ./internal/observability -count=1`

## Итерация 7/7: Точки входа, CLI, интеграционный финал

### Зона изменений (файлы)
- `cmd/mbd/main.go`
- `cmd/mbd/main_test.go`
- `cmd/mbctl/main.go`
- `cmd/mbctl/main_test.go`
- `cmd/mbbench/main.go`

### Зависимости и смысл
- Финальный wiring слой должен быть простым, проверяемым и без скрытых side effects.

### Что делаем
- Проверить bootstrap/shutdown sequencing и обработку ошибок end-to-end.
- Упростить CLI command dispatch/validation/output contracts.
- Проверить согласованность поведения инструментов относительно контрактов broker API.

### Проверки
- `go test ./cmd/mbd -count=1`
- `go test ./cmd/mbctl -count=1`
- финальный прогон: `go test ./... -count=1`

## Сквозной контроль качества (после каждой итерации)
1. Перечень изменений фиксируется по файлам и контрактам.
2. Тесты покрывают ключевые ветви поведения, а не только happy-path.
3. Нет проигнорированных ошибок в тестовом коде.
4. Нет роста технического долга за счет «временных» костылей.
5. Рефактор не меняет публичное поведение без явного обоснования и теста.
