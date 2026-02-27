# План работ по результатам Release-Gate ревью `wave-mq`

## 1. Результаты анализа (кандидаты на фиксы, без описания способов исправления)

### 1.1 Критичность P1

- `go test -race` не проходит в Raft-пути: воспроизводимый `fatal error: checkptr` в связке `raft-boltdb/boltdb` при создании `BoltStore` (`internal/controller/raft_controller.go`, `buildStores`).
- Multi-broker e2e restart-сценарий проверяет post-restart маркеры и ISR, но не валидирует сохранность pre-restart данных/непрерывность оффсетов; `expected_per_partition` в `blackbox_multi_suite.py` вычисляется, но не участвует в проверке restart.

### 1.2 Критичность P2

- Подтвержден флейк `bb-multi-smoke`: в серии 20 прогонов был 1 падение (`19/20`) в сценарии `broker restart` с `blackbox failed: timed out`.
- В `blackbox_multi_suite.py` проверки метрик/summary слабее, чем в single-suite: есть проверка `consumed < 0` (нулевое потребление считается валидным), а summary проверяется в основном на наличие ключей.
- Потенциальный nil-deref в HTTP-обработчике `/api/controller`: вызов `h.ctrl.GetClusterMetadata(...)` выполняется до проверки `h.ctrl == nil` (`internal/httpapi/api.go`).
- В тестах есть значимый слой тайминг-зависимых ожиданий через `time.Sleep(...)` (Raft/broker/replication/mqtt/observability), что повышает риск нестабильности при колебаниях окружения.

### 1.3 Дополнительные результаты проверок

- `go test ./... -count=1`: успешно.
- `go vet ./...`: успешно.
- Таргетные integration/e2e-like Go тесты с повторениями (`x10`): успешно.
- Blackbox single: `smoke 20/20`, `full 3/3`.
- Blackbox multi: `smoke 19/20` (1 timeout), дополнительная серия `10/10` успешна, `full 3/3` успешно.
- `golangci-lint run ./...`: функциональных падений не выявлено, но зафиксированы quality-замечания (complexity/hugeParam/gocyclo).

## 2. Фаза принятия решений

- На основе пункта 1 принимаются решения по приоритетам и составу фиксов.
- Реализация изменений выполняется отдельным этапом после утверждения списка.

## 3. retention policy

## 4. Выбор партиции по ключу(хеш)

## 5. Data analysis не работает для мультиброкера

## 6. иногда вылетаает rollaback failed tx closed у мультиброкера