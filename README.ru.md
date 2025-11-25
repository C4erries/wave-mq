# wave-mq

wave-mq — это односерверный log-based брокер сообщений на Go, спроектированный так, чтобы его можно было развить в небольшой, понятный Kafka‑подобный кластер. Он предоставляет кастомный бинарный протокол, минимальный MQTT 3.1.1/5.0 фронтенд (QoS0/1), consumer groups с offset’ами на брокере и сегментированное WAL‑хранилище со sparse‑индексом и retention.

## Состояние проекта

- Основной single-node брокер (storage, бинарный протокол, MQTT, consumer groups, HTTP UI/API) — реализован и подходит для локальных экспериментов и демонстраций.
- Persist метаданных топиков (`metadata.log`) — реализован; топики и партиции поднимаются после рестарта и восстанавливаются при старте брокера.
- Слой кластерных метаданных (контроллер + `/api/cluster`) — реализован для single-node и multi-broker режимов. Raft‑контроллер доступен через `-controller=raft` (single-node или multi-peer) и является рекомендуемым режимом для кластерных развёртываний; `-controller=single` оставляет легаси in‑memory контроллер.
- Путь репликации (leader → follower) — реализованы бинарный клиент (`BinaryReplicator`) и PartitionReplicator (WAL‑sink + отчёт о прогрессе в ISR). Репликация включается опционально через флаг `-replication`. RF>1 поддержан на уровне протокола и метаданных; включение репликации можно делать поэтапно.
- Multi-node / Raft‑кластер контроллера — поддерживается для локальных multi-broker кластеров. Базовые сценарии failover и rolling restart покрыты тестами; дальнейшая эксплуатационная обкатка и тулзы находятся в работе.

## Дорожная карта

### 1. Основа (текущее состояние)

- Single-node брокер с сегментированным WAL, sparse‑индексом, retention по размеру/времени и crash‑recovery.
- Сервер бинарного протокола (`internal/netproto`), минимальный MQTT‑фронтенд (`internal/mqtt`), CLI‑клиент (`cmd/mbctl`), HTTP admin API + React UI (`wave-ui`).
- Persist метаданных топиков через `metadata.log`; broker и HTTP API восстанавливают topics/partitions после рестарта.
- Контроллер кластерных метаданных:
  - `SingleNodeController` с `ClusterMetadata` и `/api/cluster`‑снапшотом для простых/single-node сценариев.
  - `RaftController`: Raft‑поддерживаемая FSM для `ClusterMetadata` (single-node или multi-peer) с командами `AssignTopic`, `RegisterBroker`, `ReportReplicaProgress` и snapshot/restore.
- Репликация:
  - `BinaryReplicator` (использует существующий Fetch бинарного протокола и возвращает записи + HighWatermark).
  - `PartitionReplicator` с интерфейсом `Sink` (WAL‑sink + reporting‑sink) и API управления ISR (`ReportReplicaProgress`) в контроллере.
  - Репликация включается флагом `-replication`; по умолчанию брокер ведёт себя как RF=1 leader‑only.

### 2. Multi-broker (кластерный режим)

Этот этап во многом реализован и отвечает за практическое поведение кластера.

#### 2.1 Raft‑контроллер в основном пути

- RaftController — полноценная реализация интерфейсов контроллера, выбирается флагами:

  ```sh
  -controller=raft \
  -raft-bind=<host:port> \
  -raft-peer=<host1:port1,host2:port2,...> \
  -raft-dir=<path или пусто для in-memory>
  ```

- В режиме `controller=raft` `/api/cluster` и все операции, меняющие метаданные (`CreateTopic`, назначения партиций, обновления ISR), проходят через Raft‑журнал.
- Режимы работы Raft:
  - in‑memory для тестов/локальной разработки;
  - TCP‑transport для реальных multi-node кластеров с настраиваемым списком peers и таймаутами.

#### 2.2 Репликация, связанная с Broker и контроллером

- `PartitionReplicator` подключён к storage через WAL‑sink:
  - аппендит записи в локальный WAL на follower‑партициях;
  - отслеживает последний применённый offset.
- Менеджер репликации:
  - получает assignment’ы follower‑партиций из `ClusterMetadata` (roles/replicas);
  - для каждой follower‑партиции запускает `PartitionReplicator`, который ходит к leader’у по бинарному протоколу (`BrokerInfo`);
  - использует `ReportReplicaProgress`, чтобы поддерживать ISR:
    - реплика репортит прогресс (last applied offset + leader HighWatermark) в контроллер;
    - контроллер обновляет `ISR` и `Version`.
- Тестами покрыт сценарий single‑leader + follower в одном процессе, проверяются репликация и обновление ISR. Репликация контролируется флагом `-replication`.

#### 2.3 Multi-node Raft‑кластер и multi-broker runtime

- Multi-peer Raft‑кластер контроллера:
  - peers задаются через `RaftBindAddr` и `RaftPeers`;
  - RaftController использует TCP‑transport для прод‑режима и in‑memory transport в тестах.
- Multi-broker‑развёртывание:
  - каждый broker поднимается с уникальным `BrokerID` и общим cluster‑config;
  - брокеры регистрируются в Raft‑контроллере через `RegisterBroker` и получают cluster‑view (leaders/replicas/ISR) через `ClusterMetadata`.
- Осознанность брокера о кластере:
  - при старте брокер открывает только те партиции, которые ему принадлежат (где он лидер или реплика) согласно `ClusterMetadata`;
  - клиентские API (`/api/topics`, `/api/topics/:name`, `/api/topics/:name/partitions/:id/messages`) отражают только те партиции, которые реально обслуживает этот broker.
- Маршрутизация клиентов по лидерам (на уровне дизайна и API):
  - клиенты могут узнавать лидеров через бинарный Metadata‑запрос и HTTP `/api/cluster`;
  - UI показывает несколько брокеров, их роли, ISR и HighWatermark’ы на отдельной странице кластера.
- Операционные сценарии:
  - rolling restart брокеров и нод контроллера;
  - failover лидера (в тестах: после Shutdown лидера выбирается новый и продолжает применять команды);
  - сжатие/расширение ISR и деградированные режимы.
- Hardening:
  - надёжность и совместимость Raft‑snapshot’ов `ClusterMetadata`;
  - `/api/controller` экспонирует состояние контроллера (mode, Raft‑state, term, peers, clusterID, версия метаданных) для операторов;
  - задокументированы процедуры bootstrap/join/rolling restart (см. ниже).

### 3. Тестирование и валидация

#### 3.1 Интеграционные тесты

- In‑process тесты, покрывающие связку broker + storage + controller + HTTP:
  - жизненный цикл топиков/партиций, восстановление после рестарта, консистентность `/api/topics` и `/api/cluster`.
- Тесты кластерного слоя:
  - statically multi-broker layout’ы, применение команд RaftController, обновление ISR, проверка того, что брокер открывает только «свои» партиции.

#### 3.2 End-to-End (E2E) тесты

- Сквозные сценарии с реальными бинарями (или docker‑compose):
  - запуск broker+UI, создание топиков, produce/consume через CLI, HTTP и MQTT;
  - проверка метрик, health‑чеков и поведения UI end‑to‑end.
- Для multi-broker:
  - сценарии с несколькими брокерами и Raft‑контроллером, включая простые случаи failover.

#### 3.3 Большое E2E‑тестирование (перебор сценариев)

- Скрипт «большого теста», гоняющий множество рандомизированных сценариев для поиска краевых случаев:
  - случайное создание топиков/партиций, join/leave consumer‑групп, produce/fetch, рестарты;
  - инварианты: нет потерянных acknowledged‑сообщений, offset’ы монотонны per partition, ISR никогда не пустой и т.п.
- Тест рассчитан на длительный прогон и большое покрытие сочетаний, близок к системному fuzzing’у.

#### 3.4 Стресс‑ и нагрузочные тесты

- Микробенчмарки для storage и горячих путей брокера:

  ```sh
  go test ./internal/storage -bench=.
  go test ./internal/broker -bench=.
  ```

- Нагрузочные тесты для бинарного протокола и MQTT:
  - высокие скорости сообщений, разные размеры payload’ов, несколько concurrent producers/consumers;
  - измерение throughput, latency percentiles и использования ресурсов.
- Для будущих multi-broker сценариев:
  - неравномерное распределение нагрузки, падение брокеров/лидеров под нагрузкой, churn ISR под давлением.

## Сборка и запуск

```sh
go build ./cmd/mbd
go build ./cmd/mbctl
```

Запуск брокера:

```sh
./mbd -data-dir=./data -bind=:7912 -mqtt=:1883 -http=:8090
```

Создание топика и produce/fetch через CLI:

```sh
./mbctl create-topic -topic test -partitions 1
./mbctl produce -topic test -partition 0 -value "hello"
./mbctl fetch -topic test -partition 0 -offset 0
```

MQTT: подключите MQTT 3.1.1/5.0‑клиент к `:1883`, сделайте SUBSCRIBE на топик и PUBLISH сообщений (QoS0/1). MQTT‑топики напрямую маппятся на broker‑topics, партиция выбирается по hash’у.

## Наблюдаемость

HTTP‑эндпоинты (по умолчанию `:8090`):

- `/metrics` — Prometheus‑метрики.
- `/healthz` — probe готовности.
- `/debug/pprof/*` — pprof‑хендлеры.

Пример:

```sh
curl http://localhost:8090/metrics
```

### Пример двух брокеров с Raft

Запуск двух брокеров, разделяющих один Raft‑кластер контроллера:

Broker 1:

```sh
./mbd \
  -broker-id=1 \
  -controller=raft \
  -raft-bind=127.0.0.1:9001 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
  -data-dir=./data1 \
  -bind=:7912 -http=:8091
```

Broker 2:

```sh
./mbd \
  -broker-id=2 \
  -controller=raft \
  -raft-bind=127.0.0.1:9002 \
  -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
  -data-dir=./data2 \
  -bind=:8912 -http=:8092
```

Ожидаемое поведение:

- одна из нод контроллера становится лидером;
- `/api/controller` на обоих брокерах показывает `mode="raft"`, актуальные `raftState`/`term`, список peers, `clusterID` и версию метаданных;
- `/api/cluster` на обоих брокерах сходится к одинаковому `ClusterMetadata`, а лидеры партиций распределены по BrokerID.

#### Операция кластера Raft

- Bootstrap:
  - запустить первый broker с полным списком `-raft-peer`;
  - убедиться, что `/api/controller` показывает лидера и корректный `clusterID`.
- Join:
  - запускать дополнительные брокеры с тем же списком `-raft-peer`;
  - убедиться, что они появляются в peers `/api/controller` и в brokers `/api/cluster`.
- Rolling restart:
  - перезапускать брокеры по одному;
  - после каждого рестарта проверять `/api/controller`, что лидер есть, а версия метаданных (`version`) продолжает расти.

## Docker Compose (broker + UI)

В репозитории есть `docker-compose.yml`, который поднимает broker и UI (`wave-ui`):

```sh
docker compose up --build
```

Порты:

- broker: `7912` (binary), `1883` (MQTT), `8090` (HTTP/metrics)
- UI: `8080` (nginx со статическими файлами, собранными Vite)

Данные брокера лежат в volume `wave_data`. UI можно собрать с `VITE_USE_MOCKS=false`, чтобы ходить в реальный HTTP API по `http://broker:8090`.

## Benchmarks и нагрузка

Микробенчмарки:

```sh
go test ./internal/storage -bench=.
go test ./internal/broker -bench=.
```

Простой генератор нагрузки для бинарного протокола:

```sh
go run ./cmd/mbbench -broker 127.0.0.1:7912 -topic bench -messages 20000 -value-size 200 -concurrency 8
```

Выводит количество произведённых сообщений, среднюю/максимальную задержку, общее время и RPS.

## Docker

Сборка образа:

```sh
docker build -t wavemq:latest .
```

Запуск single-node брокера:

```sh
docker run --rm \
  -p 7912:7912 -p 1883:1883 -p 8090:8090 \
  -v /path/on/host/data:/data \
  wavemq:latest
```

По умолчанию: `-data-dir=/data -bind=:7912 -mqtt=:1883 -http=:8090`. Флаги можно переопределить:

```sh
docker run wavemq:latest <flags>...
```

Доступ:

- Бинарный протокол: `localhost:7912` (используйте `mbctl` на хосте)
- MQTT: `localhost:1883`
- Наблюдаемость: `http://localhost:8090/metrics` и `/healthz`

## Ограничения

- Примеры в README ориентированы на single-node; multi-broker/Raft‑режим доступен через флаги (`-controller=raft`, `-raft-bind`, `-raft-peer`) и описан выше.
- Координация consumer groups локальная; offsets persist’ятся через WAL для offset’ов.
- MQTT реализован минимально (QoS0/1, без retained/will/shared‑подписок).
- Storage использует сегментированный WAL со sparse‑индексом и retention по размеру/времени.

