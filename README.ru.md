# wave-mq

Single-node log-based message broker на Go, спроектированный так, чтобы его можно было развить в кластер. Поддерживает кастомный бинарный протокол, минимальный MQTT 3.1.1/5.0 фронтенд (QoS0/1), consumer groups с offset’ами на брокере и сегментированное WAL‑хранилище со sparse‑индексом и retention.

## Состояние проекта

- Основной single-node брокер (storage, бинарный протокол, MQTT, consumer groups, HTTP UI/API) — реализован и подходит для локальных экспериментов и демонстраций.
- Persist метаданных топиков (`metadata.log`) — реализован, топики и партиции поднимаются после рестарта.
- Слой кластерных метаданных (controller + `/api/cluster`) — реализован для single-node и статического multi-broker; Raft‑контроллер существует как экспериментальная альтернатива и пока не подключён к основному бинарю.
- Путь репликации (leader → follower) — реализованы бинарный клиент и PartitionReplicator как прототипы; **не** включены в стандартный runtime, эффективный RF остаётся 1.
- Multi-node / Raft‑кластер контроллера — дизайн и scaffolding есть, производственная интеграция и эксплуатация — будущая работа.

## Дорожная карта

### 1. Основа (текущее состояние)

- Single-node брокер с сегментированным WAL, sparse‑индексом, retention по размеру/времени и crash‑recovery.
- Сервер бинарного протокола (`netproto`), минимальный MQTT‑фронтенд, CLI‑клиент, HTTP admin API + React UI.
- Persist метаданных топиков через `metadata.log`; broker и HTTP API восстанавливают topics/partitions после рестарта.
- Контроллер кластерных метаданных:
  - SingleNodeController с `ClusterMetadata` и снимком `/api/cluster`.
  - Статический multi-broker layout через `StaticClusterConfig` и round-robin лидеров.
- Экспериментальный RaftController:
  - Single-node Raft FSM для `ClusterMetadata` с командами `AssignTopic` и `ReportReplicaProgress`.
  - Snapshot/restore кластерных метаданных, пока не используется в стандартном бинаре.
- Заготовки для репликации:
  - BinaryReplicator (использует существующий Fetch в бинарном протоколе и возвращает записи + HighWatermark).
  - PartitionReplicator с интерфейсом `Sink` и API управления ISR (`ReportReplicaProgress`) в контроллере.
  - Пока не подключены к основному runtime; эффективный RF = 1.

### 2. Multi-broker (развитие)

Этот этап делится на три крупные главы.

#### 2.1 Интеграция Raft‑контроллера в основной путь

- Сделать RaftController drop-in заменой SingleNodeController за существующими интерфейсами контроллера.
- Подключить RaftController в `cmd/mbd` и HTTP:
  - конфиг/флаг для выбора между in‑memory контроллером и Raft‑контроллером;
  - `/api/cluster` и все операции, меняющие метаданные (`CreateTopic`, будущие admin API), проходят через Raft.
- Операционно стабилизировать single-node Raft:
  - частота snapshot’ов, поведение при рестарте, HTTP‑эндпоинт статуса контроллера;
  - убедиться, что всё совместимо с существующими тестами и UI.

#### 2.2 Связка репликатора с Broker и контроллером

- Подключить `PartitionReplicator` к storage:
  - реализовать `Sink`, который аппендит записи в локальный WAL и обновляет HighWatermark партиции;
  - обеспечить идемпотентность и корректное отслеживание offset’ов на follower’е.
- Запускать репликационные циклы для follower‑партиций:
  - получать assignment’ы follower’ов из `ClusterMetadata` (roles/replicas);
  - для каждой follower‑партиции запускать `PartitionReplicator`, нацеленный на leader’а (`BrokerInfo`).
- Использовать `ReportReplicaProgress` для поддержки ISR:
  - реплика репортит прогресс (last applied offset + leader HighWatermark) в контроллер;
  - контроллер обновляет `ISR` и `Version`.
- Тесты и безопасность:
  - сценарий “один leader + follower” в одном процессе, проверка репликации и обновления ISR;
  - флаги/конфиг для включения/выключения репликации в runtime.

#### 2.3 Многоузловой Raft‑кластер и multi-broker runtime

- Переход от single-node Raft к multi-peer Raft‑кластеру контроллера:
  - конфигурация Raft‑пиров (servers) из cluster‑config;
  - сетевой transport между нодами контроллера.
- Multi-broker деплой:
  - каждый broker поднимается с уникальным `BrokerID` и общим cluster‑config;
  - брокеры регистрируются в Raft‑контроллере и получают cluster‑view (leaders/replicas/ISR).
- Маршрутизация клиентов по лидерам:
  - клиенты получают информацию о лидерах через Metadata/HTTP‑API и ходят на нужный broker;
  - UI показывает несколько брокеров, их роли, ISR и HighWatermark’и.
- Операционные сценарии:
  - rolling restart брокеров и нод контроллера;
  - failover лидера, сжатие/расширение ISR, деградированные режимы.
- Укрепление:
  - надёжность и совместимость snapshot’ов Raft для `ClusterMetadata`;
  - тулзы для инспекции состояния кластера и выполнения админ‑операций.

### 3. Тестирование и валидация

#### 3.1 Интеграционные тесты

- In‑process тесты, покрывающие broker + storage + controller + HTTP:
  - жизненный цикл топиков/партиций, restart‑recovery, консистентность `/api/topics` и `/api/cluster`.
- Тесты кластерных метаданных:
  - статические multi-broker layout’ы, применение команд RaftController, обновление ISR.

#### 3.2 End-to-End (E2E) тесты

- Сквозные сценарии с реальными бинарями (или docker‑compose):
  - запуск broker+UI, создание топиков, produce/consume через CLI, HTTP и MQTT;
  - проверка метрик, health‑чеков и поведения UI end‑to‑end.
- Для multi-broker:
  - сценарии с несколькими брокерами и контроллером, включая базовые failover‑случаи.

#### 3.3 Большое E2E‑тестирование (перебор сценариев)

- Скрипт “большого теста”, который гоняет множество рандомизированных сценариев, чтобы находить краевые случаи:
  - случайное создание топиков/партиций, join/leave consumer‑групп, produce/fetch, рестарты;
  - инварианты: нет потерянных acknowledged‑сообщений, offset’ы монотонны per partition, ISR никогда не пустой и т.п.
- Тест рассчитан на длительный прогон и большое покрытие сочетаний, близко к системному fuzzing’у.

#### 3.4 Стресс‑ и нагрузочные тесты

- Микробенчмарки (уже есть) для storage и горячих путей брокера.
- Нагрузочные тесты для бинарного протокола (например, `mbbench`) и MQTT:
  - высокие скорости сообщений, разные размеры payload’ов, несколько concurrent producers/consumers;
  - измерение throughput, latency percentiles и использования ресурсов.
- Для будущего multi-broker:
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

MQTT: подключите любой MQTT 3.1.1/5.0‑клиент к `:1883`, сделайте SUBSCRIBE на топик и PUBLISH сообщений (QoS0/1). MQTT‑топики маппятся на broker topics, партиция выбирается по hash’у.

## Наблюдаемость

HTTP‑эндпоинты (по умолчанию `:8090`):

- `/metrics` — Prometheus‑метрики.
- `/healthz` — probe готовности.
- `/debug/pprof/*` — pprof‑хендлеры.

Пример:

```sh
curl http://localhost:8090/metrics
```

### Пример двух брокеров с общим Raft‑контроллером (экспериментально)

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

Ожидания: один контроллер станет лидером; `/api/cluster` на обоих брокерах со временем покажет одинаковый `ClusterMetadata`, лидеры партиций распределятся по разным BrokerID.

## Docker Compose (broker + UI)

В корне есть `docker-compose.yml`, который поднимает broker + UI (`wave-ui`):

```sh
docker compose up --build
```

Порты:

- broker: `7912` (binary), `1883` (MQTT), `8090` (HTTP/metrics)
- UI: `8080` (nginx со статикой Vite)

Данные broker’а лежат в volume `wave_data`. UI можно собрать с `VITE_USE_MOCKS=false`, чтобы ходить в реальный HTTP API по `http://broker:8090`.

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

Выводит количество сообщений, среднюю/максимальную задержку, общее время и RPS.

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

По умолчанию: `-data-dir=/data -bind=:7912 -mqtt=:1883 -http=:8090`. Флаги можно переопределить: `docker run wavemq:latest <flags>...`.

Доступ:

- Бинарный протокол: `localhost:7912` (используйте `mbctl` на хосте)
- MQTT: `localhost:1883`
- Наблюдаемость: `http://localhost:8090/metrics` и `/healthz`

## Ограничения

- По умолчанию билд остаётся single-node; multi-broker/raft‑контроллер находятся в экспериментальной стадии и не подключены к основному пути инициализации.
- Координация consumer groups локальная; offsets persist’ятся через WAL для offset’ов.
- MQTT реализован минимально (QoS0/1, без retained/will/shared‑подписок).
- Storage использует сегментированный WAL со sparse‑индексом и retention по размеру/времени.
