# wave-mq (русский обзор)

wave-mq — это экспериментальный брокер сообщений с сегментированным WAL/индексами, HTTP/MQTT и CLI-интерфейсами, написанный на Go. Поддерживает single-node режим, экспериментальные Raft-кластеры с RF>1 и MQTT-сервер, при этом контроллер хранит `ClusterMetadata`, брокеры материализуют топики и партиции только из контроллера, а локальный `metadata.log` используется как кеш.

## Статус и ограничения

- **Single-node** решает большинство задач: получите работающий HTTP/MQTT/CLI + UI, сегментированный WAL, consumer groups и наблюдаемость (`/metrics`, `/healthz`).
- **Raft + репликация** (RF>1) — экспериментальный кластер для лабораторий/демо. Клиенты должны писать и читать только через лидера; фолловеры возвращают `ErrNotLeader`/HTTP 409 с `leaderBrokerID`, а `/api/controller` показывает `leader`, `raftState`, `term`, `peers`, `clusterID` и `version`.
- **Метаданные** позволяют отслеживать лидеров, реплики, ISR и прогресс репликации (`wavemq_replication_lag_offsets`, `wavemq_replication_applied_total`).

## Экспериментальный Raft-кластер (quickstart)

1. Соберите бинарники:

   ```sh
   go build ./cmd/mbd
   go build ./cmd/mbctl
   ```

2. Выберите адреса Raft-пиров и диски:

   - `127.0.0.1:9001`, данные `./data1`, HTTP `:8091`, binary `:7912`
   - `127.0.0.1:9002`, данные `./data2`, HTTP `:8092`, binary `:8912`

3. Запустите контроллер-лидер (broker-id=1):

   ```sh
   ./mbd \
     -broker-id=1 \
     -controller=raft \
     -raft-bind=127.0.0.1:9001 \
     -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
     -raft-dir=./data1/raft \
     -data-dir=./data1 \
     -bind=:7912 -http=:8091 -mqtt=:1883 \
     -replication=true
   ```

4. Запустите второго брокера (фолловер):

   ```sh
   ./mbd \
     -broker-id=2 \
     -controller=raft \
     -raft-bind=127.0.0.1:9002 \
     -raft-peer=127.0.0.1:9001,127.0.0.1:9002 \
     -raft-dir=./data2/raft \
     -data-dir=./data2 \
     -bind=:8912 -http=:8092 -mqtt=:1883 \
     -replication=true
   ```

5. Создайте топик с `replicationFactor=2`, публикуйте и читайте через лидера, а затем изучите метаданные:

   - `/api/controller` показывает `mode`, `raftState`, `leader`, `term`, `clusterID`, `version`, `peers`. Фолловеры используют `POST /api/controller/brokers`, чтобы попросить лидера применить `RegisterBroker` за них во время старта.
   - `/api/cluster` содержит список партиций, лидеров, реплик и ISR; помогает диагностировать `leader not elected` и увидеть последствия перезапусков.
   - `/api/topics/<name>` (или метаданные бинарного протокола) указывают, какие брокеры владеют партициями, а ошибки типа `ErrNotLeader`/HTTP 409 содержат подсказку `leaderBrokerID`.

## Docker Compose (Raft-кластер)

`docker compose up --build` поднимает `broker1`, `broker2` и UI на одной сети. Каждый брокер запускается с `-controller=raft`, `-raft-bind=brokerN:9001`, общей строкой `-raft-peer`, `-raft-dir=/data/raft`, `-replication=true` и стандартными флагами `-data-dir`, `-bind`, `-mqtt`, `-http`. Внутри каждый `mbd` ждёт Raft-лидера через `/api/controller`; лидер применяет `RegisterBroker` через Raft, а фолловеры переставляют POST на `http://<leader-host>:<http-port>/api/controller/brokers`, чтобы лидер реплицировал регистрацию.

Смотрите `/api/controller` (поле `leader`, `raftState`, `term`, `peers`, `clusterID`, `version`) и `/api/cluster` (брокеры, партиции, ISR) для диагностики.

## Одноузловый режим

```sh
./mbd \
  -data-dir=./data \
  -bind=:7912 \
  -http=:8090 \
  -mqtt=:1883 \
  -controller=single \
  -replication=false
```

Такой сценарий хорошо подходит для локальной разработки и тестов: `mbctl` создаёт топики, публикует и читает, а контроллер хранит локальные метаданные и не работает через Raft.
