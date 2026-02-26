# Examples: Python and MATLAB MQTT clients

These examples target **single-node** `wave-mq` and use the broker MQTT endpoint (`:1883`).

## 1. Start broker (single-node)

From `wave-mq`:

```sh
go build ./cmd/mbd
./mbd -data-dir=./data -bind=:7912 -mqtt=:1883 -http=:8090 -controller=single -replication=false
```

## 2. Python consumer/producer (no external dependencies)

From `wave-mq/examples/python`:

Start consumer first (tail mode):

```sh
python mqtt_consumer.py --topic demo.mqtt --max-messages 5
```

Then publish:

```sh
python mqtt_producer.py --topic demo.mqtt --count 5
```

Notes:

- `mqtt_producer.py` auto-creates topic via HTTP API (`POST /api/topics`) unless `--no-auto-create` is used.
- `mqtt_consumer.py` starts from tail by default (`clean_session=true`); use `--resume` to continue from committed offsets.

## 3. MATLAB

Open and run:

```matlab
wave-mq/examples/matlab/mqtt_demo.m
```

The script connects to `127.0.0.1:1883`, subscribes to `demo.mqtt`, publishes one test message, and waits for incoming data.

## 4. Python blackbox suite (large e2e)

`examples/python/blackbox_suite.py` runs broad blackbox checks:

- control-plane endpoints (`/healthz`, `/api/*`);
- heavy HTTP produce/fetch on many partitions;
- MQTT QoS0/QoS1 pub/sub and consumer groups;
- restart persistence (data + committed offsets);
- parallel stress writers;
- metrics and summary consistency.

### Run with Docker (recommended)

From `wave-mq/examples/python`:

```sh
python blackbox_suite.py --profile full
```

Profiles:

- `smoke` - quick check
- `full` - default broad run
- `massive` - heavy load

Useful flags:

```sh
python blackbox_suite.py --profile massive --keep-stack
python blackbox_suite.py --profile full --skip-restart
python blackbox_suite.py --profile smoke --http-port 28090 --mqtt-port 21883 --binary-port 27912
```

The script uses `docker-compose.blackbox.yml` and maps ports to host defaults:

- HTTP `18090`
- MQTT `11883`
- Binary `17912` (reserved by compose, not used by suite directly)

### Run against already running broker (without Docker orchestration)

```sh
python blackbox_suite.py --no-docker --http-port 8090 --mqtt-port 1883 --skip-restart
```
