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
