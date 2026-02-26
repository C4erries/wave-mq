.PHONY: run test lint bb bb-smoke bb-full bb-massive bb-local bb-multi bb-multi-smoke bb-multi-full bb-multi-massive bb-multi-local

GO ?= go
PYTHON ?= python

BB_SCRIPT ?= ./examples/python/blackbox_suite.py
BB_PROFILE ?= full
BB_HTTP_PORT ?= 18090
BB_MQTT_PORT ?= 11883
BB_BINARY_PORT ?= 17912

BB_MULTI_SCRIPT ?= ./examples/python/blackbox_multi_suite.py
BB_MULTI_PROFILE ?= full
BB_MULTI_HTTP_PORT_1 ?= 28091
BB_MULTI_HTTP_PORT_2 ?= 28092
BB_MULTI_BINARY_PORT_1 ?= 27912
BB_MULTI_BINARY_PORT_2 ?= 28912
BB_MULTI_MQTT_PORT_1 ?= 21883
BB_MULTI_MQTT_PORT_2 ?= 22883

run:
	$(GO) run ./cmd/mbd

test:
	$(GO) test ./...

lint:
	@golangci-lint --version && echo "golangci-lint -v run --fix ./..." || echo "golangci-lint not found"
	@golangci-lint -v run --fix ./...

bb:
	$(PYTHON) $(BB_SCRIPT) --profile $(BB_PROFILE) --http-port $(BB_HTTP_PORT) --mqtt-port $(BB_MQTT_PORT) --binary-port $(BB_BINARY_PORT)

bb-smoke:
	$(PYTHON) $(BB_SCRIPT) --profile smoke --http-port $(BB_HTTP_PORT) --mqtt-port $(BB_MQTT_PORT) --binary-port $(BB_BINARY_PORT)

bb-full:
	$(PYTHON) $(BB_SCRIPT) --profile full --http-port $(BB_HTTP_PORT) --mqtt-port $(BB_MQTT_PORT) --binary-port $(BB_BINARY_PORT)

bb-massive:
	$(PYTHON) $(BB_SCRIPT) --profile massive --http-port $(BB_HTTP_PORT) --mqtt-port $(BB_MQTT_PORT) --binary-port $(BB_BINARY_PORT)

bb-local:
	$(PYTHON) $(BB_SCRIPT) --no-docker --skip-restart --profile $(BB_PROFILE) --http-port 8090 --mqtt-port 1883

bb-multi:
	$(PYTHON) $(BB_MULTI_SCRIPT) --profile $(BB_MULTI_PROFILE) --http-port-1 $(BB_MULTI_HTTP_PORT_1) --http-port-2 $(BB_MULTI_HTTP_PORT_2) --binary-port-1 $(BB_MULTI_BINARY_PORT_1) --binary-port-2 $(BB_MULTI_BINARY_PORT_2) --mqtt-port-1 $(BB_MULTI_MQTT_PORT_1) --mqtt-port-2 $(BB_MULTI_MQTT_PORT_2)

bb-multi-smoke:
	$(PYTHON) $(BB_MULTI_SCRIPT) --profile smoke --http-port-1 $(BB_MULTI_HTTP_PORT_1) --http-port-2 $(BB_MULTI_HTTP_PORT_2) --binary-port-1 $(BB_MULTI_BINARY_PORT_1) --binary-port-2 $(BB_MULTI_BINARY_PORT_2) --mqtt-port-1 $(BB_MULTI_MQTT_PORT_1) --mqtt-port-2 $(BB_MULTI_MQTT_PORT_2)

bb-multi-full:
	$(PYTHON) $(BB_MULTI_SCRIPT) --profile full --http-port-1 $(BB_MULTI_HTTP_PORT_1) --http-port-2 $(BB_MULTI_HTTP_PORT_2) --binary-port-1 $(BB_MULTI_BINARY_PORT_1) --binary-port-2 $(BB_MULTI_BINARY_PORT_2) --mqtt-port-1 $(BB_MULTI_MQTT_PORT_1) --mqtt-port-2 $(BB_MULTI_MQTT_PORT_2)

bb-multi-massive:
	$(PYTHON) $(BB_MULTI_SCRIPT) --profile massive --http-port-1 $(BB_MULTI_HTTP_PORT_1) --http-port-2 $(BB_MULTI_HTTP_PORT_2) --binary-port-1 $(BB_MULTI_BINARY_PORT_1) --binary-port-2 $(BB_MULTI_BINARY_PORT_2) --mqtt-port-1 $(BB_MULTI_MQTT_PORT_1) --mqtt-port-2 $(BB_MULTI_MQTT_PORT_2)

bb-multi-local:
	$(PYTHON) $(BB_MULTI_SCRIPT) --no-docker --skip-restart --profile $(BB_MULTI_PROFILE) --http-port-1 8091 --http-port-2 8092
