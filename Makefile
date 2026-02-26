.PHONY: run test lint bb bb-smoke bb-full bb-massive bb-local

GO ?= go
PYTHON ?= python

BB_SCRIPT ?= ./examples/python/blackbox_suite.py
BB_PROFILE ?= full
BB_HTTP_PORT ?= 18090
BB_MQTT_PORT ?= 11883
BB_BINARY_PORT ?= 17912

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
