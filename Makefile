.PHONY: run test

GO ?= go

run:
	$(GO) run ./cmd/mbd

test:
	$(GO) test ./...
