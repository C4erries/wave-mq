.PHONY: run test

GO ?= go

run:
	$(GO) run ./cmd/mbd

test:
	$(GO) test ./...

.PHONY: lint
lint:
	@golangci-lint --version && echo "golangci-lint -v run --fix ./..." || echo "golangci-lint not found"
	@golangci-lint -v run --fix ./...
