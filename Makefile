GO ?= go
.PHONY: test race vet demo evaluate build
test:
	GOFLAGS=-p=2 GOMAXPROCS=2 $(GO) test ./...
race:
	GOFLAGS=-p=2 GOMAXPROCS=2 $(GO) test -race ./...
vet:
	GOFLAGS=-p=2 GOMAXPROCS=2 $(GO) vet ./...
demo:
	$(GO) run ./cmd/quorumlab demo --output artifacts/demo.json
evaluate:
	$(GO) run ./cmd/quorumlab evaluate --output artifacts/evaluation.json
build:
	mkdir -p artifacts
	$(GO) build -o artifacts/quorumlab ./cmd/quorumlab
