.PHONY: all build test vet generate clean install

GO ?= go
PROTOC ?= protoc
BIN_DIR ?= bin

all: test build

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/apc ./cmd/apc
	$(GO) build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/apc-server ./cmd/apc-server
	$(GO) build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/apc-agent ./cmd/apc-agent

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

generate:
	PATH="$$(go env GOPATH)/bin:$$PATH" $(PROTOC) \
		--go_out=. --go_opt=module=github.com/buberlo/apple-pod-control \
		--go-grpc_out=. --go-grpc_opt=module=github.com/buberlo/apple-pod-control \
		proto/apc/v1/control.proto

install: build
	install -m 0755 $(BIN_DIR)/apc /usr/local/bin/apc
	install -m 0755 $(BIN_DIR)/apc-server /usr/local/bin/apc-server
	install -m 0755 $(BIN_DIR)/apc-agent /usr/local/bin/apc-agent

clean:
	rm -rf $(BIN_DIR)

