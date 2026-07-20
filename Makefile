.PHONY: all build test vet clean install

GO ?= go
BIN_DIR ?= bin
PREFIX ?= $(HOME)/.local
INSTALL_BIN_DIR ?= $(PREFIX)/bin

all: test build

build:
	mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/apc ./cmd/apc

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

install: build
	install -d $(DESTDIR)$(INSTALL_BIN_DIR)
	install -m 0755 $(BIN_DIR)/apc $(DESTDIR)$(INSTALL_BIN_DIR)/apc

clean:
	rm -rf $(BIN_DIR)
