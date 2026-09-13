# Tracker Auto-Assigner Makefile

APP_NAME := tracker-assigner
BIN_DIR := bin
CONFIG_PATH ?= config.yaml

.PHONY: all build test test-coverage clean run docker-build docker-run lint

all: test build

build:
	@mkdir -p $(BIN_DIR)
	go build -ldflags="-s -w" -trimpath -o $(BIN_DIR)/$(APP_NAME) ./cmd/tracker-assigner

test:
	go test -v ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

run:
	go run ./cmd/tracker-assigner -config $(CONFIG_PATH)

docker-build:
	docker build -t $(APP_NAME):latest .

docker-run:
	docker run --rm -p 8080:8080 -v $(PWD)/config.yaml:/etc/tracker-assigner/config.yaml:ro $(APP_NAME):latest

clean:
	rm -rf $(BIN_DIR) coverage.out coverage.html *.db *.db-wal *.db-shm
