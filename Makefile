BINARY := concord
CLI_BINARY := cordctl
BIN_DIR := bin
DATA_DIR := data

.PHONY: all build build-server build-ctl proto test fmt vet clean \
        run-node1 run-node2 run-node3 \
        docker-build docker-up docker-down docker-logs

# ─── Local development ────────────────────────────────────────────────────────

all: build

## proto: generate protobuf Go code
proto:
	protoc --go_out=. --go_opt=module=github.com/Faithful001/concord.git --go-grpc_out=. --go-grpc_opt=module=github.com/Faithful001/concord.git proto/raft.proto

## build: compile concord server and cordctl CLI
build: build-server build-ctl

build-server:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY) ./cmd/concord

build-ctl:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(CLI_BINARY) ./cmd/cordctl


## test: run all tests
test:
	go test -v -race ./...

## fmt: format source code
fmt:
	gofmt -w .

## vet: run go vet
vet:
	go vet ./...

## clean: remove build artefacts and local data directories
clean:
	rm -rf $(BIN_DIR) $(DATA_DIR)

# ─── Local 3-node cluster (3 terminals) ──────────────────────────────────────
# Run each in a separate terminal or use docker-up for a single-command setup.

run-node1: build
	@mkdir -p $(DATA_DIR)/node-1
	./$(BIN_DIR)/$(BINARY) \
		-id node-1 \
		-addr localhost:8001 \
		-api-addr localhost:9001 \
		-peers node-2=localhost:8002,node-3=localhost:8003 \
		-api-peers node-2=localhost:9002,node-3=localhost:9003 \
		-data-dir $(DATA_DIR)/node-1

run-node2: build
	@mkdir -p $(DATA_DIR)/node-2
	./$(BIN_DIR)/$(BINARY) \
		-id node-2 \
		-addr localhost:8002 \
		-api-addr localhost:9002 \
		-peers node-1=localhost:8001,node-3=localhost:8003 \
		-api-peers node-1=localhost:9001,node-3=localhost:9003 \
		-data-dir $(DATA_DIR)/node-2

run-node3: build
	@mkdir -p $(DATA_DIR)/node-3
	./$(BIN_DIR)/$(BINARY) \
		-id node-3 \
		-addr localhost:8003 \
		-api-addr localhost:9003 \
		-peers node-1=localhost:8001,node-2=localhost:8002 \
		-api-peers node-1=localhost:9001,node-2=localhost:9002 \
		-data-dir $(DATA_DIR)/node-3

# ─── Docker ───────────────────────────────────────────────────────────────────

## docker-build: build the Docker image
docker-build:
	docker build -t concord:latest .

## docker-up: build and start the 3-node cluster (detached)
docker-up:
	docker compose up --build -d

## docker-down: stop and remove containers (keeps volumes)
docker-down:
	docker compose down

## docker-logs: tail logs from all nodes
docker-logs:
	docker compose logs -f

# ─── Help ─────────────────────────────────────────────────────────────────────
help:
	@grep -E '^## ' Makefile | sed 's/## /  /'
