# Concord

A distributed key-value store built on Raft consensus, providing strong consistency and fault tolerance across nodes.

Concord replicates data across multiple machines and keeps working correctly even when some of them crash or the network between them misbehaves. It implements the [Raft consensus algorithm](https://raft.github.io/raft.pdf) from scratch: leader election, log replication, and safety, as the foundation for a small, understandable distributed KV store, in the spirit of systems like etcd.

---

## Table of Contents

- [Why Raft, why this project](#why-raft-why-this-project)
- [Features](#features)
- [Quick start](#quick-start)
- [Architecture overview](#architecture-overview)
- [Package-by-package breakdown](#package-by-package-breakdown)
- [How a node starts up](#how-a-node-starts-up)
- [How leader election works](#how-leader-election-works)
- [How log replication works](#how-log-replication-works)
- [The networking layer](#the-networking-layer)
- [HTTP API reference](#http-api-reference)
- [Provisioning on a VPS](#provisioning-on-a-vps)
- [Design decisions and why](#design-decisions-and-why)
- [Roadmap](#roadmap)

---

## Why Raft, why this project

A single machine holding data is simple, but fragile: if it dies, the data (or the service) goes down with it. Replicating data across multiple machines fixes durability, but creates a harder problem: how do all those machines agree on what the data actually is, especially when nodes can crash, restart, or get cut off from each other at any moment?

**Consensus algorithms** solve exactly this: they let a group of machines agree on a single, ordered sequence of operations, even under failure, with no single point of failure. **Raft** is a consensus algorithm designed specifically to be more understandable than its predecessor, Paxos, by splitting the problem into three parts:

1. **Leader election**: the cluster always has (at most) one leader, elected by majority vote, so there's a single source of truth for ordering writes.
2. **Log replication**: the leader takes writes, appends them to a log, and replicates that log to followers. An entry is only considered durable once a **majority** of nodes have it.
3. **Safety**: a set of rules (term numbers, log up-to-date checks, commit rules) that guarantee the cluster never loses a committed write or ends up with two conflicting leaders.

Concord implements all three from the ground up, providing a clean, production-grade consensus foundation.

---

## Features

- ✅ **Full Raft consensus**: leader election, log replication, safety invariants
- ✅ **HTTP API**: `GET`/`PUT`/`DELETE` keys via a clean REST API
- ✅ **Leader forwarding**: writes to any node are automatically routed to the leader
- ✅ **FSM (Finite State Machine)**: committed log entries are applied to the KV store
- ✅ **Persistence**: periodic JSON snapshots — nodes survive restarts
- ✅ **Docker & Compose**: one command spins up a 3-node cluster anywhere
- ✅ **Provisionable**: works on localhost, VPS, or any machine with Docker or Go installed

## Installation

### Option 1: One-Line Install Script (Recommended for Linux/macOS)

Install the `concord` binary directly into `/usr/local/bin` with a single command (similar to `etcd` or `helm`):

```bash
curl -fsSL https://raw.githubusercontent.com/Faithful001/concord/main/install.sh | sh
```

### Option 2: via `go install`

If you have Go installed, install `concord` directly to your `$GOPATH/bin`:

```bash
go install github.com/Faithful001/concord.git/cmd/concord@latest
```

---

## Quick start

### Option 1: Docker Compose

```bash
git clone https://github.com/Faithful001/concord.git
cd concord
docker compose up --build
```

This starts a 3-node cluster. Within a second, one node wins the election.

#### Using `cordctl` CLI (Recommended)

```bash
# Build CLI
go build -o bin/cordctl ./cmd/cordctl

# Write a key
./bin/cordctl put hello world

# Read it back (locally or with linearizable consistency)
./bin/cordctl get hello
./bin/cordctl get hello -l

# Check cluster status and leader info
./bin/cordctl status

# Delete a key
./bin/cordctl del hello

# Manage cluster membership (voting nodes and read-only learner replicas)
./bin/cordctl member list
./bin/cordctl member add node-4 localhost:8004 localhost:9004
./bin/cordctl member add node-5 localhost:8005 localhost:9005 --learner
./bin/cordctl member remove node-4
```

#### Using `curl`

```bash
# Write a key
curl -X PUT http://localhost:9001/v1/kv/hello \
     -H "Content-Type: application/json" \
     -d '{"value":"world"}'

# Read it back (from any node — even a follower)
curl http://localhost:9002/v1/kv/hello

# Check cluster status
curl http://localhost:9001/v1/status

# Delete a key
curl -X DELETE http://localhost:9001/v1/kv/hello
```

### Option 2: Local (3 terminals)

```bash
go build -o bin/concord ./cmd/concord

# Terminal 1
./bin/concord -id node-1 -addr localhost:8001 -api-addr localhost:9001 \
  -peers node-2=localhost:8002,node-3=localhost:8003 \
  -api-peers node-2=localhost:9002,node-3=localhost:9003 \
  -data-dir data/node-1

# Terminal 2
./bin/concord -id node-2 -addr localhost:8002 -api-addr localhost:9002 \
  -peers node-1=localhost:8001,node-3=localhost:8003 \
  -api-peers node-1=localhost:9001,node-3=localhost:9003 \
  -data-dir data/node-2

# Terminal 3
./bin/concord -id node-3 -addr localhost:8003 -api-addr localhost:9003 \
  -peers node-1=localhost:8001,node-2=localhost:8002 \
  -api-peers node-1=localhost:9001,node-2=localhost:9002 \
  -data-dir data/node-3
```

Or use the Makefile shortcuts: `make run-node1`, `make run-node2`, `make run-node3`.

### Option 3: go install

```bash
go install github.com/Faithful001/concord.git/cmd/concord@latest
```

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────┐
│                         cmd/concord                         │
│              (entry point: parses flags, wires              │
│               everything together, starts the node)         │
└────────────────────────────┬────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌────────────────┐   ┌────────────────┐   ┌───────────────────┐
│    pkg/raft    │   │  internal/rpc  │   │ internal/transport│
│                │   │                │   │                   │
│ Pure consensus │   │ Adapts Node to │   │ Client-side       │
│ engine. Knows  │   │ gRPC service   │   │ (dial + call) and │
│ nothing about  │   │ definition in  │   │ server-side       │
│ networking.    │   │ pkg/raftpb.    │   │ gRPC over HTTP/2. │
└───────┬────────┘   └────────┬───────┘   └─────────┬─────────┘
        │                     │                     │
        │                     └──────────┬──────────┘
        │                                │
        ▼                                ▼
┌─────────────────┐            (nodes talk to each other
│ internal/storage│            over real TCP connections)
│                 │
│ The actual KV   │
│ data (map +     │
│ mutex).         │
└────────┬────────┘
         │
         ▲
┌────────┴────────┐           ┌───────────────────┐
│ internal/fsm    │           │ internal/api      │
│                 │           │                   │
│ Applies commit- │           │ HTTP REST API.    │
│ ted log entries ├──────────▶│ Leader forwarding │
│ to the KV store │           │ built in.         │
└─────────────────┘           └───────────────────┘
```

**The core design principle:** `pkg/raft` is completely decoupled from networking (just like `etcd/raft`). It defines a `Transport` interface (just two methods: send a vote request, send an append-entries request) and depends only on that interface, never on `net/rpc`, TCP, or any concrete networking detail. This is what let the project be built and tested with an in-memory fake transport first, before real networking existed, and what lets external Go applications import `pkg/raft` to build their own distributed state machines.

---

## Package-by-package breakdown

### `cmd/concord/`

The standalone server entry point. Parses command-line flags (`-id`, `-addr`, `-api-addr`, `-peers`, `-api-peers`, `-data-dir`), constructs a `Node` and its `Transport`, restores any saved snapshot, starts the FSM goroutine and periodic snapshot saver, starts the HTTP API server, and blocks forever.

### `cmd/cordctl/`

The standalone CLI client utility (in the spirit of `etcdctl`). Interacts directly with the Concord cluster over HTTP to `put`, `get`, `del`, check `status`, and test `health`.

### `pkg/raft/`

The exported, reusable Raft consensus engine with zero networking dependencies. External Go applications can import `github.com/Faithful001/concord.git/pkg/raft` to embed Raft into custom applications.

- **`node.go`**: the `Node` struct: all persistent state (`currentTerm`, `votedFor`, `log`), volatile state (`commitIndex`, `lastApplied`), snapshot metadata (`lastIncludedIndex`, `lastIncludedTerm`), and leader-only state (`nextIndex`, `matchIndex`). Also: `Submit()` (leader appends a command and waits for commit), `TakeSnapshot()` / `RestoreSnapshot()`, `ApplyCh()`, the commit waiter system, and the election timer goroutine.
- **`role.go`**: the `Role` type (`Follower`, `Candidate`, `Leader`), a string-based enum for readable logging.
- **`election.go`**: `RequestVote` (the vote-granting handler), `startElection`/`becomeLeader` (the candidate-side logic), `sendHeartbeats` (the leader's replication loop), `replicateToPeer` (sends missing entries + handles matchIndex), and `maybeAdvanceCommitIndex` (the commit-point calculation).
- **`replication.go`**: `AppendEntries` (the follower-side consistency check, conflict detection/log truncation, and commit-index advancement with FSM notification).
- **`log.go`**: the `LogEntry` type: `Term`, `Index`, and an opaque `Command []byte` (Raft never interprets the command itself, as that's the FSM's job).
- **`transport.go`**: the `Transport` interface that `raft` depends on but never implements.
- **`apply.go`**: `ApplyMsg` (the message type sent to the FSM) and `SnapshotState` (serialisable persistent state).
- **`messages.go`**: `RequestVoteArgs`/`Reply` and `AppendEntriesArgs`/`Reply` message types.

### `pkg/raftpb/`

Generated Protocol Buffer (`proto/raft.proto`) and gRPC Go bindings (`raft.pb.go`, `raft_grpc.pb.go`) for type-safe, cross-language Raft RPC serialization.

### `internal/rpc/`

The adapter between `raft.Node` and the gRPC `RaftServiceServer` interface defined in `pkg/raftpb`.

### `internal/transport/`

The real, gRPC network-based implementation of `raft.Transport`.

- **`rpc_transport.go`**: `RPCTransport`, the **client** side: manages pooled `grpc.ClientConn` connections to peers and executes remote calls (`SendRequestVote`, `SendAppendEntries`).
- **`server.go`**: `Serve`, the **server** side: opens a TCP listener, registers `RaftServiceServer` on a `grpc.Server`, and accepts incoming gRPC requests. Every node runs both client and server.

### `internal/storage/`

A plain, thread-safe, in-memory key-value store (`Store`), independent of Raft entirely. `Get`, `Set`, `Delete`, backed by a `map[string][]byte` and a `sync.RWMutex`. Returns a sentinel `ErrKeyNotFound` for missing keys, so callers can check with `errors.Is`.

### `internal/command/`

Encodes and decodes the opaque `Command []byte` stored inside each Raft `LogEntry`. The Raft layer never interprets these bytes; the FSM decodes them to decide what to apply. Supports `SET` (opcode `0x01`, key + value) and `DELETE` (opcode `0x02`, key only).

### `internal/fsm/`

The glue between a committed Raft log entry and the actual `storage.Store`: decoding `LogEntry.Command` via the `command` package and applying the resulting `SET` or `DELETE` operation. Runs as a goroutine reading from the node's `applyCh`. Also supports direct `ApplyEntry()` calls for snapshot replay at startup.

### `internal/persist/`

Simple JSON snapshot persistence. Atomically saves `{currentTerm, votedFor, log[], commitIndex}` to a `.snap` file using a temp-file + rename pattern. On startup, loads the snapshot if it exists (returns `nil` for fresh nodes). Snapshots are taken every 30 seconds by default.

### `internal/api/`

The client-facing HTTP API. Exposes `GET /v1/kv/{key}`, `PUT /v1/kv/{key}`, `DELETE /v1/kv/{key}`, `GET /v1/status`, and `GET /healthz`. Writes check if this node is the leader: if yes, they call `node.Submit()` directly; if no, they reverse-proxy the request to the leader's API address.

---

## How a node starts up

1. `main.go` parses `-id`, `-addr`, `-api-addr`, `-peers`, `-api-peers`, and `-data-dir` from the command line.
2. It attempts to load a snapshot from `<data-dir>/<id>.snap`. If found, it restores the Raft state and replays all committed log entries into the FSM directly (so the in-memory KV store reflects pre-crash state).
3. It builds an `RPCTransport`, seeded with a map of peer ID → address.
4. It constructs a `Node` via `raft.NewNode(id, peerIDs, transport)`: the node starts as a `Follower`.
5. It starts the FSM goroutine (reads from `node.ApplyCh()`) and the periodic snapshot goroutine (every 30 seconds).
6. It launches `transport.Serve(node, addr)` in its own goroutine, which opens a TCP listener and blocks forever, accepting incoming RPCs from peers.
7. If `-api-addr` is set, it starts the HTTP API server.
8. It calls `node.Start()`, which launches `electionTimerLoop()` in its own goroutine, which is what will eventually trigger an election if no leader is heard from.
9. `main()` itself blocks forever on an empty `select {}`, keeping the process alive while the background goroutines do the real work.

At this point, every node in the cluster has an election timer running, counting down a random duration (150-300ms): the first one to time out (since no leader exists yet) will become a candidate.

---

## How leader election works

1. A node's election timer fires with no heartbeat received in that window → it calls `startElection()`.
2. It increments its own `currentTerm`, transitions to `Candidate`, votes for itself, and sends a `RequestVoteArgs` (containing its new term and its log's last index/term) to every peer, in parallel, over the real transport.
3. Each peer's `RequestVote` handler checks, in order:
   - Is the candidate's term stale? Reject.
   - Is the candidate's term newer? Catch up (`currentTerm = args.Term`), reset `votedFor`.
   - **Is the candidate's log at least as up-to-date as mine?** (Compare `LastLogTerm` first, then `LastLogIndex` as a tiebreaker.) If not, reject: this is the safety rule that prevents a node with stale data from ever becoming leader.
   - Have I already voted for someone else this term? If not (or if it was this same candidate), grant the vote.
4. The candidate counts replies as they arrive. If a majority grant their vote, it calls `becomeLeader()`, initializing `nextIndex`/`matchIndex` for every peer and launching a heartbeat loop (`sendHeartbeats`).
5. If any reply reveals a higher term than the candidate's own, it immediately steps down to `Follower`, as someone else is further along.

Randomized timeouts (a fresh random value chosen every time the timer restarts) are what prevent every follower from timing out simultaneously and splitting the vote forever.

---

## How log replication works

1. When a client sends a `PUT` or `DELETE` to the leader's API, the handler calls `node.Submit(cmd)`, which appends the command to the leader's log and blocks until it commits.
2. The leader's `sendHeartbeats` loop ticks every 75ms. On each tick, `replicateToPeer` sends an `AppendEntries` to every peer carrying any log entries that peer is missing (based on `nextIndex`). If the peer is caught up, it sends an empty heartbeat.
3. Each follower's `AppendEntries` handler:
   - Rejects if the leader's term is stale.
   - Catches up its own term if the leader's is newer, stores the `leaderID`, and resets its election timer (proof the leader is alive).
   - Runs a **consistency check**: does it have an entry at `PrevLogIndex` matching `PrevLogTerm`? If not, reject: the leader will back up `nextIndex` and retry with earlier entries.
   - For each new entry: if there's a **conflicting** entry already at that index (same index, different term, evidence of an old, abandoned leader's uncommitted writes), truncate the log from that point and take the leader's version. If the entry is new, append it. If it's already present and matches, skip it.
   - Advances its own `commitIndex` to match the leader's, capped at what it's actually received so far. Newly committed entries are sent to the FSM via `applyCh`.
4. On the leader side, a successful reply advances `matchIndex` for that peer. `maybeAdvanceCommitIndex` then scans for the highest N > commitIndex where `log[N].Term == currentTerm` and a majority have `matchIndex >= N`. When found, `commitIndex` advances to N, the FSM receives the entries, and any `Submit()` callers waiting on those indices are unblocked.

---

## The networking layer

Concord uses **gRPC** and **Protocol Buffers (proto3)** over HTTP/2 TCP sockets:

- **`proto/raft.proto`** defines the canonical Raft service contract (`RequestVote`, `AppendEntries`) and message types (`LogEntryProto`, `RequestVoteRequest`, `AppendEntriesRequest`).
- **`pkg/raftpb`** contains the generated Go protobuf structs and gRPC client/server interfaces.
- On the server side, **`grpc.NewServer()`** registers `rpc.RPCService` and listens for incoming gRPC calls via **`transport.Serve(node, addr)`**.
- On the client side, **`RPCTransport`** maintains pooled connections (`grpc.ClientConn`) to each peer and calls `RaftServiceClient.RequestVote` / `AppendEntries`.

Every node in the cluster runs **both** a server (via `transport.Serve`, so peers can reach it) and a client (via `RPCTransport`, so it can reach peers).

---

## HTTP API reference

All endpoints return JSON (except `/healthz`).

### `PUT /v1/kv/{key}`

Set a key.

```bash
curl -X PUT http://localhost:9001/v1/kv/mykey \
     -H "Content-Type: application/json" \
     -d '{"value":"myvalue"}'
```

**Response (200 OK):**
```json
{"key":"mykey","value":"myvalue"}
```

### `GET /v1/kv/{key}`

Read a key.

```bash
curl http://localhost:9001/v1/kv/mykey
```

**Response (200 OK):**
```json
{"key":"mykey","value":"myvalue"}
```

**Response (404 Not Found):**
```json
{"error":"key not found"}
```

### `DELETE /v1/kv/{key}`

Delete a key.

```bash
curl -X DELETE http://localhost:9001/v1/kv/mykey
```

**Response: 204 No Content**

### `GET /v1/status`

Node status and leader info.

```bash
curl http://localhost:9001/v1/status
```

**Response (200 OK):**
```json
{"node_id":"node-1","role":"leader","leader_id":"node-1"}
```

### `GET /healthz`

Liveness probe.

**Response: 200 OK** with body `ok`

### `GET /v1/members`

List all cluster members.

```bash
curl http://localhost:9001/v1/members
```

**Response (200 OK):**
```json
[
  {"id":"node-1","raft_addr":"localhost:8001","api_addr":"localhost:9001"},
  {"id":"node-2","raft_addr":"localhost:8002","api_addr":"localhost:9002"},
  {"id":"node-3","raft_addr":"localhost:8003","api_addr":"localhost:9003"}
]
```

### `POST /v1/members`

Add a new node to the cluster at runtime (replicated via consensus).

```bash
curl -X POST http://localhost:9001/v1/members \
     -H "Content-Type: application/json" \
     -d '{"id":"node-4","raft_addr":"localhost:8004","api_addr":"localhost:9004"}'
```

**Response: 200 OK**

### `DELETE /v1/members/{id}`

Remove a node from the cluster at runtime.

```bash
curl -X DELETE http://localhost:9001/v1/members/node-4
```

**Response: 200 OK**

### Leader forwarding

Writes sent to a follower are automatically reverse-proxied to the current leader. You can safely send writes to **any** node in the cluster. Reads are served locally from any node (may be up to one heartbeat interval stale on followers).

---

## Command-line flags

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `-id` | Yes | — | Unique node ID (e.g. `node-1`) |
| `-addr` | Yes | — | Raft TCP listen address (e.g. `localhost:8001`) |
| `-api-addr` | No | — | HTTP API listen address (e.g. `localhost:9001`). Omit for consensus-only mode. |
| `-peers` | No | — | Comma-separated `id=host:port` Raft peer addresses |
| `-api-peers` | No | — | Comma-separated `id=host:port` API peer addresses (needed for leader forwarding) |
| `-data-dir` | No | `.` | Directory for snapshot files |
| `-learner` | No | `false` | Run node as a non-voting, read-only learner replica |

---

## Provisioning on a VPS

### With Docker (recommended)

```bash
# On your VPS:
git clone https://github.com/Faithful001/concord.git
cd concord
docker compose up --build -d

# Done. Your cluster is running.
docker compose logs -f    # watch logs
docker compose down       # stop
docker compose down -v    # stop + wipe data
```

### Without Docker

```bash
# Build
go build -o concord ./cmd/concord

# Run each node (replace IPs with your actual VPS addresses)
./concord -id node-1 -addr 10.0.0.1:8001 -api-addr 10.0.0.1:9001 \
  -peers node-2=10.0.0.2:8002,node-3=10.0.0.3:8003 \
  -api-peers node-2=10.0.0.2:9002,node-3=10.0.0.3:9003 \
  -data-dir /var/lib/concord
```

### With systemd

Create `/etc/systemd/system/concord.service`:

```ini
[Unit]
Description=Concord distributed KV store
After=network.target

[Service]
ExecStart=/usr/local/bin/concord \
  -id node-1 \
  -addr :8001 \
  -api-addr :9001 \
  -peers node-2=10.0.0.2:8002,node-3=10.0.0.3:8003 \
  -api-peers node-2=10.0.0.2:9002,node-3=10.0.0.3:9003 \
  -data-dir /var/lib/concord
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now concord
```

---

## Design decisions and why

A few choices worth explaining, since they weren't the only options:

- **`Transport` is an interface, satisfied by `RPCTransport` (gRPC) and in-memory mock transports for testing.** This lets election and replication logic remain completely independent of networking details.
- **gRPC and Protocol Buffers for inter-node communication.** Provides strongly-typed RPC schemas (`proto/raft.proto`), high performance over HTTP/2, connection pooling, and cross-language interoperability.
- **`internal/` for private server internals, `pkg/` for public libraries.** Go's `internal/` convention encapsulates Concord-specific server components, while `pkg/raft` and `pkg/raftpb` provide clean public packages that can be imported by third-party Go projects.
- **JSON snapshots over WAL.** Simple, correct, easy to debug (you can read the snapshot file). The tradeoff is that the full log is serialised every snapshot cycle, but for the scale Concord targets this is fast enough. A proper WAL is a straightforward upgrade path.
- **Leader forwarding via HTTP reverse-proxy.** Simpler than client-side leader discovery + retry logic. Any node in the cluster can accept any request.

---

## Roadmap

### Future improvements (not yet scheduled)

- [x] Export standalone Raft consensus engine (`pkg/raft`) for embedding in custom applications
- [x] Standalone CLI client tool (`cmd/cordctl`) for managing clusters
- [x] Snapshotting and log compaction (so the log doesn't grow forever)
- [x] Cluster membership changes (adding/removing nodes while running)
- [x] Switching `net/rpc` for gRPC (cross-language compatibility, better tooling)
- [x] Read-only replica support / linearizable read optimizations
- [ ] Full write-ahead log (WAL) for crash recovery without full-log snapshots

---

## References

This implementation adheres to the specification detailed in the [Raft paper](https://raft.github.io/raft.pdf) ("In Search of an Understandable Consensus Algorithm" by Diego Ongaro and John Ousterhout). Protocol state transitions and safety invariants were verified against the official [Raft visualization](https://raft.github.io/).
