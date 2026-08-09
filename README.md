# Concord

A distributed key-value store built on Raft consensus, providing strong consistency and fault tolerance across nodes.

Concord replicates data across multiple machines and keeps working correctly even when some of them crash or the network between them misbehaves. It implements the [Raft consensus algorithm](https://raft.github.io/raft.pdf) from scratch: leader election, log replication, and safety, as the foundation for a small, understandable distributed KV store, in the spirit of systems like etcd.

This project is being built as a learning exercise in distributed systems, and this README tracks both what's actually implemented today and where the project is headed.

---

## Table of Contents

- [Why Raft, why this project](#why-raft-why-this-project)
- [Current status](#current-status)
- [Architecture overview](#architecture-overview)
- [Package-by-package breakdown](#package-by-package-breakdown)
- [How a node starts up](#how-a-node-starts-up)
- [How leader election works](#how-leader-election-works)
- [How log replication works](#how-log-replication-works)
- [The networking layer](#the-networking-layer)
- [Running a cluster today](#running-a-cluster-today)
- [What's missing](#whats-missing)
- [Roadmap](#roadmap)
- [Design decisions and why](#design-decisions-and-why)

---

## Why Raft, why this project

A single machine holding data is simple, but fragile: if it dies, the data (or the service) goes down with it. Replicating data across multiple machines fixes durability, but creates a harder problem: how do all those machines agree on what the data actually is, especially when nodes can crash, restart, or get cut off from each other at any moment?

**Consensus algorithms** solve exactly this: they let a group of machines agree on a single, ordered sequence of operations, even under failure, with no single point of failure. **Raft** is a consensus algorithm designed specifically to be more understandable than its predecessor, Paxos, by splitting the problem into three parts:

1. **Leader election**: the cluster always has (at most) one leader, elected by majority vote, so there's a single source of truth for ordering writes.
2. **Log replication**: the leader takes writes, appends them to a log, and replicates that log to followers. An entry is only considered durable once a **majority** of nodes have it.
3. **Safety**: a set of rules (term numbers, log up-to-date checks, commit rules) that guarantee the cluster never loses a committed write or ends up with two conflicting leaders.

Concord implements all three, from the ground up, as both a way to deeply understand distributed consensus and as a real (if intentionally small) piece of infrastructure.

---

## Current status

**What works today:**

- ✅ In-memory key-value storage (`Get`/`Set`/`Delete`), fully tested
- ✅ Leader election (`RequestVote`), including the log-up-to-date safety check
- ✅ Log replication and consistency checking (`AppendEntries`), including conflict detection and log truncation
- ✅ Randomized election timeouts, with proper reset-on-heartbeat behavior
- ✅ Leader heartbeats, sent on an interval to all peers
- ✅ Real networking: nodes run as separate OS processes, communicating over TCP via Go's `net/rpc`
- ✅ Nodes correctly step down when they discover a higher term (from votes, heartbeats, or replies)

**What's not built yet:**

- ❌ No FSM: committed log entries are not yet applied to the actual key-value store
- ❌ No client-facing API: there's no way for an external caller to actually `SET`/`GET`/`DELETE` a key against the cluster
- ❌ No persistence: everything is in-memory; a node restart loses all state
- ❌ No snapshotting or log compaction
- ❌ No cluster membership changes (adding/removing nodes while running)

In short: **the consensus core works (nodes correctly elect a leader and replicate a log), but there's currently no way to actually store or retrieve real data through it.** See [What's missing](#whats-missing) and [Roadmap](#roadmap) below.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────────┐
│                         cmd/concord                          │
│              (entry point: parses flags, wires              │
│               everything together, starts the node)          │
└───────────────────────────┬───────────────────────────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐   ┌────────────────┐   ┌──────────────────┐
│  internal/raft │   │ internal/rpc   │   │ internal/transport│
│                │   │                │   │                  │
│ Pure consensus │   │ Adapts Node's  │   │ Client-side       │
│ logic. Knows    │   │ methods to the │   │ (dial + call) and │
│ nothing about   │   │ net/rpc calling│   │ server-side       │
│ networking.     │   │ convention.    │   │ (listen + accept) │
│                │   │                │   │ real TCP/RPC.     │
└───────┬────────┘   └────────┬───────┘   └─────────┬──────────┘
        │                     │                     │
        │                     └──────────┬──────────┘
        │                                │
        ▼                                ▼
┌────────────────┐             (nodes talk to each other
│ internal/storage│              over real TCP connections)
│                │
│ The actual KV  │
│ data (map +    │
│ mutex).        │
└────────────────┘
```

**The core design principle:** `internal/raft` is completely decoupled from networking. It defines a `Transport` interface (just two methods: send a vote request, send an append-entries request) and depends only on that interface, never on `net/rpc`, TCP, or any concrete networking detail. This is what let the project be built and tested with an in-memory fake transport first, before real networking existed, and what would let `net/rpc` be swapped for gRPC later without touching a single line of consensus logic.

---

## Package-by-package breakdown

### `cmd/concord/`

The entry point. Parses command-line flags (`-id`, `-addr`, `-peers`), constructs a `Node` and its `Transport`, starts the node's background loops, and blocks forever.

### `internal/raft/`

The heart of the project: Raft consensus, with zero networking knowledge.

- **`node.go`**: the `Node` struct: all persistent state (`currentTerm`, `votedFor`, `log`), volatile state (`commitIndex`, `lastApplied`), and leader-only state (`nextIndex`, `matchIndex`). Also the election timer setup (`Start`, `Stop`, `resetElectionTimeout`) and its background loop (`electionTimerLoop`).
- **`role.go`**: the `Role` type (`Follower`, `Candidate`, `Leader`), a string-based enum for readable logging.
- **`election.go`**: `RequestVote` (the vote-granting handler) and `startElection`/`becomeLeader` (the candidate-side election logic and leader heartbeat startup).
- **`replication.go`**: `AppendEntries`: the consistency check, conflict detection/log truncation, and commit-index advancement.
- **`log.go`**: the `LogEntry` type: `Term`, `Index`, and an opaque `Command []byte` (Raft never interprets the command itself, as that's the FSM's job).
- **`transport.go`**: the `Transport` interface that `raft` depends on but never implements.

### `internal/rpc/`

The adapter between `raft.Node` and Go's `net/rpc` calling convention (`func(args, *reply) error`). `RPCService` wraps a `Node` and exposes `RequestVote`/`AppendEntries` in the shape `net/rpc` requires.

### `internal/transport/`

The real, network-based implementation of `raft.Transport`.

- **`rpc_transport.go`**: `RPCTransport`, the **client** side: dials a peer over TCP and makes an RPC call (`SendRequestVote`, `SendAppendEntries`).
- **`server.go`**: `Serve`, the **server** side: opens a TCP listener, registers the `RPCService`, and accepts incoming RPC connections. Every node runs both sides: each node is simultaneously a client (reaching out to peers) and a server (accepting calls from peers).

### `internal/storage/`

A plain, thread-safe, in-memory key-value store (`Store`), independent of Raft entirely. `Get`, `Set`, `Delete`, backed by a `map[string][]byte` and a `sync.RWMutex`. Returns a sentinel `ErrKeyNotFound` for missing keys, so callers can check with `errors.Is`.

### `internal/fsm/` _(planned, not yet built)_

Will be the glue between a committed Raft log entry and the actual `storage.Store`: decoding `LogEntry.Command` and applying it. See [Roadmap](#roadmap).

### `internal/api/` _(planned, not yet built)_

Will be the client-facing layer: accepting `SET`/`GET`/`DELETE` requests from outside the cluster, forwarding writes to the current leader if needed. See [Roadmap](#roadmap).

---

## How a node starts up

1. `main.go` parses `-id`, `-addr`, and `-peers` from the command line.
2. It builds an `RPCTransport`, seeded with a map of peer ID → address.
3. It constructs a `Node` via `raft.NewNode(id, peerIDs, transport)`: the node starts as a `Follower`, with `currentTerm = 0` and an empty log.
4. It launches `transport.Serve(node, addr)` in its own goroutine, which opens a TCP listener and blocks forever, accepting incoming RPCs from peers.
5. It calls `node.Start()`, which launches `electionTimerLoop()` in its own goroutine, which is what will eventually trigger an election if no leader is heard from.
6. `main()` itself blocks forever on an empty `select {}`, keeping the process alive while the two background goroutines do the real work.

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

_(Note: as of today, replication moves log entries between nodes correctly, but nothing yet applies committed entries to the actual key-value store: see [What's missing](#whats-missing).)_

1. The leader sends `AppendEntries` to every peer, either as a heartbeat (empty `Entries`) on a fixed interval, or carrying real new log entries when there's data to replicate.
2. Each follower's `AppendEntries` handler:
   - Rejects if the leader's term is stale.
   - Catches up its own term if the leader's is newer, and resets its election timer (proof the leader is alive).
   - Runs a **consistency check**: does it have an entry at `PrevLogIndex` matching `PrevLogTerm`? If not, reject: the leader will back up and retry with earlier entries.
   - For each new entry: if there's a **conflicting** entry already at that index (same index, different term, evidence of an old, abandoned leader's uncommitted writes), truncate the log from that point and take the leader's version. If the entry is new, append it. If it's already present and matches, skip it.
   - Advances its own `commitIndex` to match the leader's, capped at what it's actually received so far.
3. The leader considers an entry committed once a majority of followers have acknowledged it (tracked via `matchIndex`).

---

## The networking layer

Concord uses Go's built-in `net/rpc` package over raw TCP: no protobuf or code generation required, keeping the stack simple while still being genuine inter-process networking.

- **`net.Listen("tcp", addr)`** opens a real TCP socket: pure networking, no RPC-specific behavior yet.
- **`rpc.Register(service)`** tells `net/rpc` to expose `RPCService`'s methods, callable remotely by the string `"RPCService.MethodName"`.
- **`rpc.Accept(listener)`** sits on top of the listener, handling incoming connections: reading which method was requested, decoding arguments, calling the real Go method, and writing back the reply.
- On the client side, **`rpc.Dial("tcp", addr)`** opens a connection to a peer, and **`client.Call("RPCService.RequestVote", args, &reply)`** performs the actual remote call: serializing `args`, sending them, and blocking until the reply arrives.

Every node in the cluster runs **both** a server (via `transport.Serve`, so peers can reach it) and a client (via `RPCTransport`, so it can reach peers). There's no single "the server" in a peer-to-peer system like this.

---

## Running a cluster today

Three separate terminals, each running one node as its own OS process:

```bash
go run ./cmd/concord -id node-1 -addr localhost:8001 -peers node-2=localhost:8002,node-3=localhost:8003
go run ./cmd/concord -id node-2 -addr localhost:8002 -peers node-1=localhost:8001,node-3=localhost:8003
go run ./cmd/concord -id node-3 -addr localhost:8003 -peers node-1=localhost:8001,node-2=localhost:8002
```

Within a few hundred milliseconds, you should see one node's election timeout fire, an election happen, and one node log that it's become leader. Heartbeats then keep the cluster stable, so no further elections should occur unless a node is killed.

**There is currently no way to actually read or write data**: this demonstrates consensus (leader election + log replication), not yet a usable key-value store. See below.

---

## What's missing

Two pieces stand between "working Raft consensus" and "a usable distributed key-value store":

### 1. The FSM (Finite State Machine)

In Raft, the consensus layer has no opinion about what the replicated data actually _means_: `LogEntry.Command` is deliberately just an opaque `[]byte`. Something has to watch for `commitIndex` advancing past `lastApplied`, decode each newly-committed entry, and apply it to `storage.Store` (a `SET` becomes `store.Set(key, value)`, a `DELETE` becomes `store.Delete(key)`, etc.), then advance `lastApplied` to match.

Without this, committed log entries just sit in the log; they never actually update the key-value data.

### 2. The client-facing API

There's currently no way for anything outside the cluster to send a request at all. This layer needs to:

- Accept `SET`/`GET`/`DELETE` requests from a client (initially another RPC endpoint; later possibly HTTP or gRPC).
- Route writes to the current leader: if a follower receives a write, it needs to reject it or forward it, since only the leader can safely commit new entries.
- Wait for the write to actually commit (reach a majority) before acknowledging success back to the client.
- Serve reads, likely from the leader by default to avoid returning stale data.

---

## Roadmap

Concord is intended to be usable in two different ways, and both are planned:

### As a standalone binary (primary goal)

Users compile or `go install` Concord and run it as its own server process, the same way you'd run etcd, Redis, or Postgres. Multiple processes, one per machine (or one per terminal for local testing), form a cluster. A separate lightweight client (a CLI tool, or a thin client library) talks to the running cluster over the network to actually store and retrieve data.

This is the model the project has been built toward from the start, and is the more natural fit for what Concord is: a small, real piece of distributed infrastructure.

**Steps needed:**

- [ ] Build the FSM, wiring committed entries into `storage.Store`
- [ ] Build the client-facing API (`internal/api`) with leader-forwarding
- [ ] Add a minimal client (CLI or library) for sending `SET`/`GET`/`DELETE` requests
- [ ] Add persistence (write-ahead log to disk) so nodes survive restarts
- [ ] Package and document a proper `go install` / release flow

### As an importable Go package (secondary goal)

Concord's `raft` and `storage` packages should also be usable as a library, allowing another Go program to embed a Concord node directly inside its own process, rather than running Concord as a separate server. This is how libraries like `hashicorp/raft` are commonly used.

**Steps needed:**

- [ ] Design and document a clean, stable public API surface for embedding (constructing a node, wiring a custom transport, registering a custom FSM)
- [ ] Ensure internal packages that need to be embeddable move out of `internal/` (which cannot be imported by external modules) into an importable location, e.g. `pkg/` or a top-level package
- [ ] Add Go doc comments throughout for `godoc`/`pkg.go.dev` generation
- [ ] Publish proper versioned releases (`git tag v0.1.0`, etc.) so `go get` pulls a stable version

### Other future improvements (not yet scheduled)

- Snapshotting and log compaction (so the log doesn't grow forever)
- Cluster membership changes (adding/removing nodes while running)
- Switching `net/rpc` for gRPC (cross-language compatibility, better tooling)
- Read-only replica support / linearizable read optimizations

---

## Design decisions and why

A few choices worth explaining, since they weren't the only options:

- **`Role` is a `string` type, not an `int` with `iota`.** Slightly more memory per value, but self-describing when logged or printed (`"Leader"` instead of `2`), which is worth it for a project where debugging election behavior via logs is a core activity.
- **`Transport` is an interface, satisfied by both `RPCTransport` (real) and, historically, `MockTransport` (an in-process fake used during early development).** This let election and replication logic be built and tested before any real networking existed, and would let a future gRPC-based transport be swapped in without touching `raft` at all.
- **`net/rpc` over gRPC, for now.** No code generation or protobuf tooling required, which kept the networking layer approachable while learning. gRPC remains a reasonable future upgrade, especially if cross-language interoperability ever matters.
- **`internal/` for almost everything currently.** Go's `internal/` convention prevents external packages from importing these, which is appropriate while the API surface is still unstable. Packages intended for the "importable library" roadmap goal will need to move out of `internal/` once their public API is deliberately designed, not accidentally exposed.

---

## Acknowledgments

Built as a hands-on way to learn distributed systems and consensus algorithms in Go. The [Raft paper](https://raft.github.io/raft.pdf) and its accompanying [visualization](https://raft.github.io/) were the primary references throughout.
