package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Faithful001/concord.git/internal/api"
	"github.com/Faithful001/concord.git/internal/command"
	"github.com/Faithful001/concord.git/internal/fsm"
	"github.com/Faithful001/concord.git/internal/persist"
	"github.com/Faithful001/concord.git/internal/storage"
	"github.com/Faithful001/concord.git/internal/transport"
	"github.com/Faithful001/concord.git/internal/wal"
	"github.com/Faithful001/concord.git/pkg/raft"
)

func main() {
	// ── Flags ─────────────────────────────────────────────────────────────────
	id := flag.String("id", "", "unique node ID, e.g. node-1 (required)")
	addr := flag.String("addr", "", "Raft TCP listen address, e.g. :8001 (required)")
	apiAddr := flag.String("api-addr", "", "HTTP API listen address, e.g. :9001 (optional; omit for consensus-only mode)")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port Raft peer addresses, e.g. node-2=:8002,node-3=:8003")
	apiPeersFlag := flag.String("api-peers", "", "comma-separated id=host:port HTTP API peer addresses, e.g. node-2=:9002,node-3=:9003")
	dataDir := flag.String("data-dir", ".", "directory for persistent snapshots")
	learner := flag.Bool("learner", false, "run as a read-only learner replica (non-voting)")
	flag.Parse()

	if *id == "" || *addr == "" {
		log.Fatal("both -id and -addr are required")
	}

	// ── Parse peer maps ───────────────────────────────────────────────────────
	raftAddrs := make(map[string]string) // peerID → raft "host:port"
	apiAddrs := make(map[string]string)  // peerID → api  "host:port"
	var peerIDs []string

	if *peersFlag != "" {
		for _, pair := range strings.Split(*peersFlag, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid -peers entry: %q (want id=host:port)", pair)
			}
			raftAddrs[parts[0]] = parts[1]
			peerIDs = append(peerIDs, parts[0])
		}
	}

	if *apiPeersFlag != "" {
		for _, pair := range strings.Split(*apiPeersFlag, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid -api-peers entry: %q (want id=host:port)", pair)
			}
			apiAddrs[parts[0]] = parts[1]
		}
	}

	// ── Load snapshot (if any) ────────────────────────────────────────────────
	snap, err := persist.Load(*dataDir, *id)
	if err != nil {
		log.Fatalf("[%s] failed to load snapshot: %v", *id, err)
	}

	// ── Open WAL ──────────────────────────────────────────────────────────────
	w, err := wal.Open(*dataDir, *id)
	if err != nil {
		log.Fatalf("[%s] failed to open WAL: %v", *id, err)
	}
	defer w.Close()

	// ── Build components ──────────────────────────────────────────────────────
	store := storage.NewStore()
	rpcTransport := transport.NewRPCTransport(raftAddrs)
	node := raft.NewNode(*id, peerIDs, rpcTransport)
	if *learner {
		node.SetLearner(true)
		log.Printf("[%s] running as a read-only learner replica", *id)
	}
	stateMachine := fsm.New(store)

	var apiServer *api.Server
	if *apiAddr != "" {
		apiServer = api.New(*id, node, store, apiAddrs)
	}

	stateMachine.SetMembershipHandler(func(op byte, peerID, raftAddr, apiAddr string) {
		if op == command.OpAddPeer {
			node.AddPeer(peerID)
			if raftAddr != "" {
				rpcTransport.AddPeer(peerID, raftAddr)
			}
			if apiServer != nil && apiAddr != "" {
				apiServer.AddAPIPeer(peerID, apiAddr)
			}
		} else if op == command.OpAddLearner {
			node.AddLearner(peerID)
			if raftAddr != "" {
				rpcTransport.AddPeer(peerID, raftAddr)
			}
			if apiServer != nil && apiAddr != "" {
				apiServer.AddAPIPeer(peerID, apiAddr)
			}
		} else if op == command.OpRemovePeer {
			node.RemovePeer(peerID)
			rpcTransport.RemovePeer(peerID)
			if apiServer != nil {
				apiServer.RemoveAPIPeer(peerID)
			}
		}
	})

	// ── Restore snapshot ──────────────────────────────────────────────────────
	var snapState raft.SnapshotState
	if snap != nil {
		snapState = snap.ToSnapshotState()
	}

	// ── Replay WAL on top of snapshot ─────────────────────────────────────────
	// Recover any entries (and hard-state) that were written since the last
	// snapshot but not yet included in it (i.e. were lost if we crash-looped).
	walHS, walEntries, err := w.ReadAll()
	if err != nil {
		log.Fatalf("[%s] WAL replay failed: %v", *id, err)
	}
	if walHS != nil {
		if walHS.Term > snapState.CurrentTerm {
			snapState.CurrentTerm = walHS.Term
		}
		if walHS.VotedFor != "" {
			snapState.VotedFor = walHS.VotedFor
		}
	}
	for _, e := range walEntries {
		if e.Index > snapState.LastIncludedIndex {
			snapState.Log = append(snapState.Log, e)
		}
	}
	walRecovered := len(walEntries)

	if snap != nil || walRecovered > 0 {
		node.RestoreSnapshot(snapState)

		// Restore in-memory key-value data directly from the snapshot
		if snap != nil && snap.Data != nil {
			stateMachine.Restore(snap.Data)
		}

		// Replay committed entries that were in the snapshot log or recovered from WAL
		for _, entry := range snapState.Log {
			if entry.Index > snapState.LastIncludedIndex && entry.Index <= snapState.CommitIndex {
				stateMachine.ApplyEntry(raft.ApplyMsg{
					Index:   entry.Index,
					Command: entry.Command,
				})
			}
		}
		log.Printf("[%s] recovery complete: snap=%v walEntries=%d term=%d commitIndex=%d",
			*id, snap != nil, walRecovered, snapState.CurrentTerm, snapState.CommitIndex)
	}

	// ── Wire WAL hooks ────────────────────────────────────────────────────────
	node.OnHardStateChange = func(term int, votedFor string) {
		if err := w.AppendHardState(term, votedFor); err != nil {
			log.Printf("[%s] WAL hard-state write error: %v", *id, err)
		}
	}
	node.OnLogAppend = func(entries []raft.LogEntry) {
		if err := w.AppendEntries(entries); err != nil {
			log.Printf("[%s] WAL entry write error: %v", *id, err)
		}
	}

	// ── Start FSM goroutine ───────────────────────────────────────────────────
	// All future committed entries arrive via node.ApplyCh() and are applied here.
	go stateMachine.Run(node.ApplyCh())

	// ── Periodic snapshot ─────────────────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			kvData := stateMachine.Snapshot()
			state := node.TakeSnapshot(kvData)
			if saveErr := persist.Save(*dataDir, *id, persist.FromSnapshotState(state)); saveErr != nil {
				log.Printf("[%s] snapshot save error: %v", *id, saveErr)
				continue
			}
			log.Printf("[%s] snapshot saved (term=%d lastIncludedIndex=%d remainingLog=%d commitIndex=%d)",
				*id, state.CurrentTerm, state.LastIncludedIndex, len(state.Log), state.CommitIndex)
			// Truncate WAL: only keep log entries after the new snapshot boundary.
			if truncErr := w.Truncate(state.Log); truncErr != nil {
				log.Printf("[%s] WAL truncate error: %v", *id, truncErr)
			}
		}
	}()

	// ── Raft TCP listener ─────────────────────────────────────────────────────
	go func() {
		fmt.Printf("[%s] Raft listening on %s\n", *id, *addr)
		if serveErr := transport.Serve(node, *addr); serveErr != nil {
			log.Fatalf("[%s] Raft serve failed: %v", *id, serveErr)
		}
	}()

	// ── HTTP API server (optional) ────────────────────────────────────────────
	if apiServer != nil {
		go func() {
			if serveErr := apiServer.Serve(*apiAddr); serveErr != nil {
				log.Fatalf("[%s] API serve failed: %v", *id, serveErr)
			}
		}()
	} else {
		log.Printf("[%s] -api-addr not set; running in consensus-only mode (no HTTP API)", *id)
	}

	// ── Start consensus ───────────────────────────────────────────────────────
	node.Start()

	select {} // block forever — background goroutines do all the work
}