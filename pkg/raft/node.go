package raft

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ── Sentinel errors ───────────────────────────────────────────────────────────

var (
	// ErrNotLeader is returned by Submit when this node is not the leader.
	ErrNotLeader = errors.New("raft: not leader")

	// ErrSubmitTimeout is returned by Submit when the entry does not commit
	// within the deadline (5 s).  This usually means the cluster has lost
	// quorum.
	ErrSubmitTimeout = errors.New("raft: submit timed out waiting for commit")
)

// ── Node ──────────────────────────────────────────────────────────────────────

// Node is a single participant in a Raft cluster.  All exported methods are
// goroutine-safe.
type Node struct {
	mu   sync.Mutex
	id   string
	role Role

	// Persistent Raft state (§5.4 of the paper).
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Compacted snapshot metadata (§7 of the paper).
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile state — all nodes.
	commitIndex int
	lastApplied int

	// Volatile state — leader only.
	nextIndex  map[string]int
	matchIndex map[string]int

	// leaderID is the ID of the node this node currently believes to be
	// leader.  Updated whenever a valid AppendEntries or vote-grant is
	// processed.  Empty if unknown (e.g. mid-election).
	leaderID string

	peers     []string
	transport Transport

	// applyCh is a buffered channel of committed entries destined for the FSM.
	// The FSM goroutine reads from this; the node writes to it.
	applyCh chan ApplyMsg

	// commitWaiters maps log index → channels to notify when that index commits.
	// Used by Submit to block until the submitted entry is durable.
	commitWaiters map[int][]chan error

	resetCh chan struct{}
	stopCh  chan struct{}
}

// NewNode creates a new Raft node ready to be started.
func NewNode(id string, peers []string, transport Transport) *Node {
	return &Node{
		id:                id,
		role:              Follower,
		currentTerm:       0,
		votedFor:          "",
		log:               []LogEntry{},
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make(map[string]int),
		matchIndex:        make(map[string]int),
		peers:             peers,
		transport:         transport,
		applyCh:           make(chan ApplyMsg, 512),
		commitWaiters:     make(map[int][]chan error),
		resetCh:           make(chan struct{}, 1),
		stopCh:            make(chan struct{}),
	}
}

// Start launches the election timer loop.  Call this after any snapshot
// restore but before expecting the node to participate in the cluster.
func (n *Node) Start() {
	go n.electionTimerLoop()
}

// Stop signals all background goroutines to exit.
func (n *Node) Stop() {
	close(n.stopCh)
}

// ApplyCh returns the read-only channel of committed log entries.
// The caller (FSM goroutine) should range over it.
func (n *Node) ApplyCh() <-chan ApplyMsg {
	return n.applyCh
}

// IsLeader reports whether this node is currently the cluster leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// CurrentLeaderID returns the ID of the node this node currently believes to
// be leader.  May be empty during an election.
func (n *Node) CurrentLeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Peers returns a copy of the active peer IDs.
func (n *Node) Peers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	peersCopy := make([]string, len(n.peers))
	copy(peersCopy, n.peers)
	return peersCopy
}

// AddPeer adds a peer ID to the cluster membership dynamically.
func (n *Node) AddPeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, p := range n.peers {
		if p == id {
			return // already present
		}
	}
	n.peers = append(n.peers, id)
	lastIdx, _ := n.lastLogIndexAndTerm()
	n.nextIndex[id] = lastIdx + 1
	n.matchIndex[id] = 0
	log.Printf("[%s] added peer %s (total peers: %d)", n.id, id, len(n.peers))
}

// RemovePeer removes a peer ID from the cluster membership dynamically.
func (n *Node) RemovePeer(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	filtered := make([]string, 0, len(n.peers))
	for _, p := range n.peers {
		if p != id {
			filtered = append(filtered, p)
		}
	}
	n.peers = filtered
	delete(n.nextIndex, id)
	delete(n.matchIndex, id)
	log.Printf("[%s] removed peer %s (total peers: %d)", n.id, id, len(n.peers))
}


// Submit appends cmd to the Raft log and blocks until the entry commits (i.e.
// a majority of nodes have written it) or a timeout occurs.
//
// Returns ErrNotLeader if this node is not the current leader.
// Returns ErrSubmitTimeout if the cluster does not achieve quorum in time.
func (n *Node) Submit(cmd []byte) error {
	n.mu.Lock()

	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	lastIndex, _ := n.lastLogIndexAndTerm()
	idx := lastIndex + 1
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   idx,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	log.Printf("[%s] Submit: appended entry at index %d (term %d)", n.id, idx, n.currentTerm)

	ch := make(chan error, 1)
	n.commitWaiters[idx] = append(n.commitWaiters[idx], ch)
	n.mu.Unlock()

	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		return ErrSubmitTimeout
	}
}

// ── Snapshot support ──────────────────────────────────────────────────────────

// TakeSnapshot captures the state machine snapshot data and compacts the in-memory log
// up to commitIndex.
func (n *Node) TakeSnapshot(data map[string][]byte) SnapshotState {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If commitIndex > lastIncludedIndex, compact the in-memory log
	if n.commitIndex > n.lastIncludedIndex {
		compactTerm, _ := n.termAt(n.commitIndex)
		cut := n.commitIndex - n.lastIncludedIndex
		if cut > 0 && cut <= len(n.log) {
			n.log = append([]LogEntry(nil), n.log[cut:]...)
		}
		n.lastIncludedIndex = n.commitIndex
		n.lastIncludedTerm = compactTerm
	}

	logCopy := make([]LogEntry, len(n.log))
	copy(logCopy, n.log)

	return SnapshotState{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
		Log:               logCopy,
		CommitIndex:       n.commitIndex,
		Data:              data,
	}
}

// RestoreSnapshot loads previously persisted state back into the node.
// It must be called before Start.
func (n *Node) RestoreSnapshot(snap SnapshotState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.currentTerm = snap.CurrentTerm
	n.votedFor = snap.VotedFor
	n.lastIncludedIndex = snap.LastIncludedIndex
	n.lastIncludedTerm = snap.LastIncludedTerm
	n.log = snap.Log
	n.commitIndex = snap.CommitIndex
	// lastApplied = commitIndex so sendToApplyCh will not re-send already-replayed
	// entries once the node is live.
	n.lastApplied = snap.CommitIndex
	log.Printf("[%s] RestoreSnapshot: term=%d lastIncludedIndex=%d log=%d entries commitIndex=%d",
		n.id, n.currentTerm, n.lastIncludedIndex, len(n.log), n.commitIndex)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// resetElectionTimeout signals the timer goroutine to reset without firing.
func (n *Node) resetElectionTimeout() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

// randomElectionTimeout returns a fresh random duration in [150, 300] ms.
// A new value is chosen every restart to prevent split-vote livelock.
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(151)) * time.Millisecond
}

// electionTimerLoop is the background goroutine that drives elections.
func (n *Node) electionTimerLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			log.Printf("[%s] election timeout fired (role=%s)", n.id, role)
			if role != Leader {
				n.startElection(n.transport)
			}
			timer.Reset(randomElectionTimeout())

		case <-n.resetCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(randomElectionTimeout())

		case <-n.stopCh:
			return
		}
	}
}

// sendToApplyCh sends entries (from+1)..to to the FSM's applyCh via a goroutine
// so the mutex is not held while blocking on the channel.
// Must be called with n.mu held.
func (n *Node) sendToApplyCh(from, to int) {
	if to <= from {
		return
	}
	entries := make([]LogEntry, 0, to-from)
	for i := from + 1; i <= to; i++ {
		if i <= n.lastIncludedIndex {
			continue
		}
		offset := i - n.lastIncludedIndex - 1
		if offset >= len(n.log) {
			break
		}
		entries = append(entries, n.log[offset])
	}
	n.lastApplied = to
	if len(entries) > 0 {
		go func() {
			for _, e := range entries {
				n.applyCh <- ApplyMsg{Index: e.Index, Command: e.Command}
			}
		}()
	}
}

// notifyCommitWaiters signals Submit callers waiting for indices in (from, to].
// Must be called with n.mu held.
func (n *Node) notifyCommitWaiters(from, to int) {
	for idx := from + 1; idx <= to; idx++ {
		for _, ch := range n.commitWaiters[idx] {
			ch <- nil
		}
		delete(n.commitWaiters, idx)
	}
}

// clearCommitWaiters notifies all pending Submit callers with err (e.g. on
// leadership loss).  Must be called with n.mu held.
func (n *Node) clearCommitWaiters(err error) {
	for idx, waiters := range n.commitWaiters {
		for _, ch := range waiters {
			ch <- err
		}
		delete(n.commitWaiters, idx)
	}
}

// lastLogIndexAndTerm returns the index and term of the last log entry,
// or (lastIncludedIndex, lastIncludedTerm) when the in-memory log is empty.
// Must be called with n.mu held.
func (n *Node) lastLogIndexAndTerm() (int, int) {
	if len(n.log) == 0 {
		return n.lastIncludedIndex, n.lastIncludedTerm
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}
