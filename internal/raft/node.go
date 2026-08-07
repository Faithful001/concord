package raft

import (
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	mu   sync.Mutex
	id   string
	role Role

	currentTerm int
	votedFor    string
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  map[string]int
	matchIndex map[string]int

	peers     []string
	transport Transport

	resetCh chan struct{}
	stopCh  chan struct{}
}

func NewNode(id string, peers []string, transport Transport) *Node {
	return &Node{
		id:          id,
		role:        Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         []LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]int),
		matchIndex:  make(map[string]int),
		peers:       peers,
		transport:   transport,
		resetCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
}

// Start begins the election timer loop. Call this once, when the node
// boots up.
func (n *Node) Start() {
	go n.electionTimerLoop()
}

// Stop shuts down the election timer loop permanently.
func (n *Node) Stop() {
	close(n.stopCh)
}

// resetElectionTimeout signals that we heard from a leader, so the timer
// should restart from a fresh random duration. Non-blocking: if a reset is
// already pending, this is a no-op, since one pending reset is enough.
func (n *Node) resetElectionTimeout() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(151)) * time.Millisecond // 150–300ms
}

func (n *Node) electionTimerLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Timeout fired with no reset in between and no one's heard from
			// a leader in time. Only Followers/Candidates should start an
			// election; a Leader ignores its own timeout (it's the one
			// sending heartbeats, not waiting for them).
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			if role != Leader {
				n.startElection(n.transport)
			}

			timer.Reset(randomElectionTimeout())

		case <-n.resetCh:
			// Heard from a leader (or started our own election) before the
			// timeout fired, restart the countdown.
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(randomElectionTimeout())

		case <-n.stopCh:
			return
		}
	}
}