package raft

import (
	"log"
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

func (n *Node) Start() {
	go n.electionTimerLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) resetElectionTimeout() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(151)) * time.Millisecond // 150-300ms
}

func (n *Node) electionTimerLoop() {
	timer := time.NewTimer(randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			log.Printf("[%s] election timeout fired", n.id)

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