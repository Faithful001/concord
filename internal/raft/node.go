package raft

import "sync"

type Node struct {
	mu   		sync.Mutex
	id   		string
	role 		Role

	// persistent state on all servers
	currentTerm int
	votedFor    string
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex   map[string]int
	matchIndex  map[string]int

	peers 		[]string
}
