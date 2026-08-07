package raft

type Node struct {
	currentTerm int
	votedFor    string
	log         []byte
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}