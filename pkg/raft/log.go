package raft

// LogEntry is a single entry in a Raft node's replicated log.
type LogEntry struct {
	Term    int    `json:"term"`
	Index   int    `json:"index"`
	Command []byte `json:"command"`
}
