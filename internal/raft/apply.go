package raft

// ApplyMsg carries a committed log entry from the Raft node to the FSM.
// The FSM receives one ApplyMsg per committed log entry, in index order.
type ApplyMsg struct {
	Index   int
	Command []byte
}

// SnapshotState captures the persistent Raft state for snapshotting.
// It is safe to serialise with encoding/json.
type SnapshotState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
	CommitIndex int
}
