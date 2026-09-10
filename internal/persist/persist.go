package persist

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/Faithful001/concord.git/internal/raft"
)

// Snapshot is the serialisable form of a node's persistent Raft state.
type Snapshot struct {
	CurrentTerm       int               `json:"current_term"`
	VotedFor          string            `json:"voted_for"`
	LastIncludedIndex int               `json:"last_included_index"`
	LastIncludedTerm  int               `json:"last_included_term"`
	Log               []raft.LogEntry   `json:"log"`
	CommitIndex       int               `json:"commit_index"`
	Data              map[string][]byte `json:"data,omitempty"`
}

// FromSnapshotState converts a raft.SnapshotState to a Snapshot ready for Save.
func FromSnapshotState(s raft.SnapshotState) Snapshot {
	return Snapshot{
		CurrentTerm:       s.CurrentTerm,
		VotedFor:          s.VotedFor,
		LastIncludedIndex: s.LastIncludedIndex,
		LastIncludedTerm:  s.LastIncludedTerm,
		Log:               s.Log,
		CommitIndex:       s.CommitIndex,
		Data:              s.Data,
	}
}

// ToSnapshotState converts the loaded Snapshot back to a raft.SnapshotState.
func (s Snapshot) ToSnapshotState() raft.SnapshotState {
	return raft.SnapshotState{
		CurrentTerm:       s.CurrentTerm,
		VotedFor:          s.VotedFor,
		LastIncludedIndex: s.LastIncludedIndex,
		LastIncludedTerm:  s.LastIncludedTerm,
		Log:               s.Log,
		CommitIndex:       s.CommitIndex,
		Data:              s.Data,
	}
}


// Save atomically writes a snapshot to <dataDir>/<nodeID>.snap using a temp file
// + rename to avoid a torn write on crash.
func Save(dataDir, nodeID string, snap Snapshot) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dataDir, nodeID+".snap")
	tmp := path + ".tmp"

	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load reads a snapshot from <dataDir>/<nodeID>.snap.
// Returns (nil, nil) if no snapshot file exists yet — a fresh node.
func Load(dataDir, nodeID string) (*Snapshot, error) {
	path := filepath.Join(dataDir, nodeID+".snap")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}
