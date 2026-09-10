package persist

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Faithful001/concord.git/pkg/raft"
)

func TestSaveAndLoadSnapshot(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "concord_persist_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	nodeID := "node-test"
	snapState := raft.SnapshotState{
		CurrentTerm:       2,
		VotedFor:          "node-test",
		LastIncludedIndex: 8,
		LastIncludedTerm:  2,
		Log: []raft.LogEntry{
			{Term: 2, Index: 9, Command: []byte("set foo bar")},
		},
		CommitIndex: 8,
		Data: map[string][]byte{
			"foo": []byte("bar"),
		},
	}

	snap := FromSnapshotState(snapState)
	if err := Save(tempDir, nodeID, snap); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := Load(tempDir, nodeID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Load returned nil snapshot")
	}

	if loaded.CurrentTerm != 2 || loaded.VotedFor != "node-test" {
		t.Errorf("loaded meta mismatch: term=%d votedFor=%s", loaded.CurrentTerm, loaded.VotedFor)
	}
	if loaded.LastIncludedIndex != 8 || loaded.LastIncludedTerm != 2 {
		t.Errorf("loaded compaction mismatch: lastIncluded=(%d, %d)", loaded.LastIncludedIndex, loaded.LastIncludedTerm)
	}
	if len(loaded.Log) != 1 || loaded.Log[0].Index != 9 {
		t.Errorf("loaded log mismatch: %+v", loaded.Log)
	}
	if !bytes.Equal(loaded.Data["foo"], []byte("bar")) {
		t.Errorf("loaded data mismatch: %s", loaded.Data["foo"])
	}
}

func TestLoadNonExistentSnapshot(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "non_existent_dir_12345")
	loaded, err := Load(tempDir, "non-existent-node")
	if err != nil {
		t.Fatalf("Load on non-existent directory returned error: %v", err)
	}
	if loaded != nil {
		t.Fatalf("Load on non-existent directory returned non-nil snapshot: %+v", loaded)
	}
}
