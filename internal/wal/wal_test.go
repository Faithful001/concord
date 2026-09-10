package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Faithful001/concord.git/internal/wal"
	"github.com/Faithful001/concord.git/pkg/raft"
)

func openTemp(t *testing.T) (*wal.WAL, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.Open(dir, "node-1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, dir
}

// TestHardStateRoundTrip verifies that written hard-state records are replayed correctly.
func TestHardStateRoundTrip(t *testing.T) {
	w, _ := openTemp(t)

	if err := w.AppendHardState(3, "node-2"); err != nil {
		t.Fatalf("AppendHardState: %v", err)
	}
	if err := w.AppendHardState(5, "node-3"); err != nil {
		t.Fatalf("AppendHardState: %v", err)
	}

	hs, entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
	if hs == nil {
		t.Fatal("expected non-nil HardState")
	}
	// Should reflect the last hard-state record written.
	if hs.Term != 5 || hs.VotedFor != "node-3" {
		t.Errorf("unexpected HardState: %+v", hs)
	}
}

// TestEntryRoundTrip verifies that log entries survive a WAL write/read cycle.
func TestEntryRoundTrip(t *testing.T) {
	w, _ := openTemp(t)

	input := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("set foo bar")},
		{Term: 1, Index: 2, Command: []byte("set baz qux")},
		{Term: 2, Index: 3, Command: []byte("del foo")},
	}

	if err := w.AppendEntries(input); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	_, entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != len(input) {
		t.Fatalf("expected %d entries, got %d", len(input), len(entries))
	}
	for i, e := range entries {
		if e.Term != input[i].Term || e.Index != input[i].Index || string(e.Command) != string(input[i].Command) {
			t.Errorf("entry[%d] mismatch: got %+v, want %+v", i, e, input[i])
		}
	}
}

// TestTruncate verifies that Truncate keeps only the tail entries and that
// subsequent appends work correctly.
func TestTruncate(t *testing.T) {
	w, _ := openTemp(t)

	all := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
		{Term: 1, Index: 3, Command: []byte("c")},
		{Term: 2, Index: 4, Command: []byte("d")},
	}
	if err := w.AppendEntries(all); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	// Snapshot covers indices 1-2; remaining tail is 3-4.
	tail := all[2:]
	if err := w.Truncate(tail); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	_, entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after Truncate: %v", err)
	}
	if len(entries) != len(tail) {
		t.Fatalf("expected %d entries after truncate, got %d", len(tail), len(entries))
	}
	for i, e := range entries {
		if e.Index != tail[i].Index {
			t.Errorf("entry[%d]: got index %d, want %d", i, e.Index, tail[i].Index)
		}
	}

	// Verify subsequent appends after Truncate work.
	newEntry := raft.LogEntry{Term: 2, Index: 5, Command: []byte("e")}
	if err := w.AppendEntries([]raft.LogEntry{newEntry}); err != nil {
		t.Fatalf("AppendEntries after Truncate: %v", err)
	}
	_, entries, err = w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll after append: %v", err)
	}
	if len(entries) != len(tail)+1 {
		t.Fatalf("expected %d entries, got %d", len(tail)+1, len(entries))
	}
	if entries[len(entries)-1].Index != 5 {
		t.Errorf("last entry index: got %d, want 5", entries[len(entries)-1].Index)
	}
}

// TestTornWrite verifies that a truncated (partially-written) record at the end
// of the file is silently ignored during replay.
func TestTornWrite(t *testing.T) {
	w, dir := openTemp(t)

	entries := []raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("good")},
	}
	if err := w.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	w.Close()

	// Append a few bytes of garbage to simulate a torn write.
	path := filepath.Join(dir, "node-1.wal")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	f.Write([]byte{0x00, 0x00, 0x00, 0x20}) // length prefix for non-existent body
	f.Close()

	// Re-open and replay.
	w2, err := wal.Open(dir, "node-1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	_, got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll with torn write: %v", err)
	}
	// Only the intact record should be returned.
	if len(got) != 1 || got[0].Index != 1 {
		t.Errorf("expected 1 intact entry (index=1), got %d entries", len(got))
	}
}

// TestMixedRecords verifies that hard-state and entry records are correctly
// interleaved and replayed.
func TestMixedRecords(t *testing.T) {
	w, _ := openTemp(t)

	w.AppendHardState(1, "")
	w.AppendEntries([]raft.LogEntry{{Term: 1, Index: 1, Command: []byte("x")}})
	w.AppendHardState(2, "node-1")
	w.AppendEntries([]raft.LogEntry{{Term: 2, Index: 2, Command: []byte("y")}})

	hs, entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if hs == nil || hs.Term != 2 || hs.VotedFor != "node-1" {
		t.Errorf("unexpected HardState: %+v", hs)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}
