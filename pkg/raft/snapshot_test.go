package raft

import (
	"testing"
)

type mockTransport struct{}

func (m *mockTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return &RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (m *mockTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return &AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func TestLogCompactionAndSnapshot(t *testing.T) {
	transport := &mockTransport{}
	node := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	node.currentTerm = 2

	// Populate log with 5 entries
	for i := 1; i <= 5; i++ {
		node.log = append(node.log, LogEntry{
			Term:    2,
			Index:   i,
			Command: []byte("cmd"),
		})
	}
	node.commitIndex = 3

	// Snapshot state machine data
	kvData := map[string][]byte{
		"key1": []byte("val1"),
		"key2": []byte("val2"),
	}

	snap := node.TakeSnapshot(kvData)

	if snap.LastIncludedIndex != 3 {
		t.Fatalf("snap.LastIncludedIndex = %d, want 3", snap.LastIncludedIndex)
	}
	if snap.LastIncludedTerm != 2 {
		t.Fatalf("snap.LastIncludedTerm = %d, want 2", snap.LastIncludedTerm)
	}
	if len(node.log) != 2 {
		t.Fatalf("node.log length after compaction = %d, want 2", len(node.log))
	}
	if node.log[0].Index != 4 || node.log[1].Index != 5 {
		t.Fatalf("unexpected remaining log entries: %+v", node.log)
	}

	// Verify term lookup behavior after compaction
	term, ok := node.termAt(3)
	if !ok || term != 2 {
		t.Errorf("termAt(3) = (%d, %v), want (2, true)", term, ok)
	}

	term, ok = node.termAt(2)
	if ok {
		t.Errorf("termAt(2) on compacted entry returned true, want false")
	}

	term, ok = node.termAt(4)
	if !ok || term != 2 {
		t.Errorf("termAt(4) = (%d, %v), want (2, true)", term, ok)
	}

	lastIdx, lastTerm := node.lastLogIndexAndTerm()
	if lastIdx != 5 || lastTerm != 2 {
		t.Errorf("lastLogIndexAndTerm() = (%d, %d), want (5, 2)", lastIdx, lastTerm)
	}
}

func TestRestoreSnapshot(t *testing.T) {
	transport := &mockTransport{}
	node := NewNode("node-2", []string{"node-1", "node-3"}, transport)

	snapState := SnapshotState{
		CurrentTerm:       3,
		VotedFor:          "node-1",
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Log: []LogEntry{
			{Term: 3, Index: 11, Command: []byte("set a b")},
		},
		CommitIndex: 10,
		Data: map[string][]byte{
			"a": []byte("b"),
		},
	}

	node.RestoreSnapshot(snapState)

	if node.currentTerm != 3 {
		t.Errorf("node.currentTerm = %d, want 3", node.currentTerm)
	}
	if node.lastIncludedIndex != 10 || node.lastIncludedTerm != 2 {
		t.Errorf("node lastIncluded = (%d, %d), want (10, 2)", node.lastIncludedIndex, node.lastIncludedTerm)
	}
	if len(node.log) != 1 || node.log[0].Index != 11 {
		t.Errorf("node.log = %+v, want 1 entry at index 11", node.log)
	}

	lastIdx, lastTerm := node.lastLogIndexAndTerm()
	if lastIdx != 11 || lastTerm != 3 {
		t.Errorf("lastLogIndexAndTerm() = (%d, %d), want (11, 3)", lastIdx, lastTerm)
	}
}

func TestAppendEntriesAfterCompaction(t *testing.T) {
	transport := &mockTransport{}
	node := NewNode("node-3", []string{"node-1", "node-2"}, transport)
	node.currentTerm = 1
	node.lastIncludedIndex = 5
	node.lastIncludedTerm = 1
	node.commitIndex = 5

	reply := node.AppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     "node-1",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 1, Index: 6, Command: []byte("cmd6")},
		},
		LeaderCommitIndex: 6,
	})

	if !reply.Success {
		t.Fatalf("AppendEntries returned success=false, want true")
	}

	if len(node.log) != 1 || node.log[0].Index != 6 {
		t.Fatalf("node.log = %+v, want entry at index 6", node.log)
	}
	if node.commitIndex != 6 {
		t.Fatalf("node.commitIndex = %d, want 6", node.commitIndex)
	}
}
