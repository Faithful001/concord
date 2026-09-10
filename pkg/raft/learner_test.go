package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockRaftTransport struct {
	peers map[string]*Node
}

func (m *mockRaftTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	node, ok := m.peers[peer]
	if !ok {
		return nil, errors.New("unknown peer: " + peer)
	}
	return node.RequestVote(args), nil
}

func (m *mockRaftTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	node, ok := m.peers[peer]
	if !ok {
		return nil, errors.New("unknown peer: " + peer)
	}
	return node.AppendEntries(args), nil
}

func TestLearnerNodeBehavior(t *testing.T) {
	transport := &mockRaftTransport{peers: make(map[string]*Node)}

	// 2 voting nodes + 1 learner node
	node1 := NewNode("node-1", []string{"node-2", "node-3"}, transport)
	node2 := NewNode("node-2", []string{"node-1", "node-3"}, transport)
	learnerNode := NewNode("node-3", []string{"node-1", "node-2"}, transport)
	learnerNode.SetLearner(true)

	node1.AddLearner("node-3")
	node2.AddLearner("node-3")

	transport.peers["node-1"] = node1
	transport.peers["node-2"] = node2
	transport.peers["node-3"] = learnerNode

	// Test 1: Learner rejects RequestVote
	reply := learnerNode.RequestVote(&RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if reply.VoteGranted {
		t.Errorf("expected learner to reject RequestVote, but vote was granted")
	}

	// Test 2: Node 1 becomes leader
	node1.mu.Lock()
	node1.currentTerm = 1
	node1.becomeLeader()
	node1.mu.Unlock()

	// Append entry on leader and replicate
	err := node1.Submit([]byte("test-command"))
	if err != nil {
		t.Fatalf("Submit failed on leader: %v", err)
	}

	// Verify learner applied entry
	select {
	case msg := <-learnerNode.ApplyCh():
		if string(msg.Command) != "test-command" {
			t.Errorf("learner applied command %q, want 'test-command'", string(msg.Command))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for learner to apply committed entry")
	}

	// Test 3: ReadIndex linearizable read on leader
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	readIdx, err := node1.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}
	if readIdx != 1 {
		t.Errorf("readIdx = %d, want 1", readIdx)
	}

	// Test 4: WaitApplied on learner
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer waitCancel()

	if err := learnerNode.WaitApplied(waitCtx, 1); err != nil {
		t.Errorf("WaitApplied failed on learner: %v", err)
	}
}
