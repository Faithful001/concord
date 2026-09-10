package transport

import (
	"net"
	"testing"
	"time"

	"github.com/Faithful001/concord.git/pkg/raft"
)

type dummyTransport struct{}

func (d *dummyTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nil, nil
}

func (d *dummyTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nil, nil
}

func TestGRPCTransportAndServer(t *testing.T) {
	// Find available free port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	// Create node 1
	node1 := raft.NewNode("node-1", []string{"node-2"}, &dummyTransport{})
	
	// Start gRPC server in background
	go func() {
		_ = Serve(node1, addr)
	}()

	// Wait for server to start listening
	time.Sleep(100 * time.Millisecond)

	// Create transport on node 2 pointing to node 1
	trans := NewRPCTransport(map[string]string{
		"node-1": addr,
	})
	defer trans.Close()

	// Send RequestVote RPC via gRPC
	reply, err := trans.SendRequestVote("node-1", &raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("SendRequestVote failed: %v", err)
	}
	if !reply.VoteGranted {
		t.Errorf("expected vote to be granted, got false")
	}

	// Send AppendEntries RPC via gRPC
	appendReply, err := trans.SendAppendEntries("node-1", &raft.AppendEntriesArgs{
		Term:              1,
		LeaderID:          "node-2",
		PrevLogIndex:      0,
		PrevLogTerm:       0,
		Entries:           nil,
		LeaderCommitIndex: 0,
	})
	if err != nil {
		t.Fatalf("SendAppendEntries failed: %v", err)
	}
	if !appendReply.Success {
		t.Errorf("expected append entries to succeed, got false")
	}

	// Test dynamic peer manipulation
	trans.AddPeer("node-3", "127.0.0.1:9999")
	peers := trans.Peers()
	if _, ok := peers["node-3"]; !ok {
		t.Errorf("expected node-3 in peers list")
	}

	trans.RemovePeer("node-3")
	peers = trans.Peers()
	if _, ok := peers["node-3"]; ok {
		t.Errorf("expected node-3 to be removed from peers list")
	}
}
