package transport

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Faithful001/concord.git/pkg/raft"
	"github.com/Faithful001/concord.git/pkg/raftpb"
)

// RPCTransport implements raft.Transport using gRPC.
type RPCTransport struct {
	addresses map[string]string // peer ID -> "host:port"

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]raftpb.RaftServiceClient
}

// NewRPCTransport creates a new gRPC-based transport with initial peer addresses.
func NewRPCTransport(addresses map[string]string) *RPCTransport {
	addrs := make(map[string]string, len(addresses))
	for k, v := range addresses {
		addrs[k] = v
	}
	return &RPCTransport{
		addresses: addrs,
		conns:     make(map[string]*grpc.ClientConn),
		clients:   make(map[string]raftpb.RaftServiceClient),
	}
}

// AddPeer registers a new peer address dynamically.
func (t *RPCTransport) AddPeer(peer, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.addresses[peer] = addr
}

// RemovePeer unregisters a peer and terminates active connections.
func (t *RPCTransport) RemovePeer(peer string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.addresses, peer)
	if conn, ok := t.conns[peer]; ok {
		conn.Close()
		delete(t.conns, peer)
		delete(t.clients, peer)
	}
}

// Peers returns a copy of the registered peer addresses.
func (t *RPCTransport) Peers() map[string]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := make(map[string]string, len(t.addresses))
	for k, v := range t.addresses {
		cp[k] = v
	}
	return cp
}

func (t *RPCTransport) getClient(peer string) (raftpb.RaftServiceClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[peer]; ok {
		return client, nil
	}

	addr, ok := t.addresses[peer]
	if !ok {
		return nil, &UnknownPeerError{Peer: peer}
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	client := raftpb.NewRaftServiceClient(conn)
	t.conns[peer] = conn
	t.clients[peer] = client
	return client, nil
}

func (t *RPCTransport) dropClient(peer string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peer]; ok {
		conn.Close()
		delete(t.conns, peer)
		delete(t.clients, peer)
	}
}

// Close closes all active gRPC connections.
func (t *RPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	t.clients = make(map[string]raftpb.RaftServiceClient)
}

func (t *RPCTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := &raftpb.RequestVoteRequest{
		Term:         int64(args.Term),
		CandidateId:  args.CandidateID,
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	}

	res, err := client.RequestVote(ctx, req)
	if err != nil {
		t.dropClient(peer)
		return nil, err
	}

	return &raft.RequestVoteReply{
		Term:        int(res.Term),
		VoteGranted: res.VoteGranted,
	}, nil
}

func (t *RPCTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	entries := make([]*raftpb.LogEntryProto, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.LogEntryProto{
			Term:    int64(e.Term),
			Index:   int64(e.Index),
			Command: e.Command,
		}
	}

	req := &raftpb.AppendEntriesRequest{
		Term:              int64(args.Term),
		LeaderId:          args.LeaderID,
		PrevLogIndex:      int64(args.PrevLogIndex),
		PrevLogTerm:       int64(args.PrevLogTerm),
		Entries:           entries,
		LeaderCommitIndex: int64(args.LeaderCommitIndex),
	}

	res, err := client.AppendEntries(ctx, req)
	if err != nil {
		t.dropClient(peer)
		return nil, err
	}

	return &raft.AppendEntriesReply{
		Term:    int(res.Term),
		Success: res.Success,
	}, nil
}

type UnknownPeerError struct {
	Peer string
}

func (e *UnknownPeerError) Error() string {
	return "transport: unknown peer " + e.Peer
}