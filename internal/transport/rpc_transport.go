package transport

import (
	"net/rpc"
	"sync"

	"github.com/Faithful001/concord.git/internal/raft"
)

type RPCTransport struct {
	addresses map[string]string // peer ID -> "host:port"

	mu      sync.Mutex
	clients map[string]*rpc.Client // peer ID -> live connection, if any
}

func NewRPCTransport(addresses map[string]string) *RPCTransport {
	return &RPCTransport{
		addresses: addresses,
		clients:   make(map[string]*rpc.Client),
	}
}

func (t *RPCTransport) getClient(peer string) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// if we have a connection to this peer, return it
	if client, ok := t.clients[peer]; ok {
		return client, nil
	}

	/// since we don't have a connection to this peer, we need to dial a new one
	// get the address of the peer
	addr, ok := t.addresses[peer]
	if !ok {
		return nil, &UnknownPeerError{Peer: peer}
	}

	//dial a new client
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	t.clients[peer] = client
	return client, nil
}

func (t *RPCTransport) dropClient(peer string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[peer]; ok {
		client.Close()
		delete(t.clients, peer)
	}
}

func (t *RPCTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}

	var reply raft.RequestVoteReply
	if err := client.Call("RPCService.RequestVote", args, &reply); err != nil {
		// if the connection is dead, drop it
		t.dropClient(peer)
		return nil, err
	}
	return &reply, nil
}

func (t *RPCTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	client, err := t.getClient(peer)
	if err != nil {
		return nil, err
	}

	var reply raft.AppendEntriesReply
	if err := client.Call("RPCService.AppendEntries", args, &reply); err != nil {
		t.dropClient(peer)
		return nil, err
	}
	return &reply, nil
}

type UnknownPeerError struct {
	Peer string
}

func (e *UnknownPeerError) Error() string {
	return "transport: unknown peer " + e.Peer
}