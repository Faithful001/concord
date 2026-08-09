package transport

import (
	"net/rpc"

	"github.com/Faithful001/concord.git/internal/raft"
)

type RPCTransport struct {
	addresses map[string]string // peer ID -> "host:port"
}

func NewRPCTransport(addresses map[string]string) *RPCTransport {
	return &RPCTransport{addresses: addresses}
}

func (t *RPCTransport) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	addr, ok := t.addresses[peer]
	if !ok {
		return nil, &UnknownPeerError{Peer: peer}
	}

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var reply raft.RequestVoteReply
	if err := client.Call("RPCService.RequestVote", args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}

func (t *RPCTransport) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	addr, ok := t.addresses[peer]
	if !ok {
		return nil, &UnknownPeerError{Peer: peer}
	}

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var reply raft.AppendEntriesReply
	if err := client.Call("RPCService.AppendEntries", args, &reply); err != nil {
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