package rpc

import "github.com/Faithful001/concord.git/internal/raft"

// This is the adapter layer
// RPCService adapts Node's methods to the signature net/rpc requires:
// func(args, *reply) error. Node's own methods return values directly,
// which net/rpc can't call remotely.

type RPCService struct {
	node *raft.Node
}

func NewRPCService(node *raft.Node) *RPCService {
	return &RPCService{node: node}
}

func (s *RPCService) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	result := s.node.RequestVote(args)
	*reply = *result
	return nil
}

func (s *RPCService) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	result := s.node.AppendEntries(args)
	*reply = *result
	return nil
}