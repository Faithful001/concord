package rpc

import "github.com/Faithful001/concord.git/internal/raft"

// RPCService adapts Node's direct-return methods to net/rpc's func(args, *reply) error signature.

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