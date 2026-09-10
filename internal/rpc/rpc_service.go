package rpc

import (
	"context"

	"github.com/Faithful001/concord.git/pkg/raft"
	"github.com/Faithful001/concord.git/pkg/raftpb"
)

// RPCService adapts Node's methods to gRPC's RaftServiceServer interface.
type RPCService struct {
	raftpb.UnimplementedRaftServiceServer
	node *raft.Node
}

func NewRPCService(node *raft.Node) *RPCService {
	return &RPCService{node: node}
}

func (s *RPCService) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	args := &raft.RequestVoteArgs{
		Term:         int(req.Term),
		CandidateID:  req.CandidateId,
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	reply := s.node.RequestVote(args)

	return &raftpb.RequestVoteResponse{
		Term:        int64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *RPCService) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{
			Term:    int(e.Term),
			Index:   int(e.Index),
			Command: e.Command,
		}
	}

	args := &raft.AppendEntriesArgs{
		Term:              int(req.Term),
		LeaderID:          req.LeaderId,
		PrevLogIndex:      int(req.PrevLogIndex),
		PrevLogTerm:       int(req.PrevLogTerm),
		Entries:           entries,
		LeaderCommitIndex: int(req.LeaderCommitIndex),
	}

	reply := s.node.AppendEntries(args)

	return &raftpb.AppendEntriesResponse{
		Term:    int64(reply.Term),
		Success: reply.Success,
	}, nil
}