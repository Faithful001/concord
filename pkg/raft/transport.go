package raft

// Transport is the networking interface used by a Raft node to reach its peers.
// The consensus logic never touches networking details directly — it calls only
// these two methods, which any concrete transport (TCP/RPC, gRPC, in-memory mock)
// must satisfy.
type Transport interface {
	SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
