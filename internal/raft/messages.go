package raft

// RequestVoteArgs is sent by a candidate to gather votes.
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is sent by a follower in response to a RequestVoteArgs.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries or send heartbeats.
type AppendEntriesArgs struct {
	Term         		int
	LeaderID     		string
	PrevLogIndex 		int
	PrevLogTerm  		int
	Entries      		[]LogEntry
	LeaderCommitIndex 	int
}

// AppendEntriesReply is sent by a follower in response to an AppendEntriesArgs.
type AppendEntriesReply struct {
	Term    	int
	Success bool
}