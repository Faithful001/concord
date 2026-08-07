package raft

func (n *Node) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule 1: reject outright if the candidate's term is stale.
	if args.Term < n.currentTerm {
		return &RequestVoteReply{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	}

	// Rule 2: if the candidate's term is newer, we're behind. Step down to
	// Follower and reset our vote for this new term. Note: commitIndex,
	// lastApplied, nextIndex, and matchIndex are NOT touched here, they
	// track our own local replication progress, which this RPC has no
	// bearing on.
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
	}

	// Rule 3: check whether the candidate's log is at least as up-to-date
	// as ours. Compare LastLogTerm first, a higher term means more recent
	// data regardless of index. Only if terms are equal do we fall back to
	// comparing index, the longer log wins.
	lastIndex, lastTerm := n.lastLogIndexAndTerm()
	candidateLogIsUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	// Rule 4: grant the vote only if we haven't voted this term (or already
	// voted for this exact candidate) AND their log qualifies under Rule 3.
	voteGranted := false
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && candidateLogIsUpToDate {
		n.votedFor = args.CandidateID
		n.role = Follower
		voteGranted = true
	}

	return &RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}
}

// lastLogIndexAndTerm returns the index and term of the last entry in this
// node's log, or (0, 0) if the log is empty.
func (n *Node) lastLogIndexAndTerm() (int, int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}