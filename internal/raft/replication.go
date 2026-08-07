package raft

func (n *Node) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &AppendEntriesReply{
			Term:    n.currentTerm,
			Success: false,
		}
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
	}

	// reset the clock for the election timeout timer
	// n.resetElectionTimeout()

	// set the role to follower
	n.role = Follower

	//check if this is the first log entry we're about to make, i.e: args.PrevLogIndex == 0
	if args.PrevLogIndex == 0 {
		term, ok := n.termAt(args.PrevLogIndex)

		if !ok || term != args.PrevLogTerm {
			return &AppendEntriesReply{
				Term:    n.currentTerm,
				Success: false,
			}
		}
	}

	// append new entries
	for _, entry := range args.Entries {
		existingTerm, ok := n.termAt(entry.Index)
		switch {
		case ok && existingTerm != entry.Term:
			// Conflict: cut the log at this point and take the leader's version.
			n.log = n.log[:entry.Index-1]
			n.log = append(n.log, entry)
		case !ok:
			// We don't have this entry yet, so just append it.
			n.log = append(n.log, entry)
		default:
			// ok && existingTerm == entry.Term: we already have this exact
			// entry, nothing to do.
		}
	}

	// advance our commitIndex if the leader has committed further
	// than we have. Cap it at the index of the last new entry, in case we
	// haven't actually received everything the leader has committed yet.
	if args.LeaderCommitIndex > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommitIndex < lastNewIndex {
			n.commitIndex = args.LeaderCommitIndex
		} else {
			n.commitIndex = lastNewIndex
		}
	}

	return &AppendEntriesReply{
		Term:    n.currentTerm,
		Success: true,
	}

}

func (n *Node) termAt(index int) (int, bool) {
	if index < 0 || index > len(n.log) {
		return 0, false
	}

	return n.log[index-1].Term, true
}