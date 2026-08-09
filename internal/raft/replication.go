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

	n.resetElectionTimeout()

	n.role = Follower

	if args.PrevLogIndex > 0 {
		term, ok := n.termAt(args.PrevLogIndex)

		if !ok || term != args.PrevLogTerm {
			return &AppendEntriesReply{
				Term:    n.currentTerm,
				Success: false,
			}
		}
	}

	for _, entry := range args.Entries {
		existingTerm, ok := n.termAt(entry.Index)
		switch {
		case ok && existingTerm != entry.Term:
			n.log = n.log[:entry.Index-1]
			n.log = append(n.log, entry)
		case !ok:
			n.log = append(n.log, entry)
		}
	}

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

// termAt returns the term at log index, and whether it exists.
func (n *Node) termAt(index int) (int, bool) {
	if index < 1 || index > len(n.log) {
		return 0, false
	}

	return n.log[index-1].Term, true
}