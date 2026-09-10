package raft

import "log"

// AppendEntries is called by the leader to replicate log entries and send
// heartbeats.  It implements §5.3 of the Raft paper.
func (n *Node) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// §5.1: reply false if args.Term < currentTerm.
	if args.Term < n.currentTerm {
		return &AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	// Higher or equal term from a valid leader — update term and step down.
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.votedFor = ""
		n.clearCommitWaiters(ErrNotLeader)
	}

	n.role = Follower
	n.leaderID = args.LeaderID // remember who the leader is for forwarding
	n.resetElectionTimeout()

	// §5.3: consistency check — do we have an entry at PrevLogIndex with
	// matching term?
	if args.PrevLogIndex > 0 {
		term, ok := n.termAt(args.PrevLogIndex)
		if !ok || term != args.PrevLogTerm {
			return &AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	// §5.3: merge entries into our log.
	for _, entry := range args.Entries {
		if entry.Index <= n.lastIncludedIndex {
			continue // already compacted into snapshot
		}
		existingTerm, ok := n.termAt(entry.Index)
		switch {
		case ok && existingTerm != entry.Term:
			// Conflict: truncate from this point and take the leader's version.
			offset := entry.Index - n.lastIncludedIndex - 1
			n.log = n.log[:offset]
			n.log = append(n.log, entry)
		case !ok:
			// New entry past the end of our log — append it.
			n.log = append(n.log, entry)
		// case ok && existingTerm == entry.Term: already present, skip.
		}
	}

	// §5.3: advance commitIndex.
	if args.LeaderCommitIndex > n.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommitIndex
		if newCommit > lastNewIndex {
			newCommit = lastNewIndex
		}
		if newCommit > n.commitIndex {
			old := n.commitIndex
			n.commitIndex = newCommit
			log.Printf("[%s] follower advancing commitIndex to %d", n.id, n.commitIndex)
			n.sendToApplyCh(old, n.commitIndex)
		}
	}

	return &AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// termAt returns the term of the log entry at the given 1-based index, and
// whether such an entry exists (including compacted snapshot boundary).
func (n *Node) termAt(index int) (int, bool) {
	if index < 1 {
		return 0, false
	}
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm, true
	}
	if index < n.lastIncludedIndex {
		return 0, false // compacted
	}
	offset := index - n.lastIncludedIndex - 1
	if offset < 0 || offset >= len(n.log) {
		return 0, false
	}
	return n.log[offset].Term, true
}
