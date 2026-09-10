package raft

import (
	"errors"
	"log"
	"sync"
	"time"
)

// heartbeatInterval is the fixed interval between leader heartbeat / replication
// rounds.  Must be well below the minimum election timeout (150 ms).
const heartbeatInterval = 75 * time.Millisecond

// ── RequestVote ───────────────────────────────────────────────────────────────

// RequestVote is called by candidates soliciting a vote.
func (n *Node) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
		n.clearCommitWaiters(ErrNotLeader)
	}

	lastIndex, lastTerm := n.lastLogIndexAndTerm()
	// §5.4.1: candidate's log must be at least as up-to-date as ours.
	candidateUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	voteGranted := false
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && candidateUpToDate {
		n.votedFor = args.CandidateID
		n.role = Follower
		voteGranted = true
		n.resetElectionTimeout()
	}

	log.Printf("[%s] RequestVote from %s term=%d granted=%v", n.id, args.CandidateID, args.Term, voteGranted)
	return &RequestVoteReply{Term: n.currentTerm, VoteGranted: voteGranted}
}

// ── Election ──────────────────────────────────────────────────────────────────

func (n *Node) startElection(transport Transport) {
	n.mu.Lock()

	n.currentTerm++
	n.role = Candidate
	n.leaderID = ""
	n.votedFor = n.id
	electionTerm := n.currentTerm
	lastIndex, lastTerm := n.lastLogIndexAndTerm()
	candidateID := n.id
	peers := n.peers

	log.Printf("[%s] starting election for term %d", n.id, electionTerm)
	n.mu.Unlock()

	n.resetElectionTimeout()

	votes := 1 // we vote for ourselves
	var votesMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			reply, err := transport.SendRequestVote(peer, &RequestVoteArgs{
				Term:         electionTerm,
				CandidateID:  candidateID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// Stale reply — term or role changed while we waited.
			if n.currentTerm != electionTerm || n.role != Candidate {
				return
			}

			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.role = Follower
				n.votedFor = ""
				n.clearCommitWaiters(ErrNotLeader)
				return
			}

			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				wonMajority := votes*2 > len(peers)+1
				votesMu.Unlock()

				if wonMajority && n.role == Candidate {
					n.becomeLeader()
				}
			}
		}(peer)
	}

	wg.Wait()
}

// becomeLeader transitions this node to the Leader role and starts the
// replication loop.  Must be called with n.mu held.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	log.Printf("[%s] became leader for term %d", n.id, n.currentTerm)

	lastIndex, _ := n.lastLogIndexAndTerm()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	go n.sendHeartbeats(n.currentTerm)
}

// ── Replication loop (leader-side) ───────────────────────────────────────────

// sendHeartbeats ticks at heartbeatInterval and drives a replication round to
// each peer on every tick.  It exits when this node is no longer the leader.
func (n *Node) sendHeartbeats(leaderTerm int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.role != Leader || n.currentTerm != leaderTerm {
			n.mu.Unlock()
			return
		}
		peers := n.peers
		n.mu.Unlock()

		for _, peer := range peers {
			go n.replicateToPeer(peer, leaderTerm)
		}
	}
}

// replicateToPeer sends an AppendEntries RPC to peer carrying any log entries
// it is missing (or an empty heartbeat if it is caught up).
// On a successful reply it advances matchIndex / nextIndex and checks for a
// new commit point.  On failure it backs up nextIndex for the next round.
func (n *Node) replicateToPeer(peer string, leaderTerm int) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != leaderTerm {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	lastIdx, _ := n.lastLogIndexAndTerm()

	// If peer is behind compacted log, clamp nextIndex to current log start
	if nextIdx <= n.lastIncludedIndex {
		nextIdx = n.lastIncludedIndex + 1
		n.nextIndex[peer] = nextIdx
	}

	// Collect the entries this peer is missing (empty slice = heartbeat).
	var entries []LogEntry
	if nextIdx <= lastIdx {
		startOffset := nextIdx - n.lastIncludedIndex - 1
		endOffset := lastIdx - n.lastIncludedIndex
		if startOffset >= 0 && endOffset <= len(n.log) && startOffset <= endOffset {
			src := n.log[startOffset:endOffset]
			entries = make([]LogEntry, len(src))
			copy(entries, src)
		}
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm, _ := n.termAt(prevLogIndex)

	commitIndex := n.commitIndex
	leaderID := n.id
	transport := n.transport
	n.mu.Unlock()

	reply, err := transport.SendAppendEntries(peer, &AppendEntriesArgs{
		Term:              leaderTerm,
		LeaderID:          leaderID,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: commitIndex,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader || n.currentTerm != leaderTerm {
		return
	}

	if reply.Term > n.currentTerm {
		// Discovered a higher term — step down.
		n.currentTerm = reply.Term
		n.role = Follower
		n.votedFor = ""
		n.clearCommitWaiters(errors.New("raft: stepped down from leader"))
		return
	}

	if reply.Success {
		newMatch := prevLogIndex + len(entries)
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.maybeAdvanceCommitIndex(leaderTerm)
	} else {
		// Consistency check failed — back up and retry next tick.
		if n.nextIndex[peer] > n.lastIncludedIndex+1 {
			n.nextIndex[peer]--
		}
	}
}

// maybeAdvanceCommitIndex finds the highest N > commitIndex such that
// termAt(N) == leaderTerm and a majority of nodes have matchIndex >= N,
// then commits up to N.  Must be called with n.mu held.
func (n *Node) maybeAdvanceCommitIndex(leaderTerm int) {
	lastIdx, _ := n.lastLogIndexAndTerm()
	for N := lastIdx; N > n.commitIndex; N-- {
		term, ok := n.termAt(N)
		if !ok || term != leaderTerm {
			continue
		}
		count := 1 // leader itself
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				count++
			}
		}
		// Majority check (works for any cluster size, including single-node).
		if count*2 > len(n.peers)+1 {
			old := n.commitIndex
			n.commitIndex = N
			log.Printf("[%s] leader committed up to index %d", n.id, N)
			n.sendToApplyCh(old, N)
			n.notifyCommitWaiters(old, N)
			break
		}
	}
}