package raft

import (
	"sync"
)

func (n *Node) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &RequestVoteReply{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	}

	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.role = Follower
		n.votedFor = ""
	}

	lastIndex, lastTerm := n.lastLogIndexAndTerm()
	candidateLogIsUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

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

// TODO: update to real transport
type Transport interface {
	SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
}

func (n *Node) startElection(transport Transport) {
	n.mu.Lock()

	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	electionTerm := n.currentTerm
	lastIndex, lastTerm := n.lastLogIndexAndTerm()
	candidateID := n.id
	peers := n.peers

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

			if n.currentTerm != electionTerm || n.role != Candidate {
				return
			}

			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.role = Follower
				n.votedFor = ""
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

func (n *Node) becomeLeader() {
	n.role = Leader
	n.nextIndex = make(map[string]int)
	n.matchIndex = make(map[string]int)

	lastIndex, _ := n.lastLogIndexAndTerm()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	// TODO: start sending heartbeats, and keep sending them on an interval
	// for as long as we're leader.
}

// lastLogIndexAndTerm returns the index and term of our last log entry,
// or (0, 0) if the log's empty.
func (n *Node) lastLogIndexAndTerm() (int, int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}