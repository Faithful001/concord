package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
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

	log.Printf("[%s] vote for %s in term %d: %v", n.id, args.CandidateID, args.Term, voteGranted)

	return &RequestVoteReply{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}
}

type Transport interface {
	SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

func (n *Node) startElection(transport Transport) {
	n.mu.Lock()

	n.currentTerm++
	n.role = Candidate
	log.Printf("[%s] starting election for term %d", n.id, n.currentTerm)
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
	log.Printf("[%s] became leader for term %d", n.id, n.currentTerm)
	n.nextIndex = make(map[string]int)
	n.matchIndex = make(map[string]int)

	lastIndex, _ := n.lastLogIndexAndTerm()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	go n.sendHeartbeats(n.currentTerm)
}

func (n *Node) sendHeartbeats(leaderTerm int) {
	ticker := time.NewTicker(randomHeartbeatInterval())
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.role != Leader || n.currentTerm != leaderTerm {
			n.mu.Unlock()
			return
		}
		peers := n.peers
		leaderID := n.id
		commitIndex := n.commitIndex
		transport := n.transport
		n.mu.Unlock()

		for _, peer := range peers {
			go func(peer string) {
				reply, err := transport.SendAppendEntries(peer, &AppendEntriesArgs{
					Term:              leaderTerm,
					LeaderID:          leaderID,
					PrevLogIndex:      0,
					PrevLogTerm:       0,
					Entries:           []LogEntry{},
					LeaderCommitIndex: commitIndex,
				})
				if err != nil {
					return
				}

				n.mu.Lock()
				defer n.mu.Unlock()

				if reply.Term > n.currentTerm {
					n.currentTerm = reply.Term
					n.role = Follower
					n.votedFor = ""
				}
			}(peer)
		}
	}
}

func randomHeartbeatInterval() time.Duration {
	return time.Duration(50+rand.Intn(51)) * time.Millisecond // 50–100ms
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