package raft

import (
	"errors"
)


type MockTransport struct {
	nodes map[string]*Node
}

func NewMockTransport() *MockTransport {
	return &MockTransport{
		nodes: make(map[string]*Node),
	}
}

func (t *MockTransport) Register(peerId string, node *Node) error {
	// check if the peer already exists
	if _, ok := t.nodes[peerId]; ok {
		return errors.New("peer already registered")
	}

	t.nodes[peerId] = node
	return nil
}

func (t *MockTransport) SendRequestVote(peerId string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	//check if the peer exists
	node, ok := t.nodes[peerId]
	if !ok {
		return nil, errors.New("peer not found")
	}

	reply := node.RequestVote(args)
	return reply, nil
}

func (t *MockTransport) SendAppendEntries(peerId string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	//check if the peer exists
	node, ok := t.nodes[peerId]
	if !ok {
		return nil, errors.New("peer not found")
	}

	reply := node.AppendEntries(args)
	return reply, nil

}