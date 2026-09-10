package raft

type Role string

const (
	Leader    Role = "Leader"
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
)
