package raft

type RaftRole string

const (
	RaftRoleFollower  RaftRole = "Follower"
	RaftRoleCandidate RaftRole = "Candidate"
	RaftRoleLeader    RaftRole = "Leader"
)
const (
	RaftRPCRequestVote   = "Raft.RequestVote"
	RaftRPCAppendENtries = "Raft.AppendEntries"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int  // voter's term, for candidate to update itself
	VoteGranted bool // True means candidate receive vote from this voter
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // Follower can redirect to leader
}

type AppendEntriesReply struct {
	Term    int // follower's current term, for leader to update itself
	Success bool
}
