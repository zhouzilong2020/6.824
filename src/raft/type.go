package raft

import (
	"fmt"
	"sync"
	"time"

	"6.5840/labrpc"
)

const (
	heartBeatIntervalMS  = 100
	electionTimeoutMinMS = 400
	electionTimeoutMaxMS = 800
)

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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Data interface{}
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	role            RaftRole
	lastHeartBeat   time.Time
	electionTrigger chan bool
	applyCh         chan ApplyMsg // for test only

	// persistent states begin
	currentTerm int
	votedFor    int
	log         []LogEntry
	// persistent states end

	commitIdx   int
	lastApplied int
	commitCond  *sync.Cond


	// only for leader
	nextIdx  []int
	matchIdx []int
}

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
	Term            int // leader's term
	LeaderId        int // Follower can redirect to leader
	PrevLogIdx      int
	PrevLogTerm     int
	Entries         []LogEntry // empty for heartbeat, may send more than one for efficiency
	LeaderCommitIdx int
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("S%d(T%d), prev_log%d(T%d), commit %d", args.LeaderId, args.Term, args.PrevLogIdx, args.PrevLogTerm, args.LeaderCommitIdx)
}

type AppendEntriesReply struct {
	Term    int // follower's current term, for leader to update itself
	Success bool

	// info for leader to fast determine the nextIdx for a follower
	ConflictingTerm        int
	FirstConflictingLogIdx int
	LogLen                 int
}
