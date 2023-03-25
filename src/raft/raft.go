package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// for leader election
	currentTerm int
	votedFor    int
}

const (
	heartBeatIntervalMS  = 100
	electionTimeoutMinMS = 300
	electionTimeoutMaxMS = 600
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.role == RaftRoleLeader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// RequestVote is invoked by candidate to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	Debug(dVote, "S%d receive vote request from S%d, currently vote for S%v(-1 means none)", rf.me, args.CandidateId, rf.votedFor)
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm ||
		rf.votedFor == args.CandidateId ||
		rf.votedFor == -1 {
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote.
		// If the args.term is larger than current term, meaning this server can vote for another candidate.
		// there are should be at most 1 vote for one term.
		rf.currentTerm = args.Term
		rf.role = RaftRoleFollower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

	if reply.VoteGranted {
		Debug(dVote, "S%d vote for S%d", rf.me, args.CandidateId)
		rf.lastHeartBeat = time.Now()
	}
}

// RequestVote is invoked by candidate to gather votes
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.Term > rf.currentTerm {
		// recognize this leader, give up any ongoing election or its term
		rf.role = RaftRoleFollower
		// a new term begin, clear vote
		rf.currentTerm = args.Term
		rf.votedFor = -1
	} else if args.LeaderId == rf.votedFor {
		rf.lastHeartBeat = time.Now()
	}
	reply.Term = rf.currentTerm
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// timeoutTicker will trigger a election every timeoutMS.
func (rf *Raft) timeoutTicker(trigger chan bool) {
	for !rf.killed() {
		timeoutMS := rand.Int63n(electionTimeoutMaxMS-electionTimeoutMinMS) + electionTimeoutMinMS
		timeoutMSDuration := time.Duration(timeoutMS) * time.Millisecond
		time.Sleep(timeoutMSDuration)
		if time.Since(rf.lastHeartBeat) > timeoutMSDuration &&
			rf.role != RaftRoleLeader {
			trigger <- true
		}
	}
}

// runElection will run a new election when receive a trigger.
func (rf *Raft) runElection(trigger chan bool) {
	for !rf.killed() {
		if <-trigger {
			Debug(dVote, "S%d election timeout\n", rf.me)
			if rf.role == RaftRoleLeader || rf.killed() {
				continue
			}
			go rf.tryElection()
		}
	}
}

func (rf *Raft) tryElection() {
	rf.mu.Lock()
	rf.role = RaftRoleCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastHeartBeat = time.Now()
	thisTerm := rf.currentTerm
	rf.mu.Unlock()

	Debug(dVote, "S%d start an election with T%d", rf.me, thisTerm)

	request := &RequestVoteArgs{
		Term:         thisTerm,
		CandidateId:  rf.me,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
	voteCh := make(chan bool)
	for idx, peer := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(e *labrpc.ClientEnd) {
			defer func() { voteCh <- false }()
			reply := &RequestVoteReply{}
			if ok := e.Call(RaftRPCRequestVote, request, reply); ok {
				voteCh <- reply.VoteGranted
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = RaftRoleFollower
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}
		}(peer)
	}

	// count vote
	Debug(dVote, "S%d counting vote", rf.me)
	voteCnt := 1
	disVoteCnt := 0
	for i := 1; i < len(rf.peers); i++ {
		if <-voteCh {
			voteCnt += 1
		} else {
			disVoteCnt += 1
		}
		if voteCnt >= (len(rf.peers)+1)/2 ||
			disVoteCnt >= (len(rf.peers)+1)/2 {
			break
		}
	}

	// TODO (is this true?): It is impossible to have a candidate to win an election and
	// receive a valid heart beat from a leader. In order to receive a majority vote,
	// candidate's term must be higher than the majority, and the majority's term should
	// be at least as high as the current leader.
	// 1. If this candidate's term is larger than the leader, there can not be a valid heart bead.
	// 2. If this candidate's term is smaller than the leader, it can not receive a majority vote.
	if voteCnt >= (len(rf.peers)+1)/2 && rf.role == RaftRoleCandidate && thisTerm == rf.currentTerm {
		Debug(dVote, "S%d win an election with T%d", rf.me, rf.currentTerm)
		rf.mu.Lock()
		rf.role = RaftRoleLeader
		request := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()

		// send heart beat to followers
		for rf.role == RaftRoleLeader {
			for idx, peer := range rf.peers {
				if idx == rf.me {
					continue
				}
				go func(e *labrpc.ClientEnd) {
					reply := &AppendEntriesReply{}
					if ok := e.Call(RaftRPCAppendENtries, request, reply); ok {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.role = RaftRoleFollower
						}
						rf.mu.Unlock()
					}
				}(peer)
			}
			time.Sleep(heartBeatIntervalMS * time.Millisecond)
		}
	}

	Debug(dVote, "S%d does not receive enough votes or encounter a larger term number", rf.me)
	rf.mu.Lock()
	rf.role = RaftRoleFollower
	rf.mu.Unlock()

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		role:            RaftRoleFollower,
		votedFor:        -1,
		lastHeartBeat:   time.Now(),
		electionTrigger: make(chan bool),
		currentTerm:     0,
	}
	// Your initialization code here (2A, 2B, 2C).

	// timer for leader election

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.timeoutTicker(rf.electionTrigger)
	go rf.runElection(rf.electionTrigger)

	return rf
}
