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
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	role          RaftRole
	lastHeartBeat time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// for leader election
	currentTerm int
	votedFor    int
}

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
	rf.lastHeartBeat = time.Now()
	Debug(dVote, "S%d receive vote request from S%d, currently vote for S%v(-1 means none)", rf.me, args.CandidateId, rf.votedFor)
	if args.Term < rf.currentTerm {
		// recognized this candidate step down from candidate
		reply.VoteGranted = true
	} else if (args.Term >= rf.currentTerm) ||
		(rf.votedFor == args.CandidateId || rf.votedFor == -1) {
		// revert to follower
		rf.role = RaftRoleFollower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
	}

	if reply.VoteGranted {
		Debug(dVote, "S%d vote for S%d", rf.me, args.CandidateId)
	}
}

// RequestVote is invoked by candidate to gather votes
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastHeartBeat = time.Now()
	if args.Term >= rf.currentTerm {
		// recognize this leader, give up any ongoing election
		if rf.role == RaftRoleCandidate {
			rf.role = RaftRoleFollower
		}
		rf.currentTerm = args.Term
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		timeoutMS := 250 + (rand.Int63() % 300)
		rf.mu.Lock()
		if time.Since(rf.lastHeartBeat).Milliseconds() < timeoutMS {
			rf.mu.Unlock()
			time.Sleep(time.Duration(timeoutMS) * time.Millisecond)
			continue
		}

		Debug(dVote, "S%d start an election with T%d", rf.me, rf.currentTerm+1)
		// election timeout, select self as candidate and start an election.
		rf.role = RaftRoleCandidate
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.mu.Unlock()

		request := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: -1,
			LastLogTerm:  -1,
		}
		voteCh := make(chan bool)
		for idx, peer := range rf.peers {
			if idx == rf.me {
				continue
			}
			reply := &RequestVoteReply{}
			go func(e *labrpc.ClientEnd) {
				if ok := e.Call(RaftRPCRequestVote, request, reply); ok {
					voteCh <- reply.VoteGranted
					return
				}
				// fail to hear back from peer.
				voteCh <- false
			}(peer)
		}

		voteCnt := 1 // automatically vote from self
		disVoteCnt := 0
		for i := 1; i < len(rf.peers); i++ {
			isGranted := <-voteCh
			if isGranted {
				voteCnt += 1
			} else {
				disVoteCnt += 1
			}
			if voteCnt >= (len(rf.peers)+1)/2 || disVoteCnt >= (len(rf.peers)+1)/2 {
				break
			}
		}

		if voteCnt >= (len(rf.peers)+1)/2 && rf.role == RaftRoleCandidate {
			Debug(dVote, "S%d win an election with T%d", rf.me, rf.currentTerm)
			// TODO (is this true?): It is impossible to have a candidate to win an election and
			// receive a valid heart beat from a leader. In order to receive a majority vote,
			// candidate's term must be higher than the majority, and the majority's term should
			// be at least as high as the current leader.
			// 1. If this candidate's term is larger than the leader, there can not be a valid heart bead.
			// 2. If this candidate's term is smaller than the leader, it can not receive a majority vote.
			rf.mu.Lock()
			rf.role = RaftRoleLeader
			request := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			for {
				heartBeatInterval := 100 * time.Millisecond
				liveCh := make(chan bool)
				if rf.role != RaftRoleLeader {
					break
				}
				for idx, peer := range rf.peers {
					if idx == rf.me {
						continue
					}
					reply := &RequestVoteReply{}
					go func(e *labrpc.ClientEnd) {
						liveCh <- e.Call(RaftRPCAppendENtries, request, reply)
					}(peer)
				}

				liveCnt := 1
				for i := 1; i < len(rf.peers); i++ {
					isLive := <-liveCh
					if isLive {
						liveCnt += 1
					}
				}
				if liveCnt >= (len(rf.peers)+1)/2 {
					time.Sleep(heartBeatInterval)
					continue
				}
				break
			}
		} else {
			Debug(dVote, "S%d does not receive enough votes", rf.me)
		}

		// lose the election or no majority are live, un vote for self.
		Debug(dVote, "S%d become a follower", rf.me)
		rf.mu.Lock()
		// if there is other heart beat get accepted by this server
		rf.votedFor = -1
		rf.role = RaftRoleFollower
		rf.mu.Unlock()

	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = RaftRoleFollower
	rf.votedFor = -1
	rf.lastHeartBeat = time.Now()
	rf.currentTerm = 0

	// Your initialization code here (2A, 2B, 2C).

	// timer for leader election

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
