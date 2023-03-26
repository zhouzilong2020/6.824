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
	// update term
	myTerm := rf.currentTerm
	reply.Term = myTerm
	rf.currentTerm = Max(args.Term, rf.currentTerm)
	if myTerm < rf.currentTerm {
		// demoted to follower
		rf.role = RaftRoleFollower
		rf.votedFor = -1
	}

	reply.VoteGranted = false
	Debug(dVote, "S%d(T%d) receive vote request from S%d(T%d), voted for S%v",
		rf.me, myTerm, args.CandidateId, args.Term, rf.votedFor)
	if args.Term < myTerm { // reject vote
		// Reply false if term < myTerm
		reply.VoteGranted = false
		return
	}

	if (args.Term > myTerm || // immediately revert to follower which can grand vote
		(rf.votedFor == args.CandidateId || rf.votedFor == -1)) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || // if candidate's log is more up to date
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote.
		// If the args.term is larger than current term, meaning this server can vote for another candidate.
		// there are should be at most 1 vote for one term.
		Debug(dTerm, "S%d meet T%d", rf.me, args.Term)
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
	myTerm := rf.currentTerm
	rf.currentTerm = Max(rf.currentTerm, args.Term)
	if myTerm != rf.currentTerm {
		rf.votedFor = -1
	}
	reply.Term = myTerm

	if args.LeaderId == rf.votedFor {
		rf.lastHeartBeat = time.Now()
	}

	if len(args.Entries) > 0 {
		Debug(dLog, "S%d(T%d) receive AppendEntries val: %v", rf.me, myTerm, args)
	} else {
		Debug(dLog, "S%d(T%d) receive heart beat from S%d(T%d)", rf.me, myTerm, args.LeaderId, args.Term)
	}
	// reject the request
	if args.Term < myTerm || // invalid term
		len(rf.log)-1 < args.PrevLogIdx || // follower does not have that many log entry as leader does
		rf.log[args.PrevLogIdx].Term != args.PrevLogTerm { // invalid entry
		// if len(args.Entries) > 0 {
		Debug(dLog, "S%d(T%d) rejected S%d, lastIdx %d", rf.me, myTerm, args.LeaderId, len(rf.log)-1)
		// }
		reply.Success = false
		return
	}

	reply.Success = true
	if args.Term > myTerm || rf.role == RaftRoleCandidate {
		// recognize this leader, give up the ongoing election on its term (if there is one).
		rf.role = RaftRoleFollower
		// a new term begin, clear vote
		rf.votedFor = -1
	}

	isMisMatch := false
	for i, entry := range args.Entries {
		nextIdx := args.PrevLogIdx + i + 1
		// if there is not a mismatch, do not append
		if !isMisMatch {
			if len(rf.log)-1 < nextIdx {
				isMisMatch = true
			} else if rf.log[nextIdx].Term != entry.Term {
				// delete this entry and everything follows it
				rf.log = rf.log[:nextIdx]
				isMisMatch = true
			}
		}

		if isMisMatch { // mismatch happens, append everything from the args that not already in log.
			rf.log = append(rf.log, entry)
			Debug(dLog, "S%d appended log%d %v\n", rf.me, len(rf.log)-1, rf.log[len(rf.log)-1])
		}
	}

	if args.LeaderCommitIdx > rf.commitIdx {
		Debug(dCommit, "S%d(T%d) trigger commit", rf.me, myTerm)
		rf.commitIdx = Min(args.LeaderCommitIdx, len(rf.log)-1)
		rf.commitCond.Signal()
	}

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
	rf.mu.Lock()
	if rf.role != RaftRoleLeader {
		rf.mu.Unlock()
		return -1, rf.currentTerm, false
	}

	// only leader can call ApplyEntries to other server.
	newLogEntry := LogEntry{Data: command, Term: rf.currentTerm}
	rf.log = append(rf.log, newLogEntry)
	curLogIdx := len(rf.log) - 1
	myTerm := rf.currentTerm
	rf.mu.Unlock()

	successCh := make(chan bool)
	for idx, peer := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(e *labrpc.ClientEnd, idx int, successCh chan bool) {
			ok := false
			reply := &AppendEntriesReply{}
			for (!ok || !reply.Success) && rf.role == RaftRoleLeader {
				reply.Success = false
				reply.Term = 0
				request := &AppendEntriesArgs{
					Term:            myTerm,
					LeaderId:        rf.me,
					PrevLogIdx:      rf.nextIdx[idx] - 1, // start with a dummy head
					PrevLogTerm:     rf.log[rf.nextIdx[idx]-1].Term,
					Entries:         rf.log[rf.nextIdx[idx]:],
					LeaderCommitIdx: rf.commitIdx,
				}
				ok = e.Call(RaftRPCAppendENtries, request, reply)
				if ok {
					rf.mu.Lock()
					rf.checkTerm(reply.Term, idx)
					if reply.Term == myTerm {
						if reply.Success {
							rf.matchIdx[idx]++
							rf.nextIdx[idx] = curLogIdx + 1
						} else {
							rf.nextIdx[idx]--
						}
					}
					rf.mu.Unlock()
				}
			}
			successCh <- reply.Success
		}(peer, idx, successCh)
	}
	go func(successCh chan bool) {
		successCnt := 1 // self is consider a success
		for i := 1; i < len(rf.peers) && rf.role == RaftRoleLeader; i++ {
			if <-successCh {
				successCnt++
			}
			if successCnt > len(rf.peers)/2 {
				Debug(dTrace, "S%d(T%d) reach majority", rf.me, myTerm)
				// commit point, can apply to the state machine,
				// need to make sure history log applied successfully.
				rf.mu.Lock()
				rf.commitIdx = Max(rf.commitIdx, curLogIdx)
				rf.mu.Unlock()
				rf.commitCond.Signal()
				break
			}
		}
	}(successCh)

	if rf.role == RaftRoleLeader {
		Debug(dTrace, "S%d(T%d) received client call log %v", rf.me, rf.currentTerm, rf.log[len(rf.log)-1])
	}
	return curLogIdx, rf.currentTerm, rf.role == RaftRoleLeader
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

// checkTerm will check the term and current term with lock held.
func (rf *Raft) checkTerm(term int, serverIdx int) bool {
	if term > rf.currentTerm {
		Debug(dTerm, "S%d(%d) meet S%d(T%d) revert to follower",
			rf.me, rf.currentTerm, serverIdx, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.role = RaftRoleFollower
		return true
	}
	return false
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) commitLog(cond *sync.Cond) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIdx <= rf.lastApplied {
			cond.Wait()
		}
		// commit log
		for rf.commitIdx > rf.lastApplied {
			rf.mu.Unlock()
			rf.lastApplied++
			Debug(dCommit, "S%d committing log%d %v", rf.me, rf.lastApplied, rf.log[rf.lastApplied])
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Data,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}

		rf.mu.Unlock()
	}
}

// timeoutTicker will trigger a election every timeoutMS.
func (rf *Raft) timeoutTicker(trigger chan bool) {
	for !rf.killed() {
		timeoutMS := rand.Int63n(electionTimeoutMaxMS-electionTimeoutMinMS) + electionTimeoutMinMS
		timeoutMSDuration := time.Duration(timeoutMS) * time.Millisecond
		time.Sleep(timeoutMSDuration)

		rf.mu.Lock()
		lastHeartBeat := rf.lastHeartBeat
		role := rf.role
		rf.mu.Unlock()

		if time.Since(lastHeartBeat) > timeoutMSDuration &&
			role != RaftRoleLeader {
			Debug(dTimer, "S%d election timeout, last heartbeat %v, elapse %v, timeout interval %v", rf.me, lastHeartBeat, time.Since(lastHeartBeat), timeoutMSDuration)
			trigger <- true
		}
	}
}

// runElection will run a new election when receive a trigger.
func (rf *Raft) runElection(trigger chan bool) {
	for !rf.killed() {
		if <-trigger {
			if rf.role == RaftRoleLeader || rf.killed() {
				continue
			}
			go rf.tryElection()
		}
	}
}

func (rf *Raft) heartbeat(myTerm int) {
	for rf.role == RaftRoleLeader {
		commitIdx := rf.commitIdx
		for idx, peer := range rf.peers {
			if idx == rf.me {
				continue
			}
			rf.mu.Lock()
			request := &AppendEntriesArgs{
				Term:            myTerm,
				LeaderId:        rf.me,
				PrevLogIdx:      rf.nextIdx[idx] - 1, // start with a dummy head
				PrevLogTerm:     rf.log[rf.nextIdx[idx]-1].Term,
				Entries:         rf.log[rf.nextIdx[idx]:],
				LeaderCommitIdx: commitIdx,
			}
			rf.mu.Unlock()
			go func(e *labrpc.ClientEnd, idx int) {
				reply := &AppendEntriesReply{}
				if ok := e.Call(RaftRPCAppendENtries, request, reply); ok {
					rf.mu.Lock()
					rf.checkTerm(reply.Term, idx)
					rf.mu.Unlock()
				}
			}(peer, idx)
		}

		// last bulletin point in fig.2
		rf.mu.Lock()
		oldCommitIdx := rf.commitIdx
		for i := oldCommitIdx + 1; i < len(rf.log); i++ {
			matchCnt := 1
			for _, followerMatchIdx := range rf.matchIdx {
				if followerMatchIdx >= i {
					matchCnt++
				}
			}
			if matchCnt > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
				rf.commitIdx = i
			}
		}
		rf.mu.Unlock()
		if oldCommitIdx != rf.commitIdx {
			rf.commitCond.Signal()
		}

		time.Sleep(heartBeatIntervalMS * time.Millisecond)
	}
	Debug(dLeader, "S%d(T%d) is no longer a leader, stop sending heartbeat", rf.me, rf.currentTerm)
}

func (rf *Raft) tryElection() {
	rf.mu.Lock()
	rf.role = RaftRoleCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastHeartBeat = time.Now()
	myTerm := rf.currentTerm
	rf.mu.Unlock()

	Debug(dVote, "S%d start an election with T%d", rf.me, myTerm)
	request := &RequestVoteArgs{
		Term:         myTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	voteCh := make(chan bool)
	for idx, peer := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(e *labrpc.ClientEnd, idx int) {
			defer func() { voteCh <- false }()
			reply := &RequestVoteReply{}
			if ok := e.Call(RaftRPCRequestVote, request, reply); ok {
				voteCh <- reply.VoteGranted
				rf.mu.Lock()
				rf.checkTerm(reply.Term, idx)
				rf.mu.Unlock()
			}
		}(peer, idx)
	}

	// count vote
	voteCnt := 1
	disVoteCnt := 0
	for i := 1; i < len(rf.peers); i++ {
		if <-voteCh {
			voteCnt += 1
		} else {
			disVoteCnt += 1
		}
		if voteCnt > len(rf.peers)/2 ||
			disVoteCnt > len(rf.peers)/2 {
			break
		}
	}

	// TODO (is this true?): It is impossible to have a candidate to win an election and
	// receive a valid heart beat from a leader. In order to receive a majority vote,
	// candidate's term must be higher than the majority, and the majority's term should
	// be at least as high as the current leader.
	// 1. If this candidate's term is larger than the leader, there can not be a valid heart bead.
	// 2. If this candidate's term is smaller than the leader, it can not receive a majority vote.
	if voteCnt > len(rf.peers)/2 && rf.role == RaftRoleCandidate && myTerm == rf.currentTerm {
		Debug(dLeader, "S%d(T%d) is selected as leader", rf.me, rf.currentTerm)
		rf.mu.Lock()
		rf.role = RaftRoleLeader
		// reinitialize leader state
		rf.nextIdx = make([]int, len(rf.peers))
		rf.matchIdx = make([]int, len(rf.peers))
		for idx := range rf.nextIdx {
			// initialize to leader last log index+1
			rf.nextIdx[idx] = len(rf.log)
			// initialize to 0, increase monotonically
			rf.matchIdx[idx] = 0
		}
		rf.mu.Unlock()
		// start heart beat, stop when server is no longer a leader.
		go rf.heartbeat(myTerm)
	} else {
		// lose election, revert to follower
		rf.mu.Lock()
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
	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		role:            RaftRoleFollower,
		votedFor:        -1,
		lastHeartBeat:   time.Now(),
		electionTrigger: make(chan bool),
		applyCh:         applyCh,
		currentTerm:     0,
		log:             []LogEntry{{-1, 0} /*dummy head*/},
		commitIdx:       0,
		lastApplied:     0,
	}
	rf.commitCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// run an election when timeout ticker goes off.
	go rf.timeoutTicker(rf.electionTrigger)
	go rf.runElection(rf.electionTrigger)
	go rf.commitLog(rf.commitCond)

	return rf
}
