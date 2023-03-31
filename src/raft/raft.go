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
	rf.checkTerm(args.Term, args.CandidateId)

	reply.VoteGranted = false
	Debug(dVote, "S%d(T%d) receive vote request from S%d(T%d), voted for S%v",
		rf.me, myTerm, args.CandidateId, args.Term, rf.votedFor)
	if args.Term < myTerm { // reject vote
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
		rf.persist()
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
	reply.Term = myTerm
	reply.ConflictingTerm = -1
	reply.LogLen = len(rf.log)
	rf.checkTerm(args.Term, args.LeaderId)
	// either initiate by  heartbeat() of Start().
	Debug(dLog, "S%d(T%d) receive AppendEntries val: %v, cur log len: %d", rf.me, myTerm, args, len(rf.log))
	if args.Term == myTerm {
		rf.lastHeartBeat = time.Now()
		Debug(dTimer, "S%d(T%d) update heartbeat %v", rf.me, myTerm, rf.lastHeartBeat)
	}

	if args.Term < myTerm || // invalid term
		len(rf.log)-1 < args.PrevLogIdx || // follower does not have that many log entry as leader does
		rf.log[args.PrevLogIdx].Term != args.PrevLogTerm { // invalid entry
		reply.Success = false
		// replying with the conflicting term and idx for leader to set nextIdx faster.
		if len(rf.log) > args.PrevLogIdx && rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
			reply.ConflictingTerm = rf.log[args.PrevLogIdx].Term
			idx := args.PrevLogIdx
			for ; 0 <= idx; idx-- {
				if rf.log[idx].Term != reply.ConflictingTerm {
					break
				}
			}
			reply.FirstConflictingLogIdx = idx + 1
		}
		Debug(dLog2, "S%d(T%d) reject request xTerm %d xIdx %d xLen %d, log%d(T%d)",
			rf.me, rf.currentTerm, reply.ConflictingTerm, reply.FirstConflictingLogIdx,
			reply.LogLen, len(rf.log)-1, rf.log[len(rf.log)-1].Term)

		return
	}

	reply.Success = true
	if rf.role == RaftRoleCandidate {
		// recognize this leader, give up the ongoing election on its term (if there is one).
		rf.role = RaftRoleFollower
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
				rf.persist()
				isMisMatch = true
			}
		}

		if isMisMatch { // mismatch happens, append everything from the args that not already in log.
			rf.log = append(rf.log, entry)
			rf.persist()
		}
	}

	if args.LeaderCommitIdx > rf.commitIdx {
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
	// only leader can call applyEntries on followers
	if rf.role != RaftRoleLeader {
		curTerm := rf.currentTerm
		rf.mu.Unlock()
		return -1, curTerm, false
	}

	newLogEntry := LogEntry{Data: command, Term: rf.currentTerm}
	// leader first append log entry to its local log.
	rf.log = append(rf.log, newLogEntry)
	rf.persist()
	curLogIdx := len(rf.log) - 1
	myTerm := rf.currentTerm
	rf.mu.Unlock()

	Debug(dTrace, "S%d(T%d) received client call %v", rf.me, rf.currentTerm, rf.log[len(rf.log)-1])
	successCh := make(chan bool)
	for sId, peer := range rf.peers {
		if sId == rf.me {
			continue
		}
		go func(e *labrpc.ClientEnd, sId int, successCh chan bool) {
			ok := false
			reply := &AppendEntriesReply{}
			// There are 3 possible results:
			// 1. [Invalid leader] follower has a larger term, forcing leader to step down and revert to a follower.
			// 2. [Network partition] leader can not reach follower, leader will retry indefinitely. (FIXME: is this necessary? will do by a heartbeat)
			// 3. [Log Inconsistency] follower will reject the request, leader will decrement its prev log index until matched.
			for !reply.Success && rf.role == RaftRoleLeader {
				reply.Success = false
				reply.Term = 0
				rf.mu.Lock()
				request := &AppendEntriesArgs{
					Term:            myTerm,
					LeaderId:        rf.me,
					PrevLogIdx:      rf.nextIdx[sId] - 1, // start with a dummy head
					PrevLogTerm:     rf.log[rf.nextIdx[sId]-1].Term,
					Entries:         rf.log[rf.nextIdx[sId]:],
					LeaderCommitIdx: rf.commitIdx,
				}
				rf.mu.Unlock()

				ok = e.Call(RaftRPCAppendENtries, request, reply)
				if ok && rf.role == RaftRoleLeader {
					if myTerm != reply.Term {
						reply.Success = false
						break
					}
					rf.mu.Lock()
					rf.checkTerm(reply.Term, sId)
					if reply.Term == myTerm {
						if reply.Success {
							rf.matchIdx[sId] = curLogIdx
							rf.nextIdx[sId] = curLogIdx + 1
						} else {
							// follower's log is too short
							rf.nextIdx[sId] = Min(reply.LogLen, rf.nextIdx[sId])
							if reply.ConflictingTerm != -1 {
								ok, lastIdx := lastLogIdxWithTerm(rf.log, reply.ConflictingTerm)
								if ok {
									rf.nextIdx[sId] = lastIdx
								} else {
									rf.nextIdx[sId] = reply.FirstConflictingLogIdx
								}
							}
							Debug(dLog2, "S%d, xTerm %d xIdx %d xLen %d, nextIdx[%d] %d",
								rf.me, reply.ConflictingTerm, reply.FirstConflictingLogIdx, reply.LogLen, sId, rf.nextIdx[sId])
						}
					}
					rf.mu.Unlock()
				}
			}
			successCh <- reply.Success
		}(peer, sId, successCh)
	}

	go func(successCh chan bool) {
		successCnt := 1 // self is consider a success
		for i := 1; i < len(rf.peers) && rf.role == RaftRoleLeader; i++ {
			if <-successCh {
				successCnt++
			}
			if successCnt > len(rf.peers)/2 {
				Debug(dTrace, "S%d(T%d) log%d reach majority", rf.me, myTerm, curLogIdx)
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
	Debug(dInfo, "S%d(T%d) shut down", rf.me, rf.currentTerm)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// commitLog is a backend running goroutine, that will be awoken when commitIdx is changed.
// It will try to commit everything between lastApplied and commitIdx to the state machine.
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
		lastHeartBeat:   time.Now(),
		electionTrigger: make(chan bool),
		applyCh:         applyCh,
		// persist state begin
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{-1, 0} /*dummy head*/},
		// persist state end
		commitIdx:   0,
		lastApplied: 0,
	}
	rf.commitCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	Debug(dInfo, "S%d(T%d) restarted, peer cnt %d", rf.me, rf.currentTerm, len(rf.peers))

	// run an election when timeout ticker goes off.
	go rf.timeoutTicker(rf.electionTrigger)
	go rf.runElection(rf.electionTrigger)
	go rf.commitLog(rf.commitCond)

	return rf
}
