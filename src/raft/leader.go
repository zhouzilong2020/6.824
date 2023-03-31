package raft

import (
	"math/rand"
	"time"

	"6.5840/labrpc"
)

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
	defer Debug(dLeader, "S%d(T%d) is no longer a leader, stop sending heartbeat", rf.me, rf.currentTerm)
	for rf.role == RaftRoleLeader && myTerm == rf.currentTerm && !rf.killed() {
		go rf.sendAppendEntries(myTerm, false)
		// last bulletin point in fig.2
		// only commit a previous term's log if there is one log in current term is committed.
		if myTerm == rf.currentTerm {
			rf.mu.Lock()
			oldCommitIdx := rf.commitIdx
			for i := len(rf.log) - 1; oldCommitIdx < i; i-- {
				matchCnt := 1
				for sid := range rf.matchIdx {
					if rf.matchIdx[sid] >= i && sid != rf.me {
						matchCnt++
					}
				}
				if matchCnt > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
					Debug(dLog2, "S%d(T%d) bump up commitIdx to %d", rf.me, rf.currentTerm, i)
					rf.commitIdx = i
					break
				}
			}
			rf.mu.Unlock()
			if oldCommitIdx != rf.commitIdx {
				rf.commitCond.Signal()
			}
		}

		time.Sleep(heartBeatIntervalMS * time.Millisecond)
	}
}

func (rf *Raft) tryElection() {
	rf.mu.Lock()
	rf.role = RaftRoleCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.lastHeartBeat = time.Now()
	myTerm := rf.currentTerm
	rf.mu.Unlock()

	Debug(dVote, "S%d(T%d) start an election, last log %d(T%d)", rf.me, myTerm, len(rf.log), rf.log[len(rf.log)-1].Term)
	request := &RequestVoteArgs{
		Term:         myTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	voteCh := make(chan bool)
	isDiff := false
	for idx, peer := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(e *labrpc.ClientEnd, idx int) {
			defer func() { voteCh <- false }()
			reply := &RequestVoteReply{}
			if ok := e.Call(RaftRPCRequestVote, request, reply); ok {
				rf.mu.Lock()
				rf.checkTerm(reply.Term, idx)
				rf.mu.Unlock()
				if reply.Term == myTerm {
					voteCh <- reply.VoteGranted
				} else {
					isDiff = true
					voteCh <- false
				}
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
			disVoteCnt > len(rf.peers)/2 || isDiff {
			break
		}
	}

	// win the election only when the following conditions hold:
	// 1. receive a majority vote.
	// 2. term has not changed since the beginning of the election.
	// 3. still a candidate.
	if voteCnt > len(rf.peers)/2 && rf.role == RaftRoleCandidate && myTerm == rf.currentTerm && !isDiff {
		Debug(dLeader, "S%d(T%d) is selected as leader", rf.me, rf.currentTerm)
		rf.mu.Lock()
		rf.role = RaftRoleLeader
		// reinitialize leader state
		rf.nextIdx = make([]int, len(rf.peers))
		rf.matchIdx = make([]int, len(rf.peers))
		for idx := range rf.nextIdx {
			// initialize to leader last log index+1.
			rf.nextIdx[idx] = len(rf.log)
			// initialize to 0, increase monotonically.
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
