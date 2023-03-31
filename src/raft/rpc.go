package raft

import (
	"time"

	"6.5840/labrpc"
)

// sendAppendEntries send append entry to peers, and adjust nextIdx & matchIdx according to the reply msg.
func (rf *Raft) sendAppendEntries(successCh chan bool, retry bool) {
	myTerm := rf.currentTerm
	for sId, peer := range rf.peers {
		if sId == rf.me {
			continue
		}
		go func(e *labrpc.ClientEnd, sId int) {
			reply := &AppendEntriesReply{}
			// !retry means heartBeat msg
			if !retry && !time.Now().After(rf.lastContact[sId].Add(heartBeatIntervalMS*time.Millisecond)) {
				return
			}
			// keep retrying if appendEntries is rejected by followers.
			for !rf.killed() && !reply.Success && rf.role == RaftRoleLeader && myTerm == rf.currentTerm {
				rf.mu.Lock()
				curLogIdx := len(rf.log) - 1
				request := &AppendEntriesArgs{
					Term:            myTerm,
					LeaderId:        rf.me,
					PrevLogIdx:      rf.nextIdx[sId] - 1, // start with a dummy head
					PrevLogTerm:     rf.log[rf.nextIdx[sId]-1].Term,
					Entries:         rf.log[rf.nextIdx[sId]:],
					LeaderCommitIdx: rf.commitIdx,
				}
				rf.lastContact[sId] = time.Now()
				rf.mu.Unlock()

				// There are 3 possible results:
				// 1. [Invalid leader] follower has a larger term, forcing leader to step down and revert to a follower.
				// 2. [Network partition] leader can not reach follower, leader will retry indefinitely. (FIXME: is this necessary? will do by a heartbeat)
				// 3. [Log Inconsistency] follower will reject the request, leader will decrement its prev log index until matched.
				if ok := e.Call(RaftRPCAppendENtries, request, reply); ok {
					rf.mu.Lock()
					rf.checkTerm(reply.Term, sId)
					rf.mu.Unlock()
				}
				if reply.Term == myTerm {
					Debug(dLog2, "S%d(T%d), reply from S%d xTerm %d xIdx %d xLen %d, nextIdx[%d] %d, master log len %d",
						rf.me, myTerm, sId, reply.ConflictingTerm, reply.FirstConflictingLogIdx, reply.LogLen, sId, rf.nextIdx[sId], curLogIdx)
					rf.mu.Lock()
					if reply.Success {
						rf.matchIdx[sId] = curLogIdx
						rf.nextIdx[sId] = curLogIdx + 1
					} else {
						// optimization for speedup catch up
						rf.nextIdx[sId] = Min(reply.LogLen, rf.nextIdx[sId])
						if reply.ConflictingTerm != -1 {
							ok, lastIdx := lastLogIdxWithTerm(rf.log, reply.ConflictingTerm)
							if ok {
								rf.nextIdx[sId] = lastIdx
							} else {
								rf.nextIdx[sId] = reply.FirstConflictingLogIdx
							}
						}
					}
					rf.mu.Unlock()
				}
				if !retry {
					break
				}
			}
			if successCh != nil {
				successCh <- reply.Success
			}
		}(peer, sId)
	}
}
