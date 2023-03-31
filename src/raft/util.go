package raft

// checkTerm will check the term and current term with lock held.
func (rf *Raft) checkTerm(term int, serverIdx int) bool {
	if term > rf.currentTerm {
		Debug(dTerm, "S%d(%d) meet S%d(T%d) revert to follower",
			rf.me, rf.currentTerm, serverIdx, term)
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
		rf.role = RaftRoleFollower
		return true
	}
	return false
}

// lastLogIdxWithTerm use binary search to find the last entry that has the same term as input.
// if there is none, return false.
func lastLogIdxWithTerm(log []LogEntry, term int) (bool, int) {
	l, r := 0, len(log)-1
	for l < r {
		mid := (l + r) / 2
		if log[mid].Term > term {
			r = mid - 1
		} else if log[mid].Term < term {
			l = mid + 1
		} else {
			for log[mid].Term == term && mid < len(log)-1 {
				mid++
			}
			return true, mid - 1
		}
	}

	return false, -1
}
