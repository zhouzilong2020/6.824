package raft

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
