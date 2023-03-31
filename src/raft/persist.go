package raft

import (
	"bytes"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// persist persists Raft states on disk with lock held
func (rf *Raft) persist() {
	buff := new(bytes.Buffer)
	e := labgob.NewEncoder(buff)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftState := buff.Bytes()
	rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err := d.Decode(&rf.currentTerm); err != nil {
		Debug(dPersist, "S%d fail to read currentTerm from persisted data", rf.me)
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		Debug(dPersist, "S%d fail to read votedFor from persisted data", rf.me)
	}
	if err := d.Decode(&rf.log); err != nil {
		Debug(dPersist, "S%d fail to read log from persisted data", rf.me)
	}
	Debug(dPersist, "S%d read persistent states, term %d, votedFor %d, log %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
}
