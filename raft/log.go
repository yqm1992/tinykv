// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// log entries with index <= truncatedIndex are truncated
	truncatedIndex uint64

	// term of the last truncated entry(entry.Index = truncatedIndex)
	truncatedTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err1 := storage.FirstIndex()
	if err1 != nil {
		log.Error(err1)
		return nil
	}
	lastIndex, err2 := storage.LastIndex()
	if err2 != nil {
		log.Error(err2)
		return nil
	}
	entries, err3 := storage.Entries(firstIndex, lastIndex+1)
	if err3 != nil {
		log.Error(err3)
		return nil
	}
	truncatedIndex := firstIndex - 1
	truncatedTerm, err4 := storage.Term(truncatedIndex)
	if err4 != nil {
		log.Error(err4)
		return nil
	}

	raftLog := RaftLog{
		storage: storage,
		entries: entries,
		stabled: lastIndex,
		truncatedIndex: truncatedIndex,
		truncatedTerm: truncatedTerm,
	}
	return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled == l.truncatedIndex {
		return l.entries
	}

	stabledOffset, err := l.Offset(l.stabled)
	if err != nil {
		log.Fatal(err)
	}
	return l.entries[stabledOffset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if (len(l.entries) == 0 || l.applied == l.committed){
		return []pb.Entry{}
	}

	firstIndex := l.entries[0].Index
	committedOffset := uint64(l.committed - firstIndex)

	if (l.applied < firstIndex){ // l.applied == 0
		return l.entries[0:committedOffset+1]
	}
	appliedOffset := uint64(l.applied - firstIndex)

	return l.entries[appliedOffset+1:committedOffset+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.truncatedIndex
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if lastIndex := l.LastIndex(); i < l.truncatedIndex || i > lastIndex {
		return 0, fmt.Errorf("the given index = %d exceeds the valid range [%d, %d]", i, l.truncatedIndex, lastIndex)
	}
	if i == l.truncatedIndex {
		return l.truncatedTerm, nil
	}
	offset, err := l.Offset(i)
	if err != nil {
		log.Fatal(err)
	}
	return l.entries[offset].Term, nil
}

// Offset return the offset of the given index
func (l *RaftLog) Offset(i uint64) (uint64, error) {
	if len(l.entries) == 0 {
		return 0, fmt.Errorf("the log is empty")
	}
	firstIndex := l.entries[0].Index
	lastIndex := l.LastIndex()
	if i < firstIndex || i > lastIndex {
		return 0, fmt.Errorf("the given index = %d exceeds the valid range [%d, %d]", i, firstIndex, lastIndex)
	}
	return i - firstIndex, nil
}

// dropEntries drop the entries with index >= startIndex
func (l *RaftLog) dropEntries(startIndex uint64) {
	startOffset, err := l.Offset(startIndex)
	if err != nil {
		log.Fatal(err)
	}
	l.entries = l.entries[:startOffset]
	// update stableIndex
	l.stabled = min(l.stabled, l.LastIndex())
}