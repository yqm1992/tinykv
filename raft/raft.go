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
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := Raft{id: c.ID, Term: 0, State: StateFollower, heartbeatTimeout: c.HeartbeatTick, electionTimeout: c.ElectionTick}
	if r.RaftLog = newLog(c.Storage); r.RaftLog == nil {
		return nil
	}
	// InitialState() couldn't be called in newLog(), which could cause nil pointer dereference
	hardState, _, err2 := c.Storage.InitialState()
	if err2 != nil {
		log.Error(err2)
		return nil
	}

	r.RaftLog.applied = max(c.Applied, r.RaftLog.truncatedIndex)
	r.RaftLog.committed = hardState.Commit
	r.Term = hardState.Term
	r.Vote = hardState.Vote

	// check if the index order is satisfied: truncatedIndex <= appliedIndex <= committedIndex <= stabledIndex <= lastIndex
	if r.RaftLog.truncatedIndex > r.RaftLog.applied {
		log.Errorf("truncatedIndex(%v) > appliedIndex(%v)", r.RaftLog.truncatedIndex, r.RaftLog.applied)
		return nil
	}
	if  r.RaftLog.applied > r.RaftLog.committed{
		log.Errorf("appliedIndex(%v) > committedIndex(%v)", r.RaftLog.applied, r.RaftLog.committed)
		return nil
	}
	if r.RaftLog.committed > r.RaftLog.stabled {
		log.Errorf("committedIndex(%v) > stabledIndex(%v)", r.RaftLog.committed, r.RaftLog.stabled)
		return nil
	}
	if r.RaftLog.stabled > r.RaftLog.LastIndex() {
		log.Errorf("stabledIndex(%v) > lastIndex(%v)", r.RaftLog.stabled, r.RaftLog.LastIndex())
		return nil
	}

	r.Prs = make(map[uint64]*Progress)
	for _, peer_id := range c.peers{
		r.Prs[peer_id] = &Progress{}
	}
	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader || r.id == to {
		return false
	}
	lastIndex := r.RaftLog.LastIndex()
	peer := r.Prs[to]

	if peer.Next <= r.RaftLog.truncatedIndex {
		msg := pb.Message{
			From: r.id,
			To: to,
			Term: r.Term,
			MsgType: pb.MessageType_MsgSnapshot,
		}
		if r.RaftLog.localSnapshot != nil {
			cacheSnapshot := r.RaftLog.localSnapshot
			snapData := new(rspb.RaftSnapshotData)
			if err := snapData.Unmarshal(cacheSnapshot.Data); err != nil {
				log.Fatal(err)
			}
			contains := false
			for _, peer := range snapData.Region.Peers {
				if peer.Id == to {
					contains = true
					break
				}
			}
			if cacheSnapshot.Metadata.Index >= peer.Next && contains {
				msg.Snapshot = r.RaftLog.localSnapshot
				r.msgs = append(r.msgs, msg)
				return true
			}
		}
		snapShot, err := r.RaftLog.storage.Snapshot()
		if err == ErrSnapshotTemporarilyUnavailable {
			return false
		}
		if err != nil {
			log.Error(err)
			return false
		}
		r.RaftLog.localSnapshot = &snapShot
		msg.Snapshot = r.RaftLog.localSnapshot
		r.msgs = append(r.msgs, msg)
		return true
	}

	// Send Append with no entry to peer
	if peer.Next > lastIndex {
		//return false
		logTerm, _ := r.RaftLog.Term(lastIndex)
		msg := pb.Message{
			From: r.id,
			To: to,
			Term: r.Term,
			MsgType: pb.MessageType_MsgAppend,
			Index: lastIndex,
			LogTerm: logTerm,
			Commit: r.RaftLog.committed,
		}
		r.msgs = append(r.msgs, msg)
		return true
	}

	sendStartOffset := peer.Next - r.RaftLog.entries[0].Index
	sendLen := lastIndex - peer.Next + 1 // should set limit for max length of send entries
	sendEntries := []*pb.Entry{}
	for i := uint64(0); i < sendLen; i++ {
		sendEntries = append(sendEntries, &r.RaftLog.entries[sendStartOffset + i])
	}
	logIndex := peer.Next - 1
	logTerm, _ := r.RaftLog.Term(logIndex)

	msg := pb.Message{
		From: r.id,
		To: to,
		Term: r.Term,
		MsgType: pb.MessageType_MsgAppend,
		Index: logIndex,
		LogTerm: logTerm,
		Entries: sendEntries,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendNoopEntry sends an append RPC with null entry
// should be called after becomeLeader()
func (r *Raft) sendNoopEntry(to uint64){
	if r.State == StateLeader && to != r.id && len(r.RaftLog.entries) != 0 {
		// Get the prev log entry info
		sendIndex := r.RaftLog.LastIndex() - 1
		sendLogTerm, _ := r.RaftLog.Term(sendIndex)

		// Get the appended entry
		entry := r.RaftLog.entries[len(r.RaftLog.entries)-1]
		msg := pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgAppend, Index: sendIndex, LogTerm: sendLogTerm, Entries: []*pb.Entry{&entry}, Commit: r.RaftLog.committed}
		r.msgs = append(r.msgs, msg)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if (r.State == StateLeader && to != r.id){
		r.msgs = append(r.msgs, pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// Check if self is in the raft group
	if _, ok := r.Prs[r.id]; !ok {
		log.Warnf("id = %v: self is not in the raft group %v, ignore tick", r.id, nodes(r))
		return
	}
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if (r.electionElapsed >= r.electionTimeout){
			r.electionElapsed = 0
			//r.msgs = append(r.msgs, pb.Message{From: r.id, To: r.id, Term: r.Term, MsgType: pb.MessageType_MsgHup})
			r.becomeCandidate() // Does this should be done in Step() ?
			r.raiseVote()
		}
	case StateCandidate:
		r.electionElapsed++
		if (r.electionElapsed >= r.electionTimeout){
			r.electionElapsed = 0
			//r.msgs = append(r.msgs, pb.Message{From: r.id, To: r.id, Term: r.Term, MsgType: pb.MessageType_MsgHup})
			r.becomeCandidate()
			r.raiseVote()
		}
	case StateLeader:
		r.heartbeatElapsed++
		if (r.heartbeatElapsed >= r.heartbeatTimeout){
			r.heartbeatElapsed = 0
			for peer_id, _ := range r.Prs {
				r.sendHeartbeat(peer_id)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term < r.Term {
		log.Fatalf("term(%v) < currentTerm(%v)", term, r.Term)
	}
	if lead == r.id {
		log.Fatalf("id = %v can not become follower of itself", r.id)
	}

	r.State = StateFollower
	//r.heartbeatElapsed = 0

	// reset Vote info only when term has changed
	if term > r.Term {
		r.Vote = None
	}

	r.electionElapsed = 0
	r.electionTimeout = rand.Int()%10 + 10
	r.Term, r.Lead = term, lead

	// log should be printed when term will not be changed
	log.Infof("Id = %v becomes follower (lead = %v) in term %v", r.id, r.Lead, r.Term)
}

// bcastMessage broadcast message to peers
func (r *Raft) bcastMessage(m pb.Message) {
	for peer_id, _ := range r.Prs{
		if peer_id == r.id {
			continue
		}
		m.To = peer_id
		r.msgs = append(r.msgs, m)
	}
}

// raiseVote sends vote request RPC to peers
func (r *Raft) raiseVote(){
	if r.State != StateCandidate{
		log.Fatalf("only candidate can raise vote, current state: %v", r.State)
	}

	if len(r.Prs) == 1{
		r.becomeLeader()
		return
	}

	// Broadcast request vote message
	msg := pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVote}
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(lastIndex)
	msg.Index = lastIndex
	msg.LogTerm = logTerm
	r.bcastMessage(msg)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		log.Fatalf("leader can't become candidate")
	}

	r.State = StateCandidate
	r.Term++
	r.electionTimeout = rand.Int()%10 + 10
	r.electionElapsed = 0
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.msgs = make([]pb.Message, 0)

	// Vote for self
	r.Vote = r.id
	r.votes[r.id] = true

	// log should be printed when term will not be changed
	log.Infof("Id = %v becomes candidate in term %v", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateCandidate {
		log.Fatalf("only candidate can become leader, current state = %v", r.State)
	}

	log.Infof("Id = %v becomes leader in term %v", r.id, r.Term)

	r.State = StateLeader
	r.Lead = r.id
	r.leadTransferee = None
	r.heartbeatElapsed = 0
	lastIndex := r.RaftLog.LastIndex()
	for peer_id, peer := range r.Prs{
		if peer_id == r.id {
			peer.Match = lastIndex
			peer.Next = lastIndex + 1
			continue
		}
		peer.Match = 0
		peer.Next = lastIndex+1
	}

	prevLogIndex := r.RaftLog.LastIndex()
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)

	// Append a noop entry
	noopEntry := pb.Entry{Term: r.Term, Index: prevLogIndex+1, Data: nil}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)

	// Update self's Match and Next info
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// Commit the noop entry immediately if there is only one raft entity
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}

	// Broadcast append entry message
	msg := pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgAppend, Index: prevLogIndex, LogTerm: prevLogTerm, Entries: []*pb.Entry{&noopEntry}, Commit: r.RaftLog.committed}
	r.bcastMessage(msg)
}

// StepFollower the entrance of handle message for follower
// It can only be called by Step()
func (r *Raft) StepFollower(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.raiseVote()
		return
	case pb.MessageType_MsgHeartbeat:
		if r.Lead != m.From {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgAppend:
		if r.Lead != m.From {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		if r.Lead != m.From {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	case pb.MessageType_MsgTransferLeader:
		if m.From != r.id {
			log.Warnf("id = %v is not leader, can not handle the TransferLeader message from %v", r.id, m.From)
			return
		}
		if r.Lead != None {
			msg := pb.Message{To: r.Lead, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgTransferLeader}
			r.msgs = append(r.msgs, msg)
		}
	}
}

// Quorum returns the quorum of raft group
func (r *Raft) Quorum() int {
	return len(r.Prs)/2 + 1
}

// IsAcceptedByQuorum returns if an entry is accepted by quorum
func (r *Raft) IsAcceptedByQuorum(index uint64) bool {
	count := 0
	for _, peer := range r.Prs{
		if peer.Match >= index {
			count++
		}
	}
	if count >= r.Quorum() {
		return true
	}
	return false
}

// UpdateCommitted updates the committed, returns true if committed is updated
func (r *Raft) UpdateCommitted() bool {
	if r.State != StateLeader {
		log.Errorf("id = %v is not the leader, can't call UpdateCommitted()", r.id)
		return false
	}
	committedChanged := false
	for idx := r.RaftLog.LastIndex(); idx > r.RaftLog.committed; idx--{
		// can only commit log entry of current term
		if logTerm, _ := r.RaftLog.Term(idx); logTerm!= r.Term{
			break
		}
		if r.IsAcceptedByQuorum(idx) {
			r.RaftLog.committed = idx
			committedChanged = true
		}
	}
	return committedChanged
}

// StepCandidate the entrance of handle message for candidate
// It can only be called by Step()
func (r *Raft) StepCandidate(m pb.Message){
	switch m.MsgType {
	// local message, doesn't need to check term info
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.raiseVote()
	// message from peer, need to check term info
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
		r.msgs = append(r.msgs, msg)
	case pb.MessageType_MsgRequestVoteResponse:
		if _, ok := r.votes[m.From]; !ok {
			r.votes[m.From] = !m.Reject
		} else {
			return
		}
		acceptCount := 0
		for _, vote := range r.votes{
			if vote{
				acceptCount++
			}
		}
		quorum := r.Quorum()
		if acceptCount >= quorum {
			r.becomeLeader()
		} else if len(r.votes) - acceptCount >= quorum {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.Step(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.Step(m)
	}
}

// transferLeader transfers leader to peer
func (r *Raft) transferLeader(to uint64) {
	if r.State != StateLeader {
		return
	}
	if _, ok := r.Prs[to]; !ok {
		log.Warnf("transferee peer (id = %v) is not in the raft group", to)
		return
	}
	if to == r.id {
		log.Warnf("id = %v is already a leader, just set leadTransferee to None", r.id)
		r.leadTransferee = None
		return
	}
	r.leadTransferee = to
	if r.Prs[r.leadTransferee].Match == r.RaftLog.LastIndex() {
		msg := pb.Message{To: r.leadTransferee, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgTimeoutNow}
		r.msgs = append(r.msgs, msg)
		log.Infof("id = %v(leader) prepares to transfer leader to id = %v, sends MsgTimeoutNow message", r.id, r.leadTransferee)
	} else {
		r.sendAppend(r.leadTransferee)
		log.Infof("id = %v(leader) prepares to transfer leader to id = %v, sends append message", r.id, r.leadTransferee)
	}
}

// StepLeader the entrance of handle message for leader
// It can only be called by Step()
func (r *Raft) StepLeader(m pb.Message){
	switch m.MsgType {
	// local message, doesn't need to check term info
	case  pb.MessageType_MsgBeat:
		for peer_id, _ := range r.Prs {
			if r.id == peer_id {
				continue
			}
			r.sendHeartbeat(peer_id)
		}
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgPropose:
		if r.leadTransferee != None {
			log.Warnf("id = %v is in progress of transferring leader to %v, can not handle propose message", r.id, r.leadTransferee)
			return
		}
		r.handleAppendPropose(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
		r.msgs = append(r.msgs, msg)
	case pb.MessageType_MsgTransferLeader:
		r.transferLeader(m.From)
	}
}

func isLocalMessage(m pb.Message) bool {
	if m.MsgType == pb.MessageType_MsgHup || m.MsgType == pb.MessageType_MsgPropose || m.MsgType == pb.MessageType_MsgBeat || m.MsgType == pb.MessageType_MsgTransferLeader {
		return true
	}
	return false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//if _, ok := r.Prs[m.From]; !ok{
	//	return nil
	//}
	// Check if self is in the raft group
	if _, ok := r.Prs[r.id]; !ok {
		if m.MsgType != pb.MessageType_MsgHeartbeat && m.MsgType != pb.MessageType_MsgAppend &&  m.MsgType != pb.MessageType_MsgSnapshot {
			log.Warnf("id = %v is not in the raft group %v, can not handle the message [type: %v]", r.id, nodes(r), m.MsgType)
			return nil
		}
	}
	if !isLocalMessage(m) {
		if r.Term > m.Term {
			if m.MsgType == pb.MessageType_MsgRequestVote {
				msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
				r.msgs = append(r.msgs, msg)
			}
			return nil
		} else if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
	}

	switch r.State {
	case StateFollower:
		r.StepFollower(m)
	case StateCandidate:
		r.StepCandidate(m)
	case StateLeader:
		r.StepLeader(m)
	}
	return nil
}

// handleVoteRequest handle Vote RPC request for follower
func (r *Raft)handleVoteRequest(m pb.Message){
	if r.State != StateFollower || r.Term != m.Term || m.MsgType != pb.MessageType_MsgRequestVote {
		return
	}
	msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse}
	if (None == r.Vote){
		localLastIndex := r.RaftLog.LastIndex()
		localLogTerm, _ := r.RaftLog.Term(localLastIndex)

		if (localLogTerm < m.LogTerm || (localLogTerm == m.LogTerm && localLastIndex <= m.Index)){
			msg.Reject = false
			r.Vote = m.From
			log.Infof("vote: (id = %v) ---> (id = %v) in term %v", r.id, r.Vote, r.Term)
		} else{
			msg.Reject = true
		}
	} else if (m.From == r.Vote){
		msg.Reject = false
	} else {
		msg.Reject = true
	}
	r.msgs = append(r.msgs, msg)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower || r.Term != m.Term || m.MsgType != pb.MessageType_MsgAppend {
		return
	}
	if len(m.Entries) > 0 && m.Index+uint64(len(m.Entries)) != m.Entries[len(m.Entries)-1].Index {
		log.Errorf("the index order of m.entries is not correct")
		return
	}
	msg := pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Reject: true}
	lastIndex := r.RaftLog.LastIndex()

	// Check if entry (m.Index, m.Term) exists in local log
	if m.Index > lastIndex {
		msg.Index = lastIndex
		msg.LogTerm = r.RaftLog.MustGetTerm(msg.Index)
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Index >= r.RaftLog.truncatedIndex {
		logTerm := r.RaftLog.MustGetTerm(m.Index)
		if logTerm != m.LogTerm {
			if m.Index <= r.RaftLog.committed {
				log.Errorf("crash index should bigger than committed index, now crash index = %v, committed index = %v", m.Index, r.RaftLog.committed)
				return
			}
			// Do not drop entries here, the crash entries will be dropped in stage of appending log
			// Reply reject with the biggest possible match index
			msg.Index = m.Index - 1
			msg.LogTerm = r.RaftLog.MustGetTerm(msg.Index)
			r.msgs = append(r.msgs, msg)
			return
		}
	}
	i := 0
	// Check if there is conflict in appending operation
	for ; i < len(m.Entries); i++ {
		curIndex := m.Entries[i].Index
		if curIndex < r.RaftLog.truncatedIndex {
			continue
		}
		if curIndex > lastIndex {
			break
		}
		// truncatedIndex <= curIndex <= lastIndex
		logTerm := r.RaftLog.MustGetTerm(curIndex)
		// conflict position
		if m.Entries[i].Term != logTerm {
			if curIndex <= r.RaftLog.committed {
				log.Errorf("crash index should bigger than committed index, now crash index = %v, committed index = %v", curIndex, r.RaftLog.committed)
				return
			}
			// Drop the entries after conflict position (include curIndex)
			r.RaftLog.dropEntries(curIndex)
			break
		}
	}
	// Append entries
	for ; i < len(m.Entries); i++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
	}
	// Compute current max match index
	maxMatchIndex := max(m.Index, r.RaftLog.committed)
	if len(m.Entries) > 0 {
		maxMatchIndex = max(m.Entries[len(m.Entries)-1].Index, r.RaftLog.committed)
	}
	maxMatchLogTerm := r.RaftLog.MustGetTerm(maxMatchIndex)
	// Reply a accept message
	msg.Index = maxMatchIndex
	msg.LogTerm = maxMatchLogTerm
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
	// Update committed
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(maxMatchIndex, m.Commit)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateLeader {
		return
	}
	if r.Term != m.Term || m.MsgType != pb.MessageType_MsgHeartbeat {
		return
	}
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}

	r.electionElapsed = 0
	msg := pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle HeartbeatResponse RPC request
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.State != StateLeader || r.Term != m.Term || m.MsgType != pb.MessageType_MsgHeartbeatResponse {
		return
	}
	r.sendAppend(m.From)
}

// handleAppendPropose handle AppendPropose RPC request
func (r *Raft) handleAppendPropose(m pb.Message){
	if r.State != StateLeader || m.MsgType != pb.MessageType_MsgPropose {
		return
	}
	if len(m.Entries) == 0 {
		return
	}
	var sendMessageEntries []*pb.Entry
	prevLastIndex := r.RaftLog.LastIndex()
	curIndex := prevLastIndex + 1

	// append data to the local log
	for _, entry := range m.Entries{
		tempEntry := pb.Entry{
			EntryType: entry.EntryType,
			Term: r.Term,
			Index: curIndex,
			Data: entry.Data,
		}
		r.RaftLog.entries = append(r.RaftLog.entries, tempEntry)
		sendMessageEntries = append(sendMessageEntries, &tempEntry)
		curIndex++
	}

	// Update self's Match and Next
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// Commit entries immediately if there is only one raft entity
	if len(r.Prs) == 1{
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}

	// Broadcast the append operation message to peers
	for peer_id, _ := range r.Prs {
		if (peer_id == r.id){
			continue
		}
		r.sendAppend(peer_id)
	}
}

// handleAppendResponse handle AppendResponse RPC request
func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.State != StateLeader || r.Term != m.Term || m.MsgType != pb.MessageType_MsgAppendResponse {
		return
	}
	// Peer doesn't accept append operation
	if (m.Reject == true){
		peer := r.Prs[m.From]
		possibleMatchIndex := m.Index
		// possibleMatchIndex == peer.Match is normal, eg: possibleMatchIndex == 0 and peer.Match == 0
		if possibleMatchIndex < peer.Match {
			log.Errorf("id = %v receives RejectAppend response from peer id = %v, m.Index(%v) < peer.Match(%v)", r.id, m.From, possibleMatchIndex, peer.Match)
		} else if possibleMatchIndex > r.RaftLog.LastIndex() {
			log.Errorf("id = %v receives RejectAppend response from peer id = %v, m.Index(%v) > lastIndex(%v)", r.id, m.From, possibleMatchIndex, r.RaftLog.LastIndex())
		} else {
			// maybe needs to update Next
			peer.Next = possibleMatchIndex+1
		}
		// Send Append again
		r.sendAppend(m.From)
		return
	}

	// Update peer's Match and Next
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// update committed
	commitedChanged := r.UpdateCommitted()

	if m.From == r.leadTransferee {
		r.transferLeader(m.From)
		return
	}

	// Continue to send Append if peer's log is different with leader's
	if commitedChanged == false && r.Prs[m.From].Next <= r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}

	// Send Append to all peers if committed id has changed
	if commitedChanged == true {
		for peer_id, _ := range r.Prs {
			if peer_id == r.id {
				continue
			}
			r.sendAppend(peer_id)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// TODO: need to check m.Term and m.MsgType ?
	if r.State != StateFollower {
		return
	}
	if m.Snapshot == nil {
		log.Errorf("snapshot is nil")
		return
	}
	log.Infof("id= %v, rcv snapshot(index=%v), trunc=%v, applied=%v, commit=%v, last=%v", r.id, m.Snapshot.Metadata.Index, r.RaftLog.truncatedIndex, r.RaftLog.applied, r.RaftLog.committed, r.RaftLog.LastIndex())
	if m.Snapshot.Metadata.Index <= r.RaftLog.committed {
		return
	}
	if m.Snapshot.Metadata.Index <= r.RaftLog.LastIndex(){
		snapOffset, _ := r.RaftLog.Offset(m.Snapshot.Metadata.Index)
		r.RaftLog.entries = r.RaftLog.entries[snapOffset+1:]
	} else {
		r.RaftLog.entries = []pb.Entry{}
	}
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.truncatedIndex = m.Snapshot.Metadata.Index
	r.RaftLog.truncatedTerm = m.Snapshot.Metadata.Term
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = max(r.RaftLog.stabled, m.Snapshot.Metadata.Index)
	r.Prs = make(map[uint64]*Progress)
	for _, peer_id := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[peer_id] = &Progress{}
	}
	msg := pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Reject: false, Index: m.Snapshot.Metadata.Index, LogTerm: m.Snapshot.Metadata.Term}
	r.msgs = append(r.msgs, msg)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	// TODO need to check if r is leader ?
	if id == None {
		log.Errorf("id = %v: can not add node(id = %v)", r.id, id)
		return
	}
	_, ok := r.Prs[id]
	if ok == true {
		log.Warnf("id = %v: the node(id = %v) is already in the raft group %v, it does not need to be added again", r.id, id, nodes(r))
		return
	}
	// set peer's Next = 0 to force the leader send snapshot to the new added node,
	// instead of sending normal appendEntries (peer's response msg may be dropped because it may be stale)
	r.Prs[id] = &Progress{Match: 0, Next: 0}
	log.Infof("id = %v: success to add node(id = %v) to raft group, current raft group: %v", r.id, id, nodes(r))
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	// TODO need to check if r is leader ?
	_, ok := r.Prs[id]
	if ok != true {
		log.Errorf("id = %v: failed to remove node(id = %v), because it's not in the raft group %v", r.id, id, nodes(r))
		return
	}
	if id == r.Lead && len(r.Prs) > 1 {
		log.Errorf("id = %v: failed to remove node(id = %v), because it is the leader and not the last peer in the raft group %v", r.id, id, nodes(r))
		return
	}
	delete(r.Prs, id)
	log.Infof("id = %v: success to remove node(id = %v) from the raft group, current raft group: %v", r.id, id, nodes(r))
	// the quorum may decrease, so we should check if the committed needs to be updated
	if r.State == StateLeader {
		r.UpdateCommitted()
	}
}
