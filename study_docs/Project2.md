# Project2 RaftKV
Raft是一种共识算法，本项目实现了基于raft的高可用的kv服务器，分为三部分
- 实现基本的Raft算法
- 基于Raft算法实现分布式的KV Server
- 实现Raft日志压缩，以及snapshot功能

## PartA - Raft算法
Description PartA
在raft/raft.go文件中的raft.Raft实现了Raft算法的核心，包括消息处理，驱动逻辑时钟等


### partAA Raft选举
Description PartAA
Leader选举的时机：follower或者candidate因为长时间没有收到leader发来的心跳消息，触发超时选举，这一部分的逻辑由Raft.tick()实现

实现leader election，要处理以下部分：
- raft.Raft.tick() ，用于推进逻辑时钟前进一个tick，从而驱动选举超时或者心跳超时
- raft.Raft.Step()，是消息处理入口，用于处理MsgRequestVote，MsgHeartbeat等信息，并根据消息处理结果，例如：当角色改变是，调用raft.Raft.becomeXXX更新raft状态


Raft.tick()推动Raft peer计时器前进一步，follower，candidate超时后会重新发起选举，leader超时会向所有的follower发送心跳消息
```go
// tick()推动raft内部计时器前进一步
// follower，candidate超时了要重新发起选举
// leader超时了，向其他的peer发送心跳消息
func (r *Raft) tick() {
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if (r.electionElapsed >= r.electionTimeout){
			r.electionElapsed = 0
			r.becomeCandidate()
			r.raiseVote()
		}
	case StateCandidate:
		r.electionElapsed++
		if (r.electionElapsed >= r.electionTimeout){
			r.electionElapsed = 0
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
```
超时后的选举有两步：
- 将自己的身份变更为candidate
- 向其他peer发送请求投票消息

becomeCandidate()负责将Raft peer的角色变更为candidate
```go
func (r *Raft) becomeCandidate() {
    // 将角色置为candidate
	r.State = StateCandidate
    // term增加
	r.Term++
    // 随机化选举超时时间,重置超时计时器
	r.electionTimeout = rand.Int()%10 + 10
	r.electionElapsed = 0
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.msgs = make([]pb.Message, 0)

	// 给自己投一票
	r.Vote = r.id
	r.votes[r.id] = true
}
```
becomeCandidate()完毕后，raiseVote()发起投票
```go
func (r *Raft) raiseVote(){
    // Raft group中只有一个peer，就不必选举，可以直接成为leader
	if len(r.Prs) == 1{
		r.becomeLeader()
		return
	}
    // 请求投票消息中要带上Raft log的lastIndex，logTerm
	msg := pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVote}
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(lastIndex)
	msg.Index = lastIndex
	msg.LogTerm = logTerm
    // 向其他peer发起投票消息
	r.bcastMessage(msg)
}
```

对端的peer收到消息后会调用Raft.Step()进行处理,首先会对消息进行一些检查，然后根据Raft角色调用不同的处理方法
```go
func (r *Raft) Step(m pb.Message) error {
    if _, ok := r.Prs[r.id]; !ok {
		// 当前raft不在Raft group里,允许响应来自leader和candidate的消息
		if m.MsgType != pb.MessageType_MsgHeartbeat && m.MsgType != pb.MessageType_MsgAppend &&  m.MsgType != pb.MessageType_MsgSnapshot && m.MsgType != pb.MessageType_MsgRequestVote {
			return nil
		}
	}
    // 非本地消息, 需要对Term进行检查
	if !isLocalMessage(m) {
        // 对端peer的Term比我方小，如果是请求投票消息，回复拒绝，其他情况直接忽略即可
		if r.Term > m.Term {
			if m.MsgType == pb.MessageType_MsgRequestVote {
				msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
				r.msgs = append(r.msgs, msg)
			}
			return nil
		} else if r.Term < m.Term {
            // 对方的Term大，需要将角色置为follower
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
```
如果leader和candidate收到了投票消息，会回复拒绝
```go
func (r *Raft) StepLeader(m pb.Message){
	switch m.MsgType {
	...
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
		r.msgs = append(r.msgs, msg)
    ...
	}
}

func (r *Raft) StepCandidate(m pb.Message){
	switch m.MsgType {
    ...
	case pb.MessageType_MsgRequestVote:
		msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true}
		r.msgs = append(r.msgs, msg)
    ...
    }
}
```

follower会调用handleVoteRequest()处理投票请求，如果对
```go
func (r *Raft) StepFollower(m pb.Message){
	switch m.MsgType {
    ...
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
    ...
    }
}

func (r *Raft)handleVoteRequest(m pb.Message){
	if r.State != StateFollower || r.Term != m.Term || m.MsgType != pb.MessageType_MsgRequestVote {
		return
	}
	msg := pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse}
	if (None == r.Vote){
        // 当前Term，还没有投过票
		localLastIndex := r.RaftLog.LastIndex()
		localLogTerm, _ := r.RaftLog.Term(localLastIndex)

		if (localLogTerm < m.LogTerm || (localLogTerm == m.LogTerm && localLastIndex <= m.Index)){
            // 对端的Raft log更新，投赞成票给对端
			msg.Reject = false
			r.Vote = m.From
		} else{
			msg.Reject = true
		}
	} else if (m.From == r.Vote){
        // 已经给对端投过赞成票，重发一次
		msg.Reject = false
	} else {
		msg.Reject = true
	}
	r.msgs = append(r.msgs, msg)
}
```
candidate收到对方的请求后, 会调用StepCandidate()处理消息，超过半数赞成即成为leader，超半数投拒绝，即成为follower
```go
func (r *Raft) StepCandidate(m pb.Message){
	switch m.MsgType {
    ...
	case pb.MessageType_MsgRequestVoteResponse:
        // 更新投票结果
		if _, ok := r.votes[m.From]; !ok {
			r.votes[m.From] = !m.Reject
		} else {
			return
		}
        // 统计投票
		acceptCount := 0
		for _, vote := range r.votes{
			if vote{
				acceptCount++
			}
		}
		quorum := r.Quorum()
		if acceptCount >= quorum {
            // 超过半数投赞成票，即成为leader
			r.becomeLeader()
		} else if len(r.votes) - acceptCount >= quorum {
            // 超过半数的人投了拒绝票，即成为follower
			r.becomeFollower(m.Term, None)
		}
    ...
    }
}
```
becomeLeader()将Raft身份置为leader, 并添加一条空日志项到日志末尾，然后向follower广播出去
```go
func (r *Raft) becomeLeader() {
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

	// append一条带上Term的空日志项到日志
	noopEntry := pb.Entry{Term: r.Term, Index: prevLogIndex+1, Data: nil}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)
    ...
	// 向其他peer发送append空日志的消息
	msg := pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgAppend, Index: prevLogIndex, LogTerm: prevLogTerm, Entries: []*pb.Entry{&noopEntry}, Commit: r.RaftLog.committed}
	r.bcastMessage(msg)
}
```

leader会定时向定时向follower发送心跳消息，follower通过handleHeartbeat()来处理心跳消息：
```go
func (r *Raft) handleHeartbeat(m pb.Message) {
    ...
    // canditate收到心跳消息后，会将角色改变为follower
	if r.State == StateCandidate {
		r.becomeFollower(m.Term, m.From)
	}
    // 重置超时器
	r.electionElapsed = 0
	msg := pb.Message{
		From: r.id,
		To: m.From,
		Term: r.Term,
		MsgType: pb.MessageType_MsgHeartbeatResponse,
	}
	r.msgs = append(r.msgs, msg)
}
```



### partAB Raft日志复制
Description PartAB
Leader收到Propose请求后会向本地添加日志，然后向follower复制这些日志
日志的复制，需要实现MsgAppend和MsgAppendResponse的处理逻辑

日志复制，用于leader向followe同步日志消息，用来保持多副本间的一致性，leader调用sendAppend()向其他peer同步日志，sendAppendSnapshot()用来发送快照，sendAppendNormal()用来发送普通的日志项，发送完毕后会更新peer.Next，记录下一次要向peer发送的Index：
```go
func (r *Raft) sendAppend(to uint64) bool {
    ...
	peer := r.Prs[to]

	if lastSendIndex, ok := r.sendAppendSnapshot(to); ok {
		peer.Next = lastSendIndex + 1
		return true
	}
	if lastSendIndex, ok := r.sendAppendNormal(to); ok {
		peer.Next = lastSendIndex + 1
		return true
	}
	return false
}
```

暂时只考虑发送日志项的情况, 如果peer.Next > TruncatedIndex, 就调用sendAppendNormal()；追加消息中 Index，LogTerm表示追加点的信息，Commit表示当前leader中的CommitIndex

```go
func (r *Raft) sendAppendNormal(to uint64) (uint64, bool) {
    ...
	// Next大于lastIndex，发送空日志项
	if peer.Next > lastIndex {
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
		...
	}

	sendStartOffset := peer.Next - r.RaftLog.entries[0].Index
	sendLen := lastIndex - peer.Next + 1
	sendEntries := []*pb.Entry{}
	for i := uint64(0); i < sendLen; i++ {
		sendEntries = append(sendEntries, &r.RaftLog.entries[sendStartOffset + i])
	}
    // 追加点的信息
	logIndex := peer.Next - 1
	logTerm, _ := r.RaftLog.Term(logIndex)

    // 发送[Next, lastIndex)之间的日志项
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
    // 将待发送的消息暂存到r.msgs
	r.msgs = append(r.msgs, msg)
    ...
}
```

对端peer收到append信息后，仍然走r.Step()处理流程，实际调用的是r.handleAppendEntries()
```go
func (r *Raft) handleAppendEntries(m pb.Message) {
	msg := pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Reject: true}
	lastIndex := r.RaftLog.LastIndex()

	// 追加点Index小于lastIndex，拒绝append，并在回复中附带上可能的追加点信息
	if m.Index > lastIndex {
		msg.Index = lastIndex
		msg.LogTerm = r.RaftLog.MustGetTerm(msg.Index)
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Index >= r.RaftLog.truncatedIndex {
        // 追加点Index > truncatedIndex，检查追加点的Term是否匹配
		logTerm := r.RaftLog.MustGetTerm(m.Index)
		if logTerm != m.LogTerm {
			if m.Index <= r.RaftLog.committed {
				log.Errorf("crash index should bigger than committed index, now crash index = %v, committed index = %v", m.Index, r.RaftLog.committed)
				return
			}
			msg.Index = m.Index - 1
			msg.LogTerm = r.RaftLog.MustGetTerm(msg.Index)
			r.msgs = append(r.msgs, msg)
			return
		}
	}
	i := 0
    // 追加点验证完成，然后再检查追加的日志项是否和本地有冲突
	for ; i < len(m.Entries); i++ {
        // 跳过那些不冲突的日志项
		curIndex := m.Entries[i].Index
		if curIndex < r.RaftLog.truncatedIndex {
			continue
		}
		if curIndex > lastIndex {
			break
		}
		// truncatedIndex <= curIndex <= lastIndex
		logTerm := r.RaftLog.MustGetTerm(curIndex)
		if m.Entries[i].Term != logTerm {
			if curIndex <= r.RaftLog.committed {
				log.Errorf("crash index should bigger than committed index, now crash index = %v, committed index = %v", curIndex, r.RaftLog.committed)
				return
			}
			// Drop the entries after conflict position (include curIndex)
            // 检查到了冲突，需要丢弃掉[curIndex,+∞)的日志项
			r.RaftLog.dropEntries(curIndex)
			break
		}
	}
	// 继续追加日志
	for ; i < len(m.Entries); i++ {
		r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
	}
	// 计算leader匹配的日志项中最大的Index
	maxMatchIndex := max(m.Index, r.RaftLog.committed)
	if len(m.Entries) > 0 {
		maxMatchIndex = max(m.Entries[len(m.Entries)-1].Index, r.RaftLog.committed)
	}
	maxMatchLogTerm := r.RaftLog.MustGetTerm(maxMatchIndex)
	// 向leader回复append确认消息，并带上当前最新的日志匹配信息
	msg.Index = maxMatchIndex
	msg.LogTerm = maxMatchLogTerm
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
	// 更新commit信息
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(maxMatchIndex, m.Commit)
	}
}
```
leader收到来自follower对append的回复消息后，通过Step()调用到handleAppendResponse()来处理：
```go
func (r *Raft) handleAppendResponse(m pb.Message) {
    ...
	// Peer拒绝了append, m.Index表示可能的追加点
	if (m.Reject == true){
		peer := r.Prs[m.From]
		possibleMatchIndex := m.Index
		// possibleMatchIndex == peer.Match is normal, eg: possibleMatchIndex == 0 and peer.Match == 0
		if possibleMatchIndex < peer.Match {
			log.Errorf("id = %v receives RejectAppend response from peer id = %v, m.Index(%v) < peer.Match(%v)", r.id, m.From, possibleMatchIndex, peer.Match)
		} else if possibleMatchIndex > r.RaftLog.LastIndex() {
			log.Errorf("id = %v receives RejectAppend response from peer id = %v, m.Index(%v) > lastIndex(%v)", r.id, m.From, possibleMatchIndex, r.RaftLog.LastIndex())
		} else {
			// 更新peer.Next为m.Index+1
			peer.Next = possibleMatchIndex+1
		}
		// 再次发送
		r.sendAppend(m.From)
		return
	}

	// peer追加日志成功，m.Index表示的当前peer和leader匹配的日志项最大Index
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1

	// 因为有append成功的回复，需要尝试更新commit
	commitedChanged := r.UpdateCommitted()
    ...
	// committedIndex发生了变化，需要向所有的peer同步这个情况
	if commitedChanged == true {
		for peer_id, _ := range r.Prs {
			if peer_id == r.id {
				continue
			}
			r.sendAppend(peer_id)
		}
	}
}
```
UpdateCommitted()尝试更新committedIndex
```go
func (r *Raft) UpdateCommitted() bool {
	...
	committedChanged := false
    // 从后往前搜索大于commitIndex的日志项
	for idx := r.RaftLog.LastIndex(); idx > r.RaftLog.committed; idx--{
		// 只能commit当前Term的日志项
		if logTerm, _ := r.RaftLog.Term(idx); logTerm!= r.Term{
			break
		}
		if r.IsAcceptedByQuorum(idx) {
            // 找到了最新的commitedIndex后，就停止搜索
			r.RaftLog.committed = idx
			committedChanged = true
            break
		}
	}
	return committedChanged
}
```

### partAC Raft节点封装
Description PartAC
在raft/rawnode.go中的raft.RawNode是与上层交互的接口，raft.RawNode包含raft.Raft，并提供wrap函数，比如RawNode.Tick()和RawNode.Step()，还提供RawNode.Propose()让上层向Raft append日志

Ready定义了一个重要的结构体，在处理消息或者推进逻辑时钟的时候，raft.Raft可能需要与上层进行交互，例如：
- 向其他raft节点方发送消息
- 将新append的日志持久化
- apply经过committed的日志
这些交互并不会发生，而是先缓存在Ready中，由RawNode.Ready()返回给上层，由上层来决定何时处理，处理完成后，还要调用RawNode.Advance()更新raft.Raft状态机

最终提供给上层调用的是封装后的RawNode，它对上层屏蔽了Raft角色等细节, 上层驱动RawNode的大致步骤为：
    msgs := receive_msgs() // 从对端peer收取消息
    for _, msg in msgs {
        // 调用Step()处理消息
        node.Step(msg)
    }
    // 检查Raft是否有要持久化的日志，待发送的消息等
    if !node.hasReady() {
        return
    }
    ready := node.Ready()
    // 进行持久化，更新commit，apply entry等
    node.handleReady(ready)

先看RawNode的构造过程：首先构造Raft，然后将Raft封装到RawNode里
```go
func NewRawNode(config *Config) (*RawNode, error) {
	if raft := newRaft(config); raft == nil {
		return nil, nil
	} else {
		return &RawNode{Raft:raft, hardState: pb.HardState{Term: raft.Term, Vote: raft.Vote, Commit: raft.RaftLog.committed}, softState: SoftState{Lead: raft.Lead, RaftState: raft.State}}, nil
	}
}
```
Raft的构造过程：
- 构造RaftLog：newLog()从storage中读取各种Index信息，以及term信息加载至内存，构建RaftLog
- 检查RaftLog信息是否正确
```go
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	r := Raft{id: c.ID, Term: 0, State: StateFollower, heartbeatTimeout: c.HeartbeatTick, electionTimeout: c.ElectionTick}
    // 构建RaftLog
	if r.RaftLog = newLog(c.Storage); r.RaftLog == nil {
		return nil
	}
	// 读取hardState：包括Term, Vote, Commit信息
	hardState, _, err2 := c.Storage.InitialState();

	r.RaftLog.applied = max(c.Applied, r.RaftLog.truncatedIndex)
	r.RaftLog.committed = hardState.Commit
	r.Term = hardState.Term
	r.Vote = hardState.Vote

    // index必须满足的大小关系：truncatedIndex <= appliedIndex <= committedIndex <= stabledIndex <= lastIndex
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
```


RawNode调用Step()处理消息后，可能需要对新append的日志进行持久化，向其他peer发送消息，调用Ready()可以保存这些变动到Ready中，等待上层处理
```go
func (rn *RawNode) Ready() Ready {
    //
	hardState := pb.HardState{Term: rn.Raft.Term, Vote: rn.Raft.Vote, Commit: rn.Raft.RaftLog.committed}
	if isHardStateEqual(hardState, rn.hardState) {
		hardState = pb.HardState{}
	}
	softState := &SoftState{rn.Raft.Lead, rn.Raft.State}
	if softState.Lead == rn.softState.Lead && softState.RaftState == rn.softState.RaftState {
		softState = nil
	}

	raftLog := rn.Raft.RaftLog
    // 待持久化的日志（stabled~lastIndex）
	unstableEntries := raftLog.unstableEntries()
    // 新提交的日志entry
	committedEntries := raftLog.nextEnts()
	ready := Ready{
		SoftState: softState,
		HardState: hardState,
		Entries: unstableEntries,
		CommittedEntries: committedEntries,
		Messages: rn.Raft.msgs,
	}
    // 有待处理pending的snapshot
	if raftLog.pendingSnapshot != nil {
		ready.Snapshot = *raftLog.pendingSnapshot
	}
	return ready
}
```
在处理完了Ready后，再调用RawNode.Advance()来进一步推进Raft状态机（对Ready的处理流程会在partB介绍）
```go
func (rn *RawNode) Advance(rd Ready) {
	rn.Raft.RaftLog.pendingSnapshot = nil
    // 更新内存中的hardState
	if !IsEmptyHardState(rd.HardState) {
		rn.hardState = rd.HardState
	}
	if rd.SoftState != nil {
		rn.softState = *rd.SoftState
	}
    // 更新applied，stabled Index
	rn.Raft.RaftLog.applied = rn.Raft.RaftLog.applied + uint64(len(rd.CommittedEntries))
	rn.Raft.RaftLog.stabled = rn.Raft.RaftLog.stabled + uint64(len(rd.Entries))
	rn.Raft.msgs = make([]pb.Message, 0)
}
```

## PartB 基于Raft实现分布式存储
Description PartB
通过PartA中实现的Raft模块，构建一个分布式KV Server

PartA中已经实现了Raft模块，并把所有的功能封装到了RawNode，基于RawNode构建一个分布式KV存储，还要实现以下API：
- proposeRaftCommand
- HandleRaftReady
- Append
- SaveReadyState

proposeRaftCommand()用来向Raft group提出一个propose请求
```go
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
    // 先检查store_id等信息是否和本Raft信息匹配
	err := d.preProposeRaftCommand(msg);
    // msg必须要携带Request
	if msg.AdminRequest == nil && len(msg.Requests) == 0 {
		err = errors.Errorf("the RaftCmd has no request")
		cb.Done(ErrResp(err))
		return
	}
    // 只能携带一种Request，要么是Admin request（比如compact log），要么是normal request（比如GET）
	if msg.AdminRequest != nil && len(msg.Requests) > 0 {
		err = errors.Errorf("the RaftCmd can't enclose normal requests and administrator request at same time")
		cb.Done(ErrResp(err))
		return
	}
	// 将msg序列化
	data, err2 := msg.Marshal();
	prop := proposal{d.nextProposalIndex(), d.Term(), cb}
    // 调用RawNode.Propose()发起propose
	err2 = d.RaftGroup.Propose(data)
    // 保存proposal，apply该日志项的时候会用到
	d.proposals = append(d.proposals, &prop)
}
```

RawNode.Propose()会调用Raft.Step(), 最终调用到Raft.handleAppendPropose()
```go
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

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

	// 在日志后面追加一个新的日志项
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
    ...

	// Raft group只有一个peer，就直接commit
	if len(r.Prs) == 1{
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}

	// 新增日志，需要向其他peer发起复制日志的请求
	for peer_id, _ := range r.Prs {
		if (peer_id == r.id){
			continue
		}
		r.sendAppend(peer_id)
	}
}
```

HandleRaftReady()用来处理Raft状态机中待处理的内容，比如持久化日志，向peer发送msg等
```go
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	//Raft状态机有没有待处理的内容，直接返回
	if !d.RaftGroup.HasReady() {
		return
	}
    // 获取待处理的内容
	ready := d.RaftGroup.Ready()
    // 持久化存储日志
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready);
    ...
	// 向peer发送消息
	if needRecheckMessage {
		var messages []eraftpb.Message
		for _, msg := range ready.Messages {
			if _, ok := d.RaftGroup.Raft.Prs[msg.GetTo()]; ok {
				messages = append(messages, msg)
			}
		}
		d.Send(d.ctx.trans, messages)
	} else {
		d.Send(d.ctx.trans, ready.Messages)
	}

	// apply已经committed的日志
	d.applyEntries(ready.CommittedEntries)
	if d.stopped {
		return
	}
	// 推进Raft状态机
	d.RaftGroup.Advance(ready)
}
```
其中SaveReadyState()用来持久化存储日志等内容
```go
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
    // snapshot相关的处理逻辑，这里暂时用不到
	var applySnapResult *ApplySnapResult
	if ready.Snapshot.Metadata != nil {
		kvWB := new(engine_util.WriteBatch)
		raftWB := new(engine_util.WriteBatch)
		result, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, err
		}
		applySnapResult = result
	}
	// 持久化存储新append的日志, 并更新lastIndex, lastTerm
	raftWB := new(engine_util.WriteBatch)
	if err := ps.Append(ready.Entries, raftWB); err != nil {
		return nil, err
	}
    // 如果commit，Term，Vote有变动，也要持久化存储
	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}
	raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), ps.raftState)
	if err := ps.Engines.WriteRaft(raftWB); err != nil {
		return nil, err
	}
	return applySnapResult, nil
}
```
持久化存储后，调用peerMsgHandler.Send()向给定的peer发送消息，然后调用peerMsgHandler.applyEntries()apply已经committed的日志, 目前的Apply流程和commit流程是同步的
```go
func (d *peerMsgHandler) applyEntries(entries []eraftpb.Entry) {
	if d.stopped {
		return
	}
	if len(entries) > 0 {
		aCtx := d.CreateApplyContext(entries)
        // apply的主要流程
		aCtx.applyEntries()
		if aCtx.stopped {
			d.stopped = aCtx.stopped
			return
		}
        ...
		// 更新applyState
		d.peerStorage.applyState = aCtx.applyState
	}
}

func (aCtx *ApplyContext) applyEntries() {
	length := len(aCtx.committedEntries)
	if length == 0 {
		return
	}
	for i := 0; i < length; i++ {
        // 对每个entry，调用aCtx.applyEntry()
		aCtx.responses = append(aCtx.responses, aCtx.applyEntry(i))
		if aCtx.applyToDBInAdvance {
			aCtx.applyToDB()
		}
		if aCtx.stopped {
			return
		}
	}
    // 将kvWB刷新到持久化存储
	aCtx.applyToDB()
	aCtx.doneCallbacks()
}

// apply单个entry，保存修改到kvWB中
func (aCtx *ApplyContext) applyEntry(offset int) *raft_cmdpb.RaftCmdResponse {
	cb := aCtx.callbacks[offset]
	entry := &aCtx.committedEntries[offset]
	raftCmdRequest := &raft_cmdpb.RaftCmdRequest{}
	...
	if entry.GetData() != nil {
        // 反序列化entry
		if err = raftCmdRequest.Unmarshal(entry.GetData()); err != nil {
			log.Fatalf("failed to unmarshal request from entry data, detail :%v", err)
		} else 	if err = aCtx.d.preApplyEntry(raftCmdRequest); err != nil {
            // 检查region是否匹配
			resp = ErrResp(err)
		} else 	if raftCmdRequest.AdminRequest != nil {
            // entry类型是admin，比如compact log
			resp = aCtx.handleAdminEntry(cb, raftCmdRequest)
		} else {
            // 普通的entry，比如PUT
			resp = aCtx.handleNormalEntry(cb, raftCmdRequest)
		}
	}
	aCtx.applyState.AppliedIndex = expectedIndex
	return resp
}
```
handleAdminEntry()和handleNormalEntry()分别apply admin类型的entry，普通的entry，这里暂时只关注对普通entry的apply流程
```go
func (aCtx *ApplyContext) handleNormalEntry(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	cmdResp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	var resp *raft_cmdpb.Response
	var err error
    ...
	aCtx.kvWB.SetSafePoint()
    // 根据req的类型一次低啊用响应的处理函数，比如PUT就调用handlerPut()
	for _, req := range raftCmdRequest.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Invalid:
			err = errors.Errorf("here comes a request, type: CmdType_Invalid")
		case raft_cmdpb.CmdType_Get:
			resp, err = aCtx.handleGet(cb, req.GetGet())
		case raft_cmdpb.CmdType_Put:
			resp, err = aCtx.handlePut(cb, req.GetPut())
		case raft_cmdpb.CmdType_Delete:
			resp, err = aCtx.handleDelete(cb, req.GetDelete())
		case raft_cmdpb.CmdType_Snap:
			// the modifies may be written to DB
			resp, err = aCtx.handleSnap(cb, req.GetSnap())
		default:
			err = errors.Errorf("here comes a request, unknown cmd_type: %v", req.CmdType)
		}
		if err != nil {
			aCtx.kvWB.RollbackToSafePoint()
			return ErrResp(err)
		}
	}
	cmdResp.Responses = append(cmdResp.Responses, resp)
	return cmdResp
}
```


## PartC 日志压缩
### compact log
之前所实现的Raft KV存储，会保存完整的Raft日志；而真实的KV服务不可能保存所有的日志，因此需要对日志进行压缩
TinyKV中，Raft通过onTick()定期检查是否需要压缩日志, 需要的话就会向Raft发起一个compact log的propose
```go
func (d *peerMsgHandler) onTick() {
	...
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	...
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}
    ...
    // 从FirstIndex到appliedIndex，之间的的日志条目超过了阈值，就需要压缩日志了
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}
    ...
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
    // 向Raft group提出compact log的request，等待Raft Group的commit，apply
	d.proposeRaftCommand(request, nil)
}
```

等到compact log请求被commit之后，再通过apply该日志（apply流程和KV中实现PUT请求相同）项来实现压缩日志的功能，handleAdminEntry()负责apply所有的admin类型的请求
```go
func (aCtx *ApplyContext) handleAdminEntry(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	adminReq := raftCmdRequest.AdminRequest
	var resp *raft_cmdpb.RaftCmdResponse
	switch adminReq.CmdType {
    ...
	case raft_cmdpb.AdminCmdType_CompactLog:
		resp = aCtx.handleCompactLog(cb, raftCmdRequest)
    ...
	}
	return resp
}
```
handleCompactLog()处理逻辑非常简单，就是对aCtx.applyState进行更新
```go
func (aCtx *ApplyContext) handleCompactLog(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
    ...
    // 设置好新的TruncateState
    aCtx.applyState.TruncatedState.Index = compactLog.CompactIndex
    aCtx.applyState.TruncatedState.Term = compactLog.CompactTerm
    ...
}
```
日志项被批处理完毕后，applyEntries()发现TrucatedState发生了改变，开始执行真正的compact log操作：
- 压缩在内存中的日志
- 压缩在磁盘上的日志
```go
func (d *peerMsgHandler) applyEntries(entries []eraftpb.Entry) {
    ...
    // compact log
    if aCtx.applyState.TruncatedState.GetIndex() != d.peerStorage.truncatedIndex() {
        compactIndex := aCtx.applyState.TruncatedState.GetIndex()
        compactTerm := aCtx.applyState.TruncatedState.GetTerm()
        // 清除掉保存在内存中compactIndex之前的所有日志
        d.RaftGroup.Raft.RaftLog.Compact(compactIndex, compactTerm)
        // 清除掉磁盘上compactIndex之前的所有日志
        d.ScheduleCompactLog(compactIndex)
    }
    ...
}

// 在内存中删掉truncatedIndex之前的所有日志
func (l *RaftLog) Compact(compactIndex, compactTerm uint64) {
	...
	l.truncatedIndex = compactIndex
	l.truncatedTerm = compactTerm
	offset, err := l.Offset(compactIndex);
	l.entries = l.entries[offset+1:]
}

// 生成一个GCTask，异步清理磁盘上的日志
func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}
```

### snapshot
#### 发送snapshot
压缩日志后，leader保存的不再是一份完整的日志，因此在向follower同步日志时，可能出现待同步的日志已经被删除了，这个时候就要向follower直接发送快照，发送的逻辑在sendAppend()里面
```go
func (r *Raft) sendAppend(to uint64) bool {
	...
	if lastSendIndex, ok := r.sendAppendSnapshot(to); ok {
		peer.Next = lastSendIndex + 1
		return true
	}
	...
}

func (r *Raft) sendAppendSnapshot(to uint64) (uint64, bool) {
	...
    // 只有待发送的日志项已经被truncate了才需要发送snapshot
	if peer.Next > r.RaftLog.truncatedIndex {
		return lastSendIndex, false
	}
	...
	if r.RaftLog.localSnapshot != nil {
        // 检查当前已有的snapshot是否可以直接发送
		cacheSnapshot := r.RaftLog.localSnapshot
        ...
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
			lastSendIndex = msg.Snapshot.Metadata.GetIndex()
			return lastSendIndex, true
		}
	}
	// 生成新的snapshot，然后再发送
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err == ErrSnapshotTemporarilyUnavailable {
		return lastSendIndex, false
	}
	if err != nil {
		log.Error(err)
		return lastSendIndex, false
	}
	r.RaftLog.localSnapshot = &snapshot
	msg.Snapshot = r.RaftLog.localSnapshot
	r.msgs = append(r.msgs, msg)
	lastSendIndex = snapshot.Metadata.GetIndex()
	return lastSendIndex, true
}
```


#### 收到snapshot后的处理过程
follower收到快照消息后，通过stepFollower()调用handleSnapshot()处理快照
```go
func (r *Raft) handleSnapshot(m pb.Message) {
	...
    // 首先truncatesnapshot之前的所有日志
	if m.Snapshot.Metadata.Index <= r.RaftLog.LastIndex(){
		snapOffset, _ := r.RaftLog.Offset(m.Snapshot.Metadata.Index)
		r.RaftLog.entries = r.RaftLog.entries[snapOffset+1:]
	} else {
		r.RaftLog.entries = []pb.Entry{}
	}
    // 标记pendingSnapshot，等待Ready()处理
	r.RaftLog.pendingSnapshot = m.Snapshot
    // 更新RaftLog相关的元数据
	r.RaftLog.truncatedIndex = m.Snapshot.Metadata.Index
	r.RaftLog.truncatedTerm = m.Snapshot.Metadata.Term
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = max(r.RaftLog.stabled, m.Snapshot.Metadata.Index)
	r.Prs = make(map[uint64]*Progress)
    // 更新Raft peers
	for _, peer_id := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[peer_id] = &Progress{}
	}
	...
}

func (rn *RawNode) Ready() Ready {
    ...
	if raftLog.pendingSnapshot != nil {
		ready.Snapshot = *raftLog.pendingSnapshot
	}
	return ready
}
```

follower通过SaveReadyState()处理Ready消息，最后调用ApplySnapshot()来处理快照消息
```go
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	var applySnapResult *ApplySnapResult
	if ready.Snapshot.Metadata != nil {
		kvWB := new(engine_util.WriteBatch)
		raftWB := new(engine_util.WriteBatch)
		result, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB);
		applySnapResult = result
	}
    ...
}

func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	...
	snapshotIndex := snapshot.Metadata.Index
	snapshotTerm := snapshot.Metadata.Term
    // 清除掉raftWB上snapshotIndex之前的日志，以及raftDB，kvDB上region相关的元数据
	ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, snapshotIndex)
	// set applyState
	ps.applyState.AppliedIndex = snapshotIndex
	ps.applyState.TruncatedState.Index = snapshotIndex
	ps.applyState.TruncatedState.Term = snapshotTerm
	kvWB.SetMeta(meta.ApplyStateKey(ps.region.GetId()), ps.applyState)
	// set regionState
	prevRegion := ps.region
	ps.region = snapData.Region
	newRegionState := rspb.RegionLocalState{Region: ps.region}
	kvWB.SetMeta(meta.RegionStateKey(ps.region.GetId()), &newRegionState)
	// set raftState
	ps.raftState.LastIndex = snapshotIndex
	ps.raftState.LastTerm = snapshotTerm
	ps.raftState.HardState.Commit =  snapshotIndex
	raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), ps.raftState)
	// 先写kvDB，然后才去apply snapshot，最后再写raftDB，防止中间出现重启导致数据不一致
	if err := ps.Engines.WriteKV(kvWB); err != nil {
		log.Fatal(err)
		return nil, err
	}
	notifier := make(chan bool, 1)
	applyTask := runner.RegionTaskApply{
		RegionId: snapData.Region.Id,
		Notifier: notifier,
		SnapMeta: &eraftpb.SnapshotMetadata{ConfState: snapshot.Metadata.ConfState, Index: snapshotIndex, Term: snapshotTerm},
		StartKey: snapData.Region.StartKey,
		EndKey: snapData.Region.EndKey,
	}
    // 构造snapshot task，发向snapshot worker，等待apply snapshot完毕
	ps.regionSched <- &applyTask
	if ok :=  <-notifier; ok != true {
		log.Fatalf("failed to apply snapshot")
	}
	// 写入raftDB
	if err := ps.Engines.WriteRaft(raftWB); err != nil {
		log.Fatal(err)
		return nil, err
	}
    // 已经初试化了的peer storage, 需要清理掉属于之前region但不在当前region里的数据
	if ps.isInitialized() {
		ps.clearExtraData(snapData.Region)
	}

	return &ApplySnapResult{prevRegion, ps.Region()}, nil
}
```