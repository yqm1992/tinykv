# Project3 MultiRaftKV
Project2实现的KV Server只有一个Raft group，不能扩展，所有的读写请求都由这个Raft group负责，导致写请求无法并发执行。

Project-3会实现一个带有scheduler的多Raft server实现多Raft：整个KV Server由多个Raft group组成，每个Raft group单独负责一个键范围（region），不同region的key的读写请求由各自的Raft group负责，由scheduler负责对region进行调度，该项目分为三部分：
- 实现Raft leader变更
- Raft实现conf变更以及region split
- 实现region balance，引入scheduler

## partA
partA在Raft层面实现leader transfer，以及conf change

### 实现leader transfer
RawNode.TansferLeader()很简单，就是调用Raft模块去处理TransferLeader消息
```go
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
```

transferLeader()会检查peer的日志，如果日志是最新的，就向peer发送MsgTimeoutNow消息，否则就要等peer日志追赶上来
```go
func (r *Raft) transferLeader(to uint64) {
    ...
	r.leadTransferee = to
	if r.Prs[r.leadTransferee].Match == r.RaftLog.LastIndex() {
        // leadTransferee的日志已经是最新的，直接向leadTransferee发送消息
		msg := pb.Message{To: r.leadTransferee, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgTimeoutNow}
		r.msgs = append(r.msgs, msg)
		log.Infof("id = %v(leader) prepares to transfer leader to id = %v, sends MsgTimeoutNow message", r.id, r.leadTransferee)
	} else {
        // leadTransferee的日志不是最新的，需要等待leadTransferee的日志追赶上来
		r.sendAppend(r.leadTransferee)
		log.Infof("id = %v(leader) prepares to transfer leader to id = %v, sends append message", r.id, r.leadTransferee)
	}
}
```

follower的日志追赶上来后，继续调用transferLeader()
```go
func (r *Raft) handleAppendResponse(m pb.Message) {
    ...
	if m.From == r.leadTransferee {
		r.transferLeader(m.From)
		return
	}
    ...
}
```

peer的日志赶上了leader后，会收到MsgTimeoutNow消息，然后立即发起选举，等待成为新的leader
```go
func (r *Raft) StepFollower(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.raiseVote()
	case pb.MessageType_MsgTimeoutNow:
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
    ...
	}
}
```

### 实现conf change（Raft group中添加或删除Node）

RawNode.ApplyConfChange()用来向Raft group中添加Node，以及从Raft group中删除Node
```go
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}
```

向Raft group中添加Node
```go
func (r *Raft) addNode(id uint64) {
    ...
	_, ok := r.Prs[id]
	if ok == true {
        // 已经在Raft group，直接返回
		return
	}
	r.Prs[id] = &Progress{Match: 0, Next: 0}
}
```

从Raft group中删除Node
```go
func (r *Raft) removeNode(id uint64) {
	_, ok := r.Prs[id]
	if ok != true {
        // Node不在Raft group里
		return
	}
	...
	delete(r.Prs, id)
	// the quorum may decrease, so we should check if the committed needs to be updated
	if r.State == StateLeader {
        // 由于Raft group中成员减少，可能需要更新committed Index
		r.UpdateCommitted()
	}
}
```


## partB
PartA中，Raft支持leader变更以及组员变更，在此基础上partB让TinyKV支持admin command，分为两部分：
- 让TinyKV支持TransferLeader
- 让TinyKV支持Conf Change

### 让TinyKV支持TransferLeader Command

TinyKV收到了TransferLeader命令后，直接调用RawNode.TansferLeader()，不需要向其他peer复制该操作（该消息只针对leader，因此不需要commit）
```go
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
    ...
    if msg.AdminRequest != nil && msg.AdminRequest.TransferLeader != nil {
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.GetId())
		resp := &raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_TransferLeader},
		}
		cb.Done(resp)
		return
	}
    ...
}
```

### 让TinyKV支持Conf Change

ChangePeer需要经过Raft group的commit-apply路径：等待ChangePeer的entry被commit后，KV server再调用handleAdminEntry()完成apply
```go
func (aCtx *ApplyContext) handleAdminEntry(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
    ...
	switch adminReq.CmdType {
    ...
	case raft_cmdpb.AdminCmdType_ChangePeer:
		resp = aCtx.handleChangePeer(cb, raftCmdRequest)
	}
	return resp
}
```

handleChangePeer()完成实际的change peer
```go
func (aCtx *ApplyContext) handleChangePeer(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	adminReq := raftCmdRequest.AdminRequest
	resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
	d := aCtx.d
	raftPeerNum := len(d.RaftGroup.Raft.Prs)
	regionPeerNum := len(d.Region().Peers)
	if raftPeerNum != regionPeerNum {
		log.Fatalf("id = %v: num(raft.peers) = %v, num(region.peers) = %v, mismatch", d.PeerId(), raftPeerNum, regionPeerNum)
	}
	cc := eraftpb.ConfChange{ChangeType: adminReq.ChangePeer.ChangeType, NodeId: adminReq.ChangePeer.Peer.Id}
    // 调用RawNode.ApplyConfChange()
	confState := d.RaftGroup.ApplyConfChange(cc)
	resp.AdminResponse = &raft_cmdpb.AdminResponse{CmdType: raft_cmdpb.AdminCmdType_ChangePeer}
    ...
	removeSelf := false
	if _, ok := d.RaftGroup.Raft.Prs[adminReq.ChangePeer.Peer.Id]; ok == true {
		// 组员变更类型：Add Node
		curRegion.Peers = append(curRegion.Peers, adminReq.ChangePeer.Peer)
	} else {
		// 组员变更类型：Remove Node
        // 从region，以及peerCache中删除该peer
		curRegion.Peers = append(curRegion.Peers[:targetIndex], curRegion.Peers[targetIndex+1:]... )
		delete(d.peerCache, adminReq.ChangePeer.Peer.Id)
		if adminReq.ChangePeer.Peer.Id == d.PeerId() {
			removeSelf = true
		}
	}
	curRegion.RegionEpoch.ConfVer++
	if removeSelf {
        // 删除本节点，停止运行Raft
		d.destroyPeer()
		aCtx.stopped = d.stopped
	} else {
		// 更新region信息
		aCtx.updateRegion(curRegion)
	}
	return resp
}
```

ApplyContext.updateRegion()：向kvDB写入regionState，并在storeMeta中更新region
```go
func (aCtx *ApplyContext) updateRegion(curRegion *metapb.Region) {
	meta.WriteRegionState(aCtx.kvWB, curRegion, rspb.PeerState_Normal)
	storeMeta := aCtx.d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()
	storeMeta.setRegion(curRegion, aCtx.d.peer)
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: curRegion})
}
```

### 实现region split
TinyKV支持多Raft group，会对数据进行了分片，每个分片称作一个region，每个Raft group负责一个region, region定义如下：

```go
type Region struct {
	Id uint64 `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	// Region key range [start_key, end_key).
	StartKey             []byte       `protobuf:"bytes,2,opt,name=start_key,json=startKey,proto3" json:"start_key,omitempty"`
	EndKey               []byte       `protobuf:"bytes,3,opt,name=end_key,json=endKey,proto3" json:"end_key,omitempty"`
	RegionEpoch          *RegionEpoch `protobuf:"bytes,4,opt,name=region_epoch,json=regionEpoch" json:"region_epoch,omitempty"`
	Peers                []*Peer      `protobuf:"bytes,5,rep,name=peers" json:"peers,omitempty"`
}
```

每个region负责key的范围是[start_key, end_key)，最开始只有一个region范围是[“”, “”)代表整个空间；region数据量增加，达到阈值后就需要对该region执行split，split checker会检查region的大小，如果数据量达到了阈值，就将region分成两部分（以split key为分界点）


Raft会周期性的去检查负责的region是否需要split，触发逻辑在onSplitRegionCheckTick()：发送split task到split checker worker
```go
func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	...
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}
```

split checker worker收到task后，对指定的region进行检查，region中数据量达到了阈值后，向Raft发送一条MsgTypeSplitRegion的消息
```go
func (r *splitCheckHandler) Handle(t worker.Task) {
    spCheckTask, ok := t.(*SplitCheckTask)
    ...
    region := spCheckTask.Region
	regionId := region.Id
    ...
    // key != nil 需要split，以key为分界点
	key := r.splitCheck(regionId, region.StartKey, region.EndKey)
	if key != nil {
		_, userKey, err := codec.DecodeBytes(key)
		if err == nil {
			// It's not a raw key.
			// To make sure the keys of same user key locate in one Region, decode and then encode to truncate the timestamp
			key = codec.EncodeBytes(userKey)
		}
		msg := message.Msg{
			Type:     message.MsgTypeSplitRegion,
			RegionID: regionId,
			Data: &message.MsgSplitRegion{
				RegionEpoch: region.GetRegionEpoch(),
				SplitKey:    key,
			},
		}
        // 发送消息到region
		err = r.router.Send(regionId, msg);
        ...
	}
    ...
}
```

splitCheck()负责真正的检查逻辑
```go
func (r *splitCheckHandler) splitCheck(regionID uint64, startKey, endKey []byte) []byte {
	...
	it := engine_util.NewCFIterator(engine_util.CfDefault, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if engine_util.ExceedEndKey(key, endKey) {
			// 发送消息给Raft，更新region的大小
			r.router.Send(regionID, message.Msg{
				Type: message.MsgTypeRegionApproximateSize,
				Data: r.checker.currentSize,
			})
			break
		}
		if r.checker.onKv(key, item) {
            // region数据量已经超过阈值，需要split（分界点就是key）
			break
		}
	}
	return r.checker.getSplitKey()
}
```

Raft收到MsgTypeSplitRegion消息后，调用onPrepareSplitRegion()，发送一个SchedulerAskSplitTask到scheduler_task worker处理
```go
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
    ...
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
    ...
	}
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}
```

scheduler_task worker调用onAskSplit()来处理task：先向scheduler发起AskSplit RPC，获得新的regionID以及peerIDs，然后向Raft发送AdminRequest, 等待Raft group的commit，apply
```go
func (r *SchedulerTaskHandler) Handle(t worker.Task) {
	switch t.(type) {
	case *SchedulerAskSplitTask:
		r.onAskSplit(t.(*SchedulerAskSplitTask))
    ...
	}
}

func (r *SchedulerTaskHandler) onAskSplit(t *SchedulerAskSplitTask) {
    // 向scheduler发起AskSplit RPC，获取regionID以及peerIDs
	resp, err := r.SchedulerClient.AskSplit(context.TODO(), t.Region)
	if err != nil {
		log.Error(err)
		return
	}

	aq := &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitRequest{
			SplitKey:    t.SplitKey,
			NewRegionId: resp.NewRegionId,
			NewPeerIds:  resp.NewPeerIds,
		},
	}
    // Raft发送AdminRequest, 等待Raft group的commit，apply
	r.sendAdminRequest(t.Region.GetId(), t.Region.GetRegionEpoch(), t.Peer, aq, t.Callback)
}
```

待split request被Raft group commit后，执行apply步骤，调用handleSplit()生成新的peer负责new region
```go
func (aCtx *ApplyContext) handleAdminEntry(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
    ...
	switch adminReq.CmdType {
    ...
	case raft_cmdpb.AdminCmdType_Split:
		resp = aCtx.handleSplit(cb, raftCmdRequest)
    ...
	}
}

func (aCtx *ApplyContext) handleSplit(cb *message.Callback, raftCmdRequest *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
    ...
	curRegion := d.Region()
	newRegion := &metapb.Region{
		Id: splitReq.GetNewRegionId(),
		StartKey: splitReq.SplitKey,
		EndKey: d.Region().GetEndKey(),
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
	}
    // 向new region中加入peers
	for idx, peerInfo := range curRegion.Peers {
		newRegion.Peers = append(newRegion.Peers, &metapb.Peer{Id: splitReq.NewPeerIds[idx], StoreId: peerInfo.StoreId})
	}
    // region发生了变化，版本号+1
	curRegion.RegionEpoch.Version++
	curRegion.EndKey = splitReq.SplitKey
    // 在当前的store上，创建新的peer
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Fatal(err)
	}
	d.RaftGroup.Raft.RaftLog.ResetCacheSnapshot()
    // 同时更新region的信息
	aCtx.updateRegion(curRegion)
	newPeer.updateRegion(d.ctx, newRegion)
    // 在本地注册new peer的信息
	d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(newRegion.GetId(), message.Msg{Type: message.MsgTypeStart})
	return resp
}
```

## partC
TinyKV的数据分散存储在多个region上，经过一段时间的运行后，各个store上负责的region可能不平衡，region过多地集中在某些store上，因此需要一个中心scheduler对集群进行调度，保证每个store负责的region尽量平衡

要实现调度，需要Scheduler掌握整个集群的相关信息：比如region分散在哪些store上，region当前的数据量等。因此TinyKV中每个region要定时向Scheduler发送心跳消息，Scheduler收到region的心跳消息后会更新相应region的信息，同时Scheduler会定期检查region是否平衡，不平衡就会执行均衡策略

TinyKV实现region balance分为两步：
- Scheduler收集region消息
- 实现region balance

### Scheduler收集region信息
Scheduler收集region信息有两部分：
- region的leader周期性发送心跳消息到Scheduler
- Scheduler更新region的信息

#### region发送心跳到Scheduler
```go
func (d *peerMsgHandler) onTick() {
	...
    // 周期性向Scheduler发送Heartbeat
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
    ...
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

// 构造一个SchedulerRegionHeartbeatTask，送往SchedulerTask worker处理
func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(p.Region(), clonedRegion)
	if err != nil {
		return
	}
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}
```

SchedulerTask worker收到task后调用Handle()进行处理，向Scheduler发起RegionHeartbeat RPC
```go
func (r *SchedulerTaskHandler) Handle(t worker.Task) {
	switch t.(type) {
    ...
	case *SchedulerRegionHeartbeatTask:
		r.onHeartbeat(t.(*SchedulerRegionHeartbeatTask))
    ...
	}
}

func (r *SchedulerTaskHandler) onHeartbeat(t *SchedulerRegionHeartbeatTask) {
	var size int64
	if t.ApproximateSize != nil {
		size = int64(*t.ApproximateSize)
	}

	req := &schedulerpb.RegionHeartbeatRequest{
		Region:          t.Region,
		Leader:          t.Peer,
		PendingPeers:    t.PendingPeers,
		ApproximateSize: uint64(size),
	}
	r.SchedulerClient.RegionHeartbeat(req)
}
```


#### Scheduler更新region的信息
收到RegionHeartbeat RPC请求后，Scheduler调用RegionHeartbeat()处理，负责实际处理的是processRegionHeartbeat()
```go
// RegionHeartbeat implements gRPC PDServer.
func (s *Server) RegionHeartbeat(stream schedulerpb.Scheduler_RegionHeartbeatServer) error {
    ...
	for {
		request, err := server.Recv()
        ...
		err = cluster.HandleRegionHeartbeat(region)
        ...
	}
}

func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
	if err := c.processRegionHeartbeat(region); err != nil {
		return err
	}
    ...
}
```

processRegionHeartbeat()的处理逻辑：
```go
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
	...
    localRegion := c.core.GetRegion(region.GetID())
    ...
    // 先检查Scheduler上是否有该region的信息
	if localRegion != nil {
		localEpoch := localRegion.GetRegionEpoch()
		if heartbeatEpoch.Version < localEpoch.Version || heartbeatEpoch.ConfVer < localEpoch.ConfVer {
            // region心跳消息过期
			return err
		}
	} else {
        // 新的region，如果和已知的region负责的key有重叠部分，Epoch一定要是最新的
		overlapRegions = c.core.GetOverlaps(region)
		for _, curRegion := range overlapRegions {
			localEpoch := curRegion.GetRegionEpoch()
			if heartbeatEpoch.Version < localEpoch.Version || heartbeatEpoch.ConfVer < localEpoch.ConfVer {
                // region心跳消息过去
				return err
			}
		}
	}

	c.core.PutRegion(region)
	for _, peer := range region.GetPeers() {
		// 更新region涉及到的store相关的统计信息
		c.updateStoreStatusLocked(peer.GetStoreId())
	}
	// 对重叠的region，
	for _, curRegion := range overlapRegions {
		if curRegion.GetLeader() != nil {
			c.prepareChecker.remove(region)
		}
	}
	if region.GetLeader() != nil && (localRegion == nil || localRegion.GetLeader() == nil) {
		c.prepareChecker.collect(region)
	}
	return nil
}
```

### 实现region balance
region balance分为两部分：
- Scheduler根据region分布情况生成调度Operator(包含针对region的操作步骤)
- 根据Operator执行具体的调度
    - Scheduler根据Operator向region发送调度命令
    - KV Server执行调度命令

#### Scheduler产生调度Operator
Scheduler周期性地检查是否需要执行region balance，检查逻辑在coordinator.runScheduler()
```go
func (c *coordinator) runScheduler(s *scheduleController) {
	...
	for {
		select {
		case <-timer.C:
			...
			if op := s.Schedule(); op != nil {
				c.opController.AddOperator(op)
			}
            ...
		}
	}
}
```

scheduleController.Schedule()调用balanceRegionScheduler.Schedule()
```go
func (s *scheduleController) Schedule() *operator.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// 实际调用的是balanceRegionScheduler.Schedule()
		if op := s.Scheduler.Schedule(s.cluster); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}
```

balanceRegionScheduler.Schedule()检查store上的region分布情况，如果需要执行balance就返回相应的Move operator，大致流程为：
- 确定source store
- 在source store上找最合适的region
- 找最合适的target store

```go
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
    // 获取所有store的信息
	stores := cluster.GetStores()
	sources := filter.SelectTargetStores(stores, s.filters, cluster)

	if len(sources) < 2 {
		return nil
	}
	// 按照RegionSize降序排列stores
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	for index, source := range sources {
		if index == len(sources) - 1 {
			break
		}
		for  j := 0; j < balanceRegionRetryLimit; j++ {
            // 尝试从当前的source迁移region
			if op := s.moveRegionOut(cluster, source); op != nil {
				return op
			}
		}
		log.Debug("no operator created for selected stores", zap.String("scheduler", s.GetName()), zap.Uint64("source", source.GetID()))
	}
	return nil
}
```

moveRegionOut()尝试从给定的source store迁移出一个region到其他store
```go
func (s *balanceRegionScheduler) moveRegionOut(cluster opt.Cluster, source *core.StoreInfo) *operator.Operator {
	stores := cluster.GetStores()
    // 按照RegionSize顺序排列stores
	targets := filter.SelectTargetStores(stores, s.filters, cluster)
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})
    // getRegionFuncs是函数数组
	getRegionFuncs := make([]func(storeID uint64, opts ...core.RegionOption) *core.RegionInfo, 3)
	getRegionFuncs[0] = cluster.RandPendingRegion
	getRegionFuncs[1] = cluster.RandFollowerRegion
	getRegionFuncs[2] = cluster.RandLeaderRegion
	for i := 0; i < len(getRegionFuncs); i++ {
        // 先确定一个待迁移的region，迁移的优先顺序：pending, follower, leader
		region := getRegionFuncs[i](source.GetID())
		if region == nil {
			continue
		}
		regionStores := region.GetStoreIds()
        // 在region peers以外，满足RegionSize条件
		for j := 0; j < len(targets); j++ {
			target := targets[j]
            // 寻找合适的target store（RegionSize，已经）
			if target.GetRegionSize() >= source.GetRegionSize() {
				break
			}
			if _, ok := regionStores[target.GetID()]; ok {
				continue
			}
			if op := s.createOperator(cluster, region, source, target); op != nil {
				return op
			}
		}
	}
	return nil
}
```

#### 执行Operator

##### Scheduler向KV Server发送调度命令
如果Scheduler有针对某个region的调度策略，在收到该region的心跳后，会根据operator里的step向目标region发送命令（包括TransferLeader，AddPeer，RemovePeer）
```go
// HandleRegionHeartbeat processes RegionInfo reports from client.
func (c *RaftCluster) HandleRegionHeartbeat(region *core.RegionInfo) error {
    ...
    // 执行operator
	co.opController.Dispatch(region, schedule.DispatchFromHeartBeat)
	return nil
}

func (oc *OperatorController) Dispatch(region *core.RegionInfo, source string) {
	// Check existed operator.
	if op := oc.GetOperator(region.GetID()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			// When the "source" is heartbeat, the region may have a newer
			// confver than the region that the operator holds. In this case,
			// the operator is stale, and will not be executed even we would
			// have sent it to TiKV servers. Here, we just cancel it.
			origin := op.RegionEpoch()
			latest := region.GetRegionEpoch()
			changes := latest.GetConfVer() - origin.GetConfVer()
            // 检查confVer是否过期
			if source == DispatchFromHeartBeat &&
				changes > uint64(op.ConfVerChanged(region)) {

				if oc.RemoveOperator(op) {
					log.Info("stale operator", zap.Uint64("region-id", region.GetID()), zap.Duration("takes", op.RunningTime()),
						zap.Reflect("operator", op), zap.Uint64("diff", changes))
					oc.opRecords.Put(op, schedulerpb.OperatorStatus_CANCEL)
				}

				return
			}
            // 向目标region发送命令
			oc.SendScheduleCommand(region, step, source)
			return
		}
		if op.IsFinish() && oc.RemoveOperator(op) {
            // operator已经执行完毕
			log.Info("operator finish", zap.Uint64("region-id", region.GetID()), zap.Duration("takes", op.RunningTime()), zap.Reflect("operator", op))
			oc.opRecords.Put(op, schedulerpb.OperatorStatus_SUCCESS)
		} else if timeout && oc.RemoveOperator(op) {
            // operator超时了，删除之
			log.Info("operator timeout", zap.Uint64("region-id", region.GetID()), zap.Duration("takes", op.RunningTime()), zap.Reflect("operator", op))
			oc.opRecords.Put(op, schedulerpb.OperatorStatus_TIMEOUT)
		}
	}
}
```

##### KV Server执行Scheduler的调度命令
KV Server收到Scheduler的RegionHeartbeatResponse（包含调度命令）后，会调用SchedulerTaskHandler.onRegionHeartbeatResponse()处理
```go
func (c *client) receiveRegionHeartbeat(stream schedulerpb.Scheduler_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}

		if h := c.heartbeatHandler.Load(); h != nil {
            // 调用handler
			h.(func(*schedulerpb.RegionHeartbeatResponse))(resp)
		}
	}
}
```

client.heartbeatHandler的设置流程:
```go
func (r *SchedulerTaskHandler) Start() {
    // r.SchedulerClient是一个interface，具体实现是client
	r.SchedulerClient.SetRegionHeartbeatResponseHandler(r.storeID, r.onRegionHeartbeatResponse)
}

func (c *client) SetRegionHeartbeatResponseHandler(_ uint64, h func(*schedulerpb.RegionHeartbeatResponse)) {
	if h == nil {
		h = func(*schedulerpb.RegionHeartbeatResponse) {}
	}
	c.heartbeatHandler.Store(h)
}
```

onRegionHeartbeatResponse()会向相应的region发送AdminRequest，等待KV Server的apply
```go
func (r *SchedulerTaskHandler) onRegionHeartbeatResponse(resp *schedulerpb.RegionHeartbeatResponse) {
	if changePeer := resp.GetChangePeer(); changePeer != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerRequest{
				ChangeType: changePeer.ChangeType,
				Peer:       changePeer.Peer,
			},
		}, message.NewCallback())
	} else if transferLeader := resp.GetTransferLeader(); transferLeader != nil {
		r.sendAdminRequest(resp.RegionId, resp.RegionEpoch, resp.TargetPeer, &raft_cmdpb.AdminRequest{
			CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderRequest{
				Peer: transferLeader.Peer,
			},
		}, message.NewCallback())
	}
}
```