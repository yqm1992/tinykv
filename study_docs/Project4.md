## Percolator分布式事务简介
Percolator是一个构建在BigTable（BigTable只支持单行事务）上的分布式事务算法，它能够提快照隔离级别的ACID语义，Percolator的实现需要分布式KV存储提供以下列族：
- lock （用来保存对key上的lock信息）
- write （用来记录commit信息）
- data （存储实际的数据）

Percolator还依赖于一个服务timestamp oracle，按照严格递增的顺序产生时间戳，所有的读写操作都要先从orace处获取一个时间戳TS，TS代表读或者写的开始时间.

percolator算法中事务的提交分为两个阶段：prewrite阶段，和commit阶段

prewrite阶段：
- 1、从oracle处获取一个时间戳，称之为事务的开始时间戳startTS
- 2、从事务涉及的所有key里，选出一个作为primay key，其余的作为secondary keys
- 3、对事务的每一个key加lock，向dataCF中写入要提交的value，primary key上的lock称之为primary lock；
	- 对key添加lock：向lockCF写入值：（key， primary_key + startTS），在lockCF里，属于同一个事务所有的key，对应的value都是primary_key + startTS
	- 向dataCF中写入要提交的value，并附上时间戳信息：（key+startTS， value）

如果存在key，被其他事务的lock锁住，或者在startTS之后有新的提交记录，当前的prewrite应该失败，并回滚

所有的key都prewrite成功后，就可以进入第二阶段了
commit阶段：
- 1、再次从timestamp oracle处获取一个时间戳，称之为commitTS
- 2、提交primary key：移除primary key上的lock，向writeCF中添加(primaryKey + commitTS, startTS)记录，记录下事务对应的startTS
- 3、对secondary keys重复上述步骤

在commit阶段步骤2完成后，整个事务就算提交成功了



TinyKv上实现了Percolator所依赖的Transactional API

## partA MVCC
在KvStorage的基础上实现MVCC API，transactional API会调用到这些MVCC API,作为事务的基础,

```go MVCC API有如下：
// PutWrite 对key进行commit或者rollback, 在txn添加一个对WriteCF的PUT操作（key+ts -> write），后续commit或rollback的时候会用到
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write);

// GetLock 获取key上的lock信息,在rollback，commit等中会用到
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error);

// PutLock对一个key加lock, 向txn中添加对lockCF的Put操作（key -> lock），在prewrite的时候会用到
func (txn *MvccTxn) PutLock(key []byte, lock *Lock);

// DeleteLock 删除key上的lock，向txn中添加对lockCF的Delete操作（key -> lock）,在commit或rollback的时候会用到
func (txn *MvccTxn) DeleteLock(key []byte);

// GetValue 查询key对事务txn的所有可见版本的value中最新的一个，在KvGet_中会用到
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error);

// PutValue在txn里添加一个对DefaultCF的PUT操作（key+ts->val），在prewrite的时候会用到
func (txn *MvccTxn) PutValue(key []byte, value []byte);

// DeleteValue 删除一个未提交的key，在txn里添加一个对DefaultCF的Delete操作（key+ts）,在commit或者rollback的时候会用到
func (txn *MvccTxn) DeleteValue(key []byte);

// CurrentWrite 根据事务的startTS去搜索key对应的write记录（在writeCF上）,用来检查事务的状态，在rollback以及KvCheckTxnStatus的时候会用到
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error);

// MostRecentWrite 搜索key对事务txn所有可见版本中最新的一次write记录（commitTS < txn.startTS）
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error);
```

## partB
基于MVCC API实现了三个事务API，分别是KvGet，KvPrewrite以及KvCommit，其中：
- KvGet：负责快照级别的单点读操作
- KvPrewrite： percolator第一阶段提交的API
- KvCommit： percolator第二阶段提交的API

### KvGet
事务读取key对应的value，如果key上有lock，并且事务会被lock锁住，直接返回lock信息；否则就在key的可见的已提交记录里，返回一个最近版本的value
```go
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// ... 精简掉细节，只保留主干
	lock, err = txn.GetLock(req.GetKey())
	// key上有lock && lock.TS < txn.startTS，证明lock会block住当前的事务
	if lock.IsLockedFor(req.GetKey(), txn.StartTS, resp) {
		return resp, nil
	}
	val, err = txn.GetValue(req.GetKey())
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
		resp.NotFound = false
	}
	return resp, nil
}
```

### KvPrewrite
prewrite是两阶段提交的第一阶段，prewrite的keys是事务keys的一个子集，如果存在任何key被lock住，或是在(startTS, +∞)期间有新的提交记录，prewrite会失败；prewrite成功的话，会对所有的key上锁，并写入预提交的值

下面是KvPrewrite的源码介绍，去除了一些细节部分，只保留了主干部分
```go
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// 构造MvccTxn
	// 获取内存中的latches, 防止访问过程中有key被修改
	...
	for _, mut := range req.Mutations {
		// 检查lock以及write记录
		lock, err = txn.GetLock(key);
		if lock != nil {
			// key上已经有lock，失败
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(key)})
			return resp, nil
		}
		recentWrite, writeTs, tempError := txn.MostRecentWrite(key);
		if recentWrite != nil && writeTs >= txn.StartTS {
			// (txn.StartTS, +∞) 有了新的提交记录，失败
			keyError := &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{StartTs: txn.StartTS, ConflictTs: writeTs, Key: key, Primary: primaryLock}}
			resp.Errors = append(resp.Errors, keyError)
			return resp, nil
		}
		// 写lock，defualt CF
		switch op {
		case kvrpcpb.Op_Put:
			txn.PutLock(key, newLock)
			txn.PutValue(key, val)
		case kvrpcpb.Op_Del:
			txn.PutLock(key, newLock)
			txn.DeleteValue(key)
		...
		}
		...
	}
	err = server.storage.Write(req.GetContext(), txn.Writes());
	return resp, nil
}
```

### KvCommit
KvCommit是两阶段提交的第二阶段，要先提交primary key，然后再提交secondary keys，调用KvCommit的前提是已经成功执行了pre-write

下面是KvCommit的源码介绍，去除了一些细节部分，只保留了主干部分
```go
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	...
	// 获取latches，防止其他事务对keys进行修改
	for _, key := range req.Keys {
		// check lock
		lock, err = txn.GetLock(key);
		if lock == nil {
			// 没有找到lock，停止commit
			return resp, nil
		} else if lock.Ts != txn.StartTS {
			// key被其他事务lock住了
			return resp, nil
		}
		// 这里不需要再次检查write conflict，因为key上有lock，可以保证
		// 向事务里添加Put write，以及DeleteLock操作
		write := &mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
		txn.DeleteLock(key)
		txn.PutWrite(key, req.CommitVersion, write)
	}
	// 正式提交
	server.storage.Write(req.GetContext(), txn.Writes());
	return resp, nil
}
```




## partC Transactional API

### Scanner

Scanner可以看做是一个迭代器，用来从数据库扫描多个连续的kv键值对，只保留事务可见的部分
scan.nextLock和scan.nextUserkey负责检查各自当前的userkey是否对事务可见
```go
// 获取下一个键值对，如果遍历完毕，会返回(nil, nil, nil)
// 忽略掉错误处理部分，只保留主干
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// 扫描的过程中，可能存在某些key被其他事务锁住，我们要同时考虑到writeCF和lockCF
	writeIter := scan.writeIter
	lockIter := scan.lockIter

	for ; writeIter.Valid() || lockIter.Valid(); {
		// compareIterWriteLock检查writeIter对应的userKey和lockIter对应的lockKey之间的大小关系
		// -1代表：userKey < lockKey, 当前需要处理的curKey是userKey, userKey只有write记录，没有lock
		// 0代表：userKey == lockKey，当前要处理的curKey是userKey(lockKey)，curKey同时有write记录和lock
		// 1代表：userKey > lockKey，当前要处理的curKey是lockKey，curKey只有lock没有write记录
		switch compareIterWriteLock(writeIter, lockIter) {
		case -1:
			if userKey, foundVal := scan.nextUserKey(); foundVal != nil {
				return userKey, foundVal, nil
			}
		case 0:
			// lock会锁住当前的事务，直接返回
			// 当前的key有对事务可见的value，直接返回
			// writeIter， lockIter都要seek到各自的下一个userKey
			lockKey, err := scan.nextLock()
			userKey, foundVal := scan.nextUserKey()
			if err != nil {
				return lockKey, nil, err
			}
			if foundVal != nil {
				return userKey, foundVal, nil
			}
		case 1:
			// 如果当前事务被lock锁住，可以直接返回
			if lockKey, err := scan.nextLock(); err != nil {
				return lockKey, nil, err
			}
		}
	}
	// scan完毕
	return nil, nil, nil
}
```


### KvScan
KvScan负责从数据库中读取多个值，实现依赖于scanner

下面是KvScan的源码介绍，去除了一些细节部分，只保留了主干部分
```go
func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	...
	scan := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scan.Close()

	for i := uint32(0); i < req.GetLimit(); i++ {
		if key, val, err := scan.Next(); err != nil {
			// 事务会被当前key上的lock锁住
			keyError := &kvrpcpb.KeyError{Retryable: err.Error()}
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Error: keyError})
		} else if key != nil{
			if val != nil {
				// 扫描到了一个对事务可见的键值对
				resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: val})
			}
		} else {
			// scan已经扫描完毕
			break
		}
	}
	return resp, nil
}
```

### KvBatchRollback
回滚一个事务：
	如果事务已经被commit，回复回滚失败
	如果事务已经rollback，不用做额外的操作
	如果key被当前事务pre-write了(lock != nil && lock.Ts == txn.StartTS)，清除掉lock以及未提交的value，然后标记回滚
	如果key没有被当前事务pre-write(lock == nil || lock.Ts != txn.StartTS)，不用做额外的操作


`之前的版本有错误`: lock != nil 并不能说明当前的key上不存在事务的write记录（commit或者是rollback），因此因为lock == nil就不检查writeCF是错误的；要先检查writeCF记录，再去检查lock

下面是KvBatchRollback的源码介绍，去除了一些细节部分，只保留了主干部分
```go
// 调用者要保证所有的key应该来自于同一个事务
func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	...
	// 在内存中获取keys对应的latches
	for _, key := range req.Keys {
		// 先检查writeCF中是否有记录
		write, _, tempError := txn.CurrentWrite(key);
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				// transaction 已经被rollback了
				continue
			} else {
				// 事务已经被committed，停止rollback
				resp.Error = &kvrpcpb.KeyError{Abort: "the transaction is already committed !"}
			}
			return resp, nil
		} else if (lock != nil && lock.Ts == txn.StartTS ) {
			// key处于prewrite完成的状态，回滚需要清除掉lock以及未提交的value
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		} else {
			// writeCF中不存在记录,lock有如下两种情况：
			// (1) lock == nil
			// 		key上不存在lock
			// (2) lock != nil && lock.Ts != txn.StartTS
			// 		key上存在的lock属于其他事务

			// 这两种情况都表示当前事务没有对key完成pre-write，因此可以直接rollback，不需要留下记录			
		}
		write := &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, txn.StartTS, write)
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	return resp, nil
}
```


### KvCheckTxnStatus
KvCheckTxnStatus检查当前事务的状态(根据primary key的状态进行判断)
	如果事务已经committed，返回commit状态
	如果事务已经rollback，返回rollback状态
	如果事务的lock过期了，会执行rollback操作，返回rollback状态
	如果事务没有被lock住，执行rollback操作，返回rollback状态

测试用例问题：
	TestCheckTxnStatusTtlNotExpired4C测试用例里面没有对LockTtl进行判断，无法区分是rollback还是lockAlived状态，可以添加：
		assert.Equal(t, true, resp.LockTtl > 0)

下面是KvCheckTxnStatus的源码介绍，去除了一些细节部分，只保留了主干部分
```go
func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	...
	// 首先检查事务是否已经rollback或者是committed
	write, writeTs, tempError := txn.CurrentWrite(req.PrimaryKey);
	if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			// 事务已经rollback 
			resp.CommitVersion = 0
			// ^^^ 其实不用写这条语句，resp.CommitVersion默认就是0
		} else {
			// 事务已经committed
			resp.CommitVersion = writeTs
		}
	} else {
		lock, err = txn.GetLock(req.GetPrimaryKey());
		if lock == nil || lock.Ts != txn.StartTS {
			// primary key上没有事务对应的lock（表示没有lock，或者primary key被其他事务lock住，表示primary key还未完成pre-write）
			// 执行rollback操作，标记resp.Action为Action_LockNotExistRollback
			keys := [][]byte{req.GetPrimaryKey()}
			rollbackReq := &kvrpcpb.BatchRollbackRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys}
			rollbackResp, err := server.KvBatchRollback(nil, rollbackReq);
			resp.Action = kvrpcpb.Action_LockNotExistRollback
		} else {
			// primary key已经pre-write
			currentPhysicTime := mvcc.PhysicalTime(req.GetCurrentTs())
			lockPhysicTime := mvcc.PhysicalTime(lock.Ts)

			if currentPhysicTime >= lockPhysicTime + lock.Ttl {
				// primay key的lock已经超时，对primary key执行回滚操作
				// 标记resp.Action为Action_TTLExpireRollback
				keys := [][]byte{req.GetPrimaryKey()}
				rollbackReq := &kvrpcpb.BatchRollbackRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys}
				rollbackResp, err = server.KvBatchRollback(nil, rollbackReq);
				resp.Action = kvrpcpb.Action_TTLExpireRollback
			} else {
				// primary key的lock仍处于active状态
				resp.LockTtl = lock.Ttl - (currentPhysicTime - lockPhysicTime)
			}
		}
	}
	return resp, nil
}
```

### KvResolveLock
用途：client判断好事务的状态后，会对secondary keys执行resolve lock的操作

KvResolveLock对给定的事务执行解锁操作，解锁的步骤大致如下：
- 首先会先找出该事务的所有locks
- 然后根据commit_version来决定执行rollback或者是commit操作（commit_version = 0代表回滚，大于0代表commit）

下面是KvResolveLock的源码介绍，去除了一些细节部分，只保留了主干部分
```go
// 该函数需要得到事务的状态后，才能调用
func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// 获取事务所拥有的所有locks
	pairs, err = mvcc.AllLocksForTxn(txn); 
	// 获取所有的keys
	var keys [][]byte
	for _, item := range pairs {
		keys = append(keys, item.Key)
	}
	// 根据CommitVersion决定对keys执行rollback或者commit操作
	if req.CommitVersion == 0 {
		// rollback
		rollbackReq := &kvrpcpb.BatchRollbackRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys}
		rollbackResp, err := server.KvBatchRollback(nil, rollbackReq);
	} else {
		// commit
		commitReq := &kvrpcpb.CommitRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys, CommitVersion: req.CommitVersion}
		commitResp, err := server.KvCommit(nil, commitReq);
	}
	return resp, nil
}
```


### TestScanLocked测试用例
Scan的测试用例中还没有扫描Locked的case，自己构造了一个简单测试用例TestScanLocked
```go
func TestScanLocked(t *testing.T) {
	builder := builderForScanLocked(t)

	req0 := builder.scanRequest([]byte{99}, 10000)
	req0.Version = 55

	req1 := builder.scanRequest([]byte{100}, 10000)
	req1.Version = 55

	req2 := builder.scanRequest([]byte{99}, 10000)
	req2.Version = 300

	req3 := builder.scanRequest([]byte{100}, 10000)
	req3.Version = 300

	resps := builder.runRequests(req0, req1, req2, req3)

	resp := resps[0].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 2, len(resp.Pairs))
	assert.Equal(t, []byte{99}, resp.Pairs[0].Key)
	assert.Equal(t, []byte{42}, resp.Pairs[0].Value)
	assert.Equal(t, []byte{100}, resp.Pairs[1].Key)
	assert.Equal(t, []byte{43}, resp.Pairs[1].Value)

	resp = resps[1].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 1, len(resp.Pairs))
	assert.Equal(t, []byte{100}, resp.Pairs[0].Key)
	assert.Equal(t, []byte{43}, resp.Pairs[0].Value)

	resp = resps[2].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 2, len(resp.Pairs))
	assert.Equal(t, true, resp.Pairs[0].Error != nil)
	assert.Equal(t, true, resp.Pairs[1].Error != nil)

	resp = resps[3].(*kvrpcpb.ScanResponse)
	assert.Nil(t, resp.RegionError)
	assert.Equal(t, 1, len(resp.Pairs))
	assert.Equal(t, true, resp.Pairs[0].Error != nil)

	//assert.Nil(t, resp0.RegionError)
	//assert.Nil(t, resp0.Error)
	//assert.Equal(t, []byte{42}, resp0.Value)
	//
	//assert.Nil(t, resp1.RegionError)
	//lockInfo := resp1.Error.Locked
	//assert.Equal(t, []byte{99}, lockInfo.Key)
	//assert.Equal(t, []byte{99}, lockInfo.PrimaryLock)
	//assert.Equal(t, uint64(200), lockInfo.LockVersion)
}

func builderForScanLocked(t *testing.T) *testBuilder {
	values := []kv{
		{cf: engine_util.CfDefault, key: []byte{99}, ts: 50, value: []byte{42}},
		{cf: engine_util.CfWrite, key: []byte{99}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
		{cf: engine_util.CfLock, key: []byte{99}, value: []byte{99, 1, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0}},

		{cf: engine_util.CfDefault, key: []byte{100}, ts: 50, value: []byte{43}},
		{cf: engine_util.CfWrite, key: []byte{100}, ts: 54, value: []byte{1, 0, 0, 0, 0, 0, 0, 0, 50}},
		{cf: engine_util.CfLock, key: []byte{100}, value: []byte{100, 1, 0, 0, 0, 0, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	builder := newBuilder(t)
	builder.init(values)
	return &builder
}
```