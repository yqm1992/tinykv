package server

import (
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	if req == nil {
		return nil, nil
	}
	var reader storage.StorageReader
	var val []byte
	var err error

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}

	if val, err = reader.GetCF(req.GetCf(), req.GetKey()); err != nil {
		return nil, err
	}

	if val == nil {
		return &kvrpcpb.RawGetResponse{Value: nil, NotFound: true}, nil
	} else {
		return &kvrpcpb.RawGetResponse{Value: val, NotFound: false}, nil
	}
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	if req == nil {
		return nil, nil
	}
	var batch []storage.Modify
	data := storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}
	batch = append(batch, storage.Modify{Data: data})
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	if req == nil {
		return nil, nil
	}
	var batch []storage.Modify
	data := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	batch = append(batch, storage.Modify{Data: data})
	err := server.storage.Write(req.GetContext(), batch)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	if req == nil {
		return nil, nil
	}
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	var count uint32 = 0
	var kvpairs []*kvrpcpb.KvPair
	for iter.Seek(req.GetStartKey()) ; iter.Valid() && count < req.GetLimit(); iter.Next(){
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		kvpairs = append(kvpairs, &kvrpcpb.KvPair{Key: key, Value: value})
		count++
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvpairs}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	if req == nil {
		return nil, nil
	}
	if req.Key == nil {
		return &kvrpcpb.GetResponse{Error: &kvrpcpb.KeyError{Retryable: "the key is nil"}}, nil
	}
	var reader storage.StorageReader
	var lock *mvcc.Lock
	var val []byte
	var err error
	resp := &kvrpcpb.GetResponse{}

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	
	// TODO: if we need to acquire latch here ?

	if lock, err = txn.GetLock(req.GetKey()); err != nil {
		return nil, err
	}
	if lock.IsLockedFor(req.GetKey(), txn.StartTS, resp) {
		return resp, nil
	}
	if val, err = txn.GetValue(req.GetKey()); err != nil {
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
		resp.NotFound = false
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	if req == nil {
		return nil, nil
	}
	log.Debugf("prepare to prewrite transaction(startVersion: %v)", req.StartVersion)

	if len(req.Mutations) == 0 {
		return &kvrpcpb.PrewriteResponse{}, nil
	}

	var reader storage.StorageReader
	var err error
	var lock *mvcc.Lock

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	primaryLock := req.GetPrimaryLock()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// acquire latches
	var keysToLatch [][]byte
	for _, mut := range req.Mutations {
		key := mut.GetKey()
		if key != nil {
			keysToLatch = append(keysToLatch, key)
		}
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)

	resp := &kvrpcpb.PrewriteResponse{}
	type KeyErrorWrap struct{
		Error       *kvrpcpb.KeyError
	}
	for _, mut := range req.Mutations {
		key := mut.GetKey()
		val := mut.GetValue()
		op := mut.GetOp()
		// check if there is lock and write conflict
		if op == kvrpcpb.Op_Put || op == kvrpcpb.Op_Del {
			// check lock
			if lock, err = txn.GetLock(key); err != nil {
				return nil, err
			} else if lock != nil {
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: lock.Info(key)})
				return resp, nil
			}
			// check write conflict
			if recentWrite, writeTs, tempError := txn.MostRecentWrite(key); tempError != nil {
				return nil, tempError
			} else if recentWrite != nil && writeTs >= txn.StartTS {
				keyError := &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{StartTs: txn.StartTS, ConflictTs: writeTs, Key: key, Primary: primaryLock}}
				resp.Errors = append(resp.Errors, keyError)
				return resp, nil
			}
		}
		newLock := &mvcc.Lock{
			Primary: primaryLock,
			Ts: req.GetStartVersion(),
			Ttl: req.GetLockTtl(),
			Kind: mvcc.WriteKindFromProto(op),
		}
		switch op {
		case kvrpcpb.Op_Put:
			txn.PutLock(key, newLock)
			txn.PutValue(key, val)
		case kvrpcpb.Op_Del:
			txn.PutLock(key, newLock)
			txn.DeleteValue(key)
		case kvrpcpb.Op_Rollback:
			log.Fatalf("no implementation of op type: %v", mut.GetOp())
		case kvrpcpb.Op_Lock:
			log.Fatalf("no implementation of op type: %v", mut.GetOp())
		default:
			log.Fatalf("unknown op type: %v", mut.GetOp())
		}
	}
	if err = server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	if req == nil {
		return nil, nil
	}
	log.Infof("prepare to commit transaction(startVersion: %v)", req.StartVersion)
	if len(req.Keys) == 0 {
		return &kvrpcpb.CommitResponse{}, nil
	}
	var reader storage.StorageReader
	var err error
	var lock *mvcc.Lock
	resp := &kvrpcpb.CommitResponse{}

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// acquire latches
	var keysToLatch [][]byte
	for _, key := range req.Keys {
		if key != nil {
			keysToLatch = append(keysToLatch, key)
		}
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)
	// prepare to commit
	for _, key := range req.Keys {
		if key == nil {
			log.Warnf("commit key is nil, skip it !")
			continue
		}
		// check lock
		if lock, err = txn.GetLock(key); err != nil {
			return nil, err
		} else if lock == nil {
			log.Warnf("abort commit of transaction(startTS: %v), lock of key:%v is not found, the transaction may be rollback !", txn.StartTS, key)
			return resp, nil
		} else if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{Locked: lock.Info(key), Retryable: "the key is locked by another transaction"}
			return resp, nil
		}
		// it is unnecessary to check write conflict again, because the lock can promise it
		write := &mvcc.Write{StartTS: txn.StartTS, Kind: lock.Kind}
		txn.DeleteLock(key)
		txn.PutWrite(key, req.CommitVersion, write)
	}
	if err = server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	if req == nil {
		return nil, nil
	}

	var reader storage.StorageReader
	var err error
	resp := &kvrpcpb.ScanResponse{}

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scan := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scan.Close()

	for i := uint32(0); i < req.GetLimit(); i++ {
		if key, val, err := scan.Next(); err != nil {
			keyError := &kvrpcpb.KeyError{Retryable: err.Error()}
			resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Error: keyError})
		} else if key != nil{
			if val != nil {
				resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: key, Value: val})
			}
		} else {
			break
		}
	}
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	if req == nil || req.PrimaryKey == nil {
		return nil, nil
	}

	var reader storage.StorageReader
	var err error
	var lock *mvcc.Lock
	resp := &kvrpcpb.CheckTxnStatusResponse{Action: kvrpcpb.Action_NoAction}

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	req.GetCurrentTs()

	// check if the transaction has been written
	if write, writeTs, tempError := txn.CurrentWrite(req.PrimaryKey); tempError != nil {
		return nil, tempError
	} else if write != nil {
		if write.Kind == mvcc.WriteKindRollback {
			// has been rollback
			log.Warnf("the transaction(startTs: %v) has already been rollback", txn.StartTS)
		} else {
			// has been committed
			log.Warnf("the transaction(startTs: %v) has already been committed", txn.StartTS)
			resp.CommitVersion = writeTs
		}
	} else {
		// transaction has not been written
		// check if there is lock
		if lock, err = txn.GetLock(req.GetPrimaryKey()); err != nil {
			return nil, err
		} else if lock == nil || lock.Ts != txn.StartTS {
			// lock is not found or is hold by another transaction, the transaction hast not been pre-written
			keys := [][]byte{req.GetPrimaryKey()}
			rollbackReq := &kvrpcpb.BatchRollbackRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys}
			var rollbackResp *kvrpcpb.BatchRollbackResponse
			if rollbackResp, err = server.KvBatchRollback(nil, rollbackReq); err != nil {
				return nil, err
			} else if rollbackResp.GetRegionError() != nil {
				resp.RegionError = rollbackResp.GetRegionError()
			} else if rollbackResp.GetError() != nil {
				return nil, fmt.Errorf(rollbackResp.Error.GetAbort())
			} else {
				if lock == nil {
					log.Warnf("lock is not found, the transaction(startTs: %v) hast not been pre-written", txn.StartTS)
				} else {
					log.Warnf("lock is hold by another transaction, the transaction(startTs: %v) hast not been pre-written", txn.StartTS)
				}
				resp.Action = kvrpcpb.Action_LockNotExistRollback
			}
		} else {
			// has been pre-written(with lock hold)
			currentPhysicTime := mvcc.PhysicalTime(req.GetCurrentTs())
			lockPhysicTime := mvcc.PhysicalTime(lock.Ts)

			if currentPhysicTime >= lockPhysicTime + lock.Ttl {
				log.Warnf("the lock of transaction(startTs: %v) is timeout !", txn.StartTS)
				keys := [][]byte{req.GetPrimaryKey()}
				rollbackReq := &kvrpcpb.BatchRollbackRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys}
				var rollbackResp *kvrpcpb.BatchRollbackResponse
				if rollbackResp, err = server.KvBatchRollback(nil, rollbackReq); err != nil {
					return nil, err
				} else if rollbackResp.GetRegionError() != nil {
					resp.RegionError = rollbackResp.GetRegionError()
				} else if rollbackResp.GetError() != nil {
					return nil, fmt.Errorf(rollbackResp.Error.GetAbort())
				} else {
					resp.Action = kvrpcpb.Action_TTLExpireRollback
				}
			} else {
				log.Warnf("the lock of transaction(startTs: %v) is alive !", txn.StartTS)
				resp.LockTtl = lock.Ttl - (currentPhysicTime - lockPhysicTime)
			}
		}
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	if req == nil {
		return nil, nil
	}
	if len(req.Keys) == 0 {
		return &kvrpcpb.BatchRollbackResponse{}, nil
	}

	var reader storage.StorageReader
	var err error
	var lock *mvcc.Lock
	resp := &kvrpcpb.BatchRollbackResponse{}

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// acquire latches
	var keysToLatch [][]byte
	for _, key := range req.Keys {
		if key != nil {
			keysToLatch = append(keysToLatch, key)
		}
	}
	server.Latches.WaitForLatches(keysToLatch)
	defer server.Latches.ReleaseLatches(keysToLatch)
	// prepare to rollback
	for _, key := range req.Keys {
		if key == nil {
			log.Warnf("the key is nil, skip it !")
			continue
		}
		if lock, err = txn.GetLock(key); err != nil {
			return nil, err
		} else if lock == nil {
			// check if it is already committed or rollback
			if write, _, tempError := txn.CurrentWrite(key); tempError != nil {
				return nil, err
			} else if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					log.Warnf("transaction(startTS: %v) is already rollback !", txn.StartTS)
					continue
				} else {
					log.Warnf("can not abort transaction(startTS: %v), it is already committed !", txn.StartTS)
					resp.Error = &kvrpcpb.KeyError{Abort: "the transaction is already committed !"}
				}
				return resp, nil
			} else {
				log.Warnf("transaction(startTS: %v) has not been prewrite, rollback it in advance !", txn.StartTS)
			}
		} else if lock.Ts != txn.StartTS {
			log.Warnf("transaction(startTS: %v), the key:%v is locked by another transaction, rollback it in advance !", txn.StartTS, key)
		} else {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		write := &mvcc.Write{StartTS: txn.StartTS, Kind: mvcc.WriteKindRollback}
		txn.PutWrite(key, txn.StartTS, write)
	}
	if err = server.storage.Write(req.GetContext(), txn.Writes()); err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	if req == nil {
		return nil, nil
	}
	if req.GetStartVersion() == 0 {
		return &kvrpcpb.ResolveLockResponse{}, nil
	}
	if req.GetCommitVersion() != 0 && req.GetStartVersion() > req.GetCommitVersion() {
		return &kvrpcpb.ResolveLockResponse{Error: &kvrpcpb.KeyError{Abort: fmt.Sprintf("commitVersion(%v) is greater than startVersion(%v)", req.GetCommitVersion(), req.GetStartVersion())}}, nil
	}

	var reader storage.StorageReader
	var err error
	resp := &kvrpcpb.ResolveLockResponse{}
	var pairs []mvcc.KlPair

	if reader, err = server.storage.Reader(req.GetContext()); err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	if pairs, err = mvcc.AllLocksForTxn(txn); err != nil {
		return nil, err
	}
	var keys [][]byte
	// TODO: where to use the primaryKey ?
	//var primaryKey []byte
	for _, item := range pairs {
		keys = append(keys, item.Key)
		//primaryKey = item.Lock.Primary
	}
	if req.CommitVersion == 0 {
		// rollback
		rollbackReq := &kvrpcpb.BatchRollbackRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys}
		if rollbackResp, err := server.KvBatchRollback(nil, rollbackReq); err != nil {
			return nil, err
		} else {
			resp.RegionError = rollbackResp.GetRegionError()
			resp.Error = rollbackResp.GetError()
		}
	} else {
		// commit
		commitReq := &kvrpcpb.CommitRequest{Context: req.GetContext(), StartVersion: txn.StartTS, Keys: keys, CommitVersion: req.CommitVersion}
		if commitResp, err := server.KvCommit(nil, commitReq); err != nil {
			return nil, err
		} else {
			resp.RegionError = commitResp.GetRegionError()
			resp.Error = commitResp.GetError()
		}
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
