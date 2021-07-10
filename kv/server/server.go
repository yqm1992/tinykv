package server

import (
	"context"
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
	keyErrorWrap := &KeyErrorWrap{}
	for _, mut := range req.Mutations {
		key := mut.GetKey()
		val := mut.GetValue()
		op := mut.GetOp()
		// check if there is lock and write conflict
		if op == kvrpcpb.Op_Put || op == kvrpcpb.Op_Del {
			// check lock
			if lock, err = txn.GetLock(key); err != nil {
				return nil, err
			} else if lock.IsLockedFor(key, txn.StartTS, keyErrorWrap) {
				resp.Errors = append(resp.Errors, keyErrorWrap.Error)
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
		}
		switch op {
		case kvrpcpb.Op_Put:
			newLock.Kind = mvcc.WriteKindPut
			txn.PutLock(key, newLock)
			txn.PutValue(key, val)
		case kvrpcpb.Op_Del:
			newLock.Kind = mvcc.WriteKindDelete
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
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
