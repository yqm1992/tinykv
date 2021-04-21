package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil
	}
	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{s, nil, nil}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if len(batch) > 0 {
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, m := range batch {
				var err1 error
				switch data := m.Data.(type) {
				case storage.Put:
					entry := &badger.Entry{
						Key:   engine_util.KeyWithCF(data.Cf, data.Key),
						Value: data.Value,
					}
					err1 = txn.SetEntry(entry)
				case storage.Delete:
					entry := &badger.Entry{
						Key: engine_util.KeyWithCF(data.Cf, data.Key),
					}
					err1 = txn.Delete(entry.Key)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
	txn     *badger.Txn
	iter    engine_util.DBIterator
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(s.storage.db, cf, key)
	if err == badger.ErrKeyNotFound{
		return nil, nil
	}
	return val, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	s.txn = s.storage.db.NewTransaction(false)
	s.iter = engine_util.NewCFIterator(cf, s.txn)
	return s.iter
}

func (s *StandAloneStorageReader) Close() {
	s.iter.Close()
	s.txn.Discard()
}
