# Project1 StandaloneKV
实现一个单机的存储引擎KV Server

Client和Server的交互流程：
    Client通过RPC调用Server上对应的API：RawGet，RawPut，RawDelete
    Server调用Storage的API从存储引擎中读取数据，或向存储引擎中写入数据，向CLient返回调用结果

实现单机存储引擎的Kv server分为两步：
- 1、基于badgerDB，实现Storage，StorageReader这两个interface的单机存储引擎StandAloneStorage
- 2、基于Storage实现Server层面的API：RawGet，RawPut，RawDelete


Server依赖于Storage，StorageReader这两个interface的实现，两者的定义如下：

```go
type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvrpcpb.Context, batch []Modify) error
	Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}

type StorageReader interface {
	// 从指定额的列族cf中读取key的value
	GetCF(cf string, key []byte) ([]byte, error)
    // 返回指定列族的迭代器
	IterCF(cf string) engine_util.DBIterator
	Close()
}
```

## 实现StandAloneStorage

通过对badgerDB进行封装，实现单机存储引擎，分为两部分：
- 实现Storage Interface: StandAloneStorage
- 实现Reader Interface: StandAloneStorageReader

### 实现Storage Interface： StandAloneStorage
```go
// 对badgerDB进行封装
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    // 根据config，调用badger.Open方法，打开一个badger实例
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
    return &StandAloneStorageReader{s.db.NewTransaction(false)}, nil
}

// 忽略掉了一些细节（包括错误处理），只保留了主干
// 将所有的modify转换成badgerDB类型的entry，放到badgerDB的事务里，然后调用badgerDB.Update方法一次性提交
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if len(batch) > 0 {
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, m := range batch {
				switch data := m.Data.(type) {
				case storage.Put:
					entry := &badger.Entry{
						Key:   engine_util.KeyWithCF(data.Cf, data.Key),
						Value: data.Value,
					}
					txn.SetEntry(entry);
				case storage.Delete:
					txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key));
				}
			}
			return nil
		})
	}
	return nil
}
```
### 实现Reader Interface：StandAloneStorageReader

StandAloneStorageReader负责实现快照读，因此需要包含一个badgerDB事务
```go 单机存储引擎的Reader
type StandAloneStorageReader struct {
    // badgerDB事务
	txn     *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
    // 快照单点读
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound{
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	r.txn.Discard()
}
```

            
## Server 对外提供Raw API

Server对外提供的Raw API有：
- RawGet
- RawPut
- RawDelete

### RawGet
```go
// RawGet获取一个key对应的value
// 调用server.storage.Reader.GetCF方法来实现
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
    ...
    reader, err = server.storage.Reader(req.GetContext());
    val, err = reader.GetCF(req.GetCf(), req.GetKey());
	if val == nil {
		return &kvrpcpb.RawGetResponse{Value: nil, NotFound: true}, nil
	} else {
		return &kvrpcpb.RawGetResponse{Value: val, NotFound: false}, nil
	}
}
```

### RawPut
```go
// RawPut向数据库中插入一行数据
// 构造Put请求，然后调用server.storage.Write方法
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	var batch []storage.Modify
	data := storage.Put{Key: req.GetKey(), Value: req.GetValue(), Cf: req.GetCf()}
	batch = append(batch, storage.Modify{Data: data})
	err := server.storage.Write(req.GetContext(), batch);
	return &kvrpcpb.RawPutResponse{}, nil
}
```

### RawDelete
```go
// RawDelete从数据库中删除key指定的行
// 构造Delete请求，然后调用server.storage.Write方法
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
    ...
	var batch []storage.Modify
	data := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	batch = append(batch, storage.Modify{Data: data})
	err := server.storage.Write(req.GetContext(), batch);
	return &kvrpcpb.RawDeleteResponse{}, nil
}
```

