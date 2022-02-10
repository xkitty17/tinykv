package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines // kv & raft badger
	config  *config.Config       // engines 的参数
}

// 创建 kv 和 raft engine
// 保存 config 参数
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"

	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, true)

	return &StandAloneStorage{
		engines: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

// https://github.com/tidb-incubator/tinykv/blob/course/doc/project1-StandaloneKV.md
// And you don’t need to consider the kvrpcpb.Context now, it’s used in the following projects.
// project1 用不到 context

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 创建一个只读的transaction
	txn := s.engines.Kv.NewTransaction(false)
	return &StandAloneStorageReader{txn: txn}, nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil // test-driven
	}
	return value, err
}

func (sr StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr StandAloneStorageReader) Close() {
	sr.txn.Discard() // 只读，只用调用这个，写的话需要只需要调用commit
}

// batch 是 modify数组 modify: Put/Delete操作
// Put: key val CF
// Delete: key CF
// badger本身不支持，用 engine_util里的 函数 将CF追加key上存储
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engines.Kv, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engines.Kv, modify.Cf(), modify.Key())
			if err != nil {
				return err
			}
		}
	}

	return nil
}
