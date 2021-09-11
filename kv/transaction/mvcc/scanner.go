package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(keyMaxVersion(startKey))
	return &Scanner{txn: txn, iter: iter}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
	scan.txn = nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	var val []byte
	var err error
	var write *Write
	var foundKey []byte
	var foundVal []byte

	iter := scan.iter
	for ;iter.Valid();  {
		item := iter.Item()
		// is this write is visible to txn
		if writeTs := decodeTimestamp(item.Key()); writeTs > scan.txn.StartTS {
			iter.Next()
			continue
		}
		if val, err = item.Value(); err != nil {
			return nil, nil, err
		}
		if write, err = ParseWrite(val); err != nil {
			return nil, nil, err
		}
		// the deleted value is also visible to transaction
		if write.Kind != WriteKindPut && write.Kind != WriteKindDelete {
			iter.Next()
			continue
		}
		foundKey = DecodeUserKey(item.Key())
		if write.Kind == WriteKindPut {
			if val, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(foundKey, write.StartTS)); err != nil {
				return nil, nil, err
			}
			foundVal = val
		}
		// skip foundKey's old versions
		iter.Seek(keyMinVersion(foundKey))
		if iter.Valid() && bytes.Compare(DecodeUserKey(iter.Item().Key()), foundKey) == 0 {
			iter.Next()
		}
		if write.Kind == WriteKindDelete {
			log.Warnf("skip the deleted key: %v", foundKey)
			// The found write is delete, skips it
			foundKey = nil
			continue
		}
		break
	}
	return foundKey, foundVal, nil
}
