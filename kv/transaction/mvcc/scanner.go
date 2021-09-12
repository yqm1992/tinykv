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
	writeIter engine_util.DBIterator
	lockIter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	lockIter := txn.Reader.IterCF(engine_util.CfLock)
	writeIter.Seek(keyMaxVersion(startKey))
	lockIter.Seek(startKey)
	return &Scanner{txn: txn, writeIter: writeIter, lockIter: lockIter}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.writeIter.Close()
	scan.lockIter.Close()
	scan.txn = nil
}

// nextUserKey will return the userKey and the most recent visible value, and seek to the next userKey
func (scan *Scanner) nextUserKey() (userKey []byte, mostRecentVisibleValue[]byte) {
	var val []byte
	var err error
	var write *Write
	var mostRecentWrite *Write

	writeIter := scan.writeIter
	if !writeIter.Valid() {
		return nil, nil
	}
	userKey = DecodeUserKey(writeIter.Item().Key())
	if userKey == nil {
		log.Fatalf("The userKey got from storage is nil")
	}

	for ; writeIter.Valid(); {
		item := writeIter.Item()
		if bytes.Compare(DecodeUserKey(item.Key()), userKey) != 0 {
			break
		}
		if writeTs := decodeTimestamp(item.Key()); writeTs > scan.txn.StartTS {
			writeIter.Next()
			continue
		}
		if val, err = item.Value(); err != nil {
			log.Fatal(err)
		}
		if write, err = ParseWrite(val); err != nil {
			log.Fatal(err)
		}
		// the deleted value is also visible to the transaction
		if write.Kind != WriteKindPut && write.Kind != WriteKindDelete {
			writeIter.Next()
			continue
		}
		mostRecentWrite = write
		// skip userKey's old versions
		writeIter.Seek(keyMinVersion(userKey))
		if writeIter.Valid() && bytes.Compare(DecodeUserKey(writeIter.Item().Key()), userKey) == 0 {
			writeIter.Next()
		}
		break
	}
	if mostRecentWrite != nil && mostRecentWrite.Kind == WriteKindPut {
		if mostRecentVisibleValue, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, mostRecentWrite.StartTS)); err != nil {
			log.Fatal(err)
		}
		if mostRecentVisibleValue == nil {
			log.Fatalf("The value of key: %s got from storage is nil", EncodeKey(userKey, mostRecentWrite.StartTS))
		}
	}
	return
}

// nextLock will return the key of lockIter and err (if the txn is locked by this lock), and seek to the next key (if it exists)
func (scan *Scanner) nextLock() (lockKey []byte, err error) {
	var lock *Lock
	var val []byte

	lockIter := scan.lockIter
	lockKey = lockIter.Item().Key()
	if lockKey == nil {
		log.Fatalf("the lockKey got from storage is nil")
	}
	if val, err = lockIter.Item().Value(); err != nil {
		log.Fatal(err)
	}
	if lock, err = ParseLock(val); err != nil {
		log.Fatal(err)
	}
	lockIter.Next()
	keyErr := &KeyError{}
	if lock.IsLockedFor(lockKey, scan.txn.StartTS, keyErr) {
		return lockKey, keyErr
	}
	return lockKey, nil
}

// compareIterWriteLock returns an integer comparing two iters.
// The result will be 0 if writeIter==lockIter, -1 if writeIter < lockIter, and +1 if  writeIter > lockIter.
// A invalid iter is equivalent to another invalid iter
// A invalid iter is bigger than the valid one
// Result of comparing two valid iters depends on the their keys' comparison
func compareIterWriteLock(writeIter engine_util.DBIterator, lockIter engine_util.DBIterator) int {
	if writeIter.Valid() && lockIter.Valid() {
		userKey := DecodeUserKey(writeIter.Item().Key())
		lockKey := lockIter.Item().Key()
		return bytes.Compare(userKey, lockKey)
	}
	if !writeIter.Valid() && !lockIter.Valid() {
		log.Warnf("both the writeIter and lockIter are invalid")
		return 0
	}
	if writeIter.Valid() {
		return -1
	} else {
		return 1
	}
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	writeIter := scan.writeIter
	lockIter := scan.lockIter

	for ; writeIter.Valid() || lockIter.Valid(); {
		switch compareIterWriteLock(writeIter, lockIter) {
		case -1: // the key only has writes
			if userKey, foundVal := scan.nextUserKey(); foundVal != nil {
				return userKey, foundVal, nil
			}
		case 0: // the key has both writes and lock
			lockKey, err := scan.nextLock()
			userKey, foundVal := scan.nextUserKey()
			// check the lock firstly
			if err != nil {
				return lockKey, nil, err
			}
			// check the write
			if foundVal != nil {
				return userKey, foundVal, nil
			}
		case 1: // the key only has lock
			if lockKey, err := scan.nextLock(); err != nil {
				return lockKey, nil, err
			}
		}
	}
	return nil, nil, nil
}
