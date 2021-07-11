package charmdb

import (
	"bytes"
	"charmdb/data/hash"
	"charmdb/storage"
	"sync"
	"time"
)

type HashIdex struct {
	mu      sync.RWMutex
	indexes *hash.Hash
}

func newHashIdx() *HashIdex {
	return &HashIdex{indexes: hash.New()}
}

func (this *CharmDB) HSet(key []byte, field []byte, value []byte) (res int, err error) {
	if err = this.checkKeyValue(key, value); err != nil {
		return
	}

	oldVal := this.HGet(key, field)
	if bytes.Compare(oldVal, value) == 0 {
		return
	}

	this.hashIndex.mu.Lock()
	defer this.hashIndex.mu.Lock()

	e := storage.NewEntry(key, value, field, Hash, HashHSet)
	if err = this.store(e); err != nil {
		return
	}
	res = this.hashIndex.indexes.HSet(string(key), string(field), value)
	return
}

func (this *CharmDB) HSetNx(key, field, value []byte) (res int, err error) {
	if err = this.checkKeyValue(key, value); err != nil {
		return
	}
	this.hashIndex.mu.Lock()
	defer this.hashIndex.mu.Unlock()

	if res = this.hashIndex.indexes.HSetNx(string(key), string(field), value); res == 1 {
		e := storage.NewEntry(key, value, field, Hash, HashHSet)
		if err = this.store(e); err != nil {
			return
		}
	}
	return
}

func (this *CharmDB) HGet(key, field []byte) []byte {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}

	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return nil
	}

	return this.hashIndex.indexes.HGet(string(key), string(field))
}

func (this *CharmDB) HGetAll(key []byte) [][]byte {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}

	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return nil
	}

	return this.hashIndex.indexes.HGetAll(string(key))
}

func (this *CharmDB) HDel(key []byte, field ...[]byte) (res int, err error) {
	if err = this.checkKeyValue(key, nil); err != nil {
		return
	}
	if field == nil || len(field) == 0 {
		return
	}
	this.hashIndex.mu.Lock()
	defer this.hashIndex.mu.Unlock()

	for _, f := range field {
		if ok := this.hashIndex.indexes.HDel(string(key), string(f)); ok == 1 {
			e := storage.NewEntry(key, nil, f, Hash, HashHDel)
			if err = this.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

func (this *CharmDB) HLen(key []byte) int {
	if err := this.checkKeyValue(key, nil); err != nil {
		return 0
	}
	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return 0
	}
	return this.hashIndex.indexes.HLen(string(key))
}

func (this *CharmDB) HKeys(key []byte) (val []string) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return nil
	}
	return this.hashIndex.indexes.HKeys(string(key))
}

func (this *CharmDB) HVals(key []byte) (val [][]byte) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return nil
	}

	return this.hashIndex.indexes.HVals(string(key))
}

func (this *CharmDB) HClear(key []byte) (err error) {
	if err = this.checkKeyValue(key, nil); err != nil {
		return
	}

	if !this.HKeyExists(key) {
		return ErrKeyNotExist
	}

	this.hashIndex.mu.Lock()
	defer this.hashIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
	if err := this.store(e); err != nil {
		return err
	}
	this.hashIndex.indexes.HClear(string(key))
	delete(this.expires[Hash], string(key))
	return
}

func (this *CharmDB) HExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if err = this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.hashIndex.mu.Lock()
	defer this.hashIndex.mu.Unlock()
	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, Hash, HashHExpire)
	if err := this.store(e); err != nil {
		return err
	}

	this.expires[Hash][string(key)] = deadline
	return
}

func (this *CharmDB) HKeyExists(key []byte) (ok bool) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return
	}
	return this.hashIndex.indexes.HKeyExists(string(key))
}

func (this *CharmDB) HExists(key, field []byte) int {
	if err := this.checkKeyValue(key, nil); err != nil {
		return 0
	}
	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return 0
	}
	return this.hashIndex.indexes.HExists(string(key), string(field))
}

func (this *CharmDB) HTTL(key []byte) (ttl int64) {
	this.hashIndex.mu.RLock()
	defer this.hashIndex.mu.RUnlock()

	if this.checkExpired(key, Hash) {
		return
	}

	deadline, exist := this.expires[Hash][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
