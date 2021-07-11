package charmdb

import (
	"charmdb/data/set"
	"charmdb/storage"
	"sync"
	"time"
)

type SetIdx struct {
	mu      sync.RWMutex
	indexes *set.Set
}

func newSetIdx() *SetIdx {
	return &SetIdx{indexes: set.New()}
}

func (this *CharmDB) SAdd(key []byte, members ...[]byte) (res int, err error) {
	if err = this.checkKeyValue(key, members...); err != nil {
		return
	}
	this.setIndex.mu.Lock()
	defer this.setIndex.mu.Unlock()

	for _, m := range members {
		exist := this.setIndex.indexes.SIsMember(string(key), m)
		if !exist {
			e := storage.NewEntryNoExtra(key, m, Set, SetSAdd)
			if err = this.store(e); err != nil {
				return
			}
			res = this.setIndex.indexes.SAdd(string(key), m)
		}
	}
	return
}

func (this *CharmDB) SPop(key []byte, count int) (values [][]byte, err error) {
	if err = this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.setIndex.mu.Lock()
	defer this.setIndex.mu.Unlock()

	if this.checkExpired(key, Set) {
		return nil, ErrKeyExpired
	}

	values = this.setIndex.indexes.SPop(string(key), count)
	for _, v := range values {
		e := storage.NewEntryNoExtra(key, v, Set, SetSRem)
		if err = this.store(e); err != nil {
			return
		}
	}
	return
}

func (this *CharmDB) SIsMember(key, member []byte) bool {
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()

	if this.checkExpired(key, Set) {
		return false
	}
	return this.setIndex.indexes.SIsMember(string(key), member)
}

func (this *CharmDB) SRandMember(key []byte, count int) [][]byte {
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()

	if this.checkExpired(key, Set) {
		return nil
	}
	return this.setIndex.indexes.SRandMember(string(key), count)
}

func (this *CharmDB) SRem(key []byte, members ...[]byte) (res int, err error) {
	if err = this.checkKeyValue(key, members...); err != nil {
		return
	}
	this.setIndex.mu.Lock()
	defer this.setIndex.mu.Unlock()

	if this.checkExpired(key, Set) {
		return
	}

	for _, m := range members {
		if ok := this.setIndex.indexes.SRem(string(key), m); ok {
			e := storage.NewEntryNoExtra(key, m, Set, SetSRem)
			if err = this.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

func (this *CharmDB) SMove(src, dst, member []byte) error {
	this.setIndex.mu.Lock()
	defer this.setIndex.mu.Unlock()
	if this.checkExpired(src, Set) {
		return ErrKeyExpired
	}
	if this.checkExpired(dst, Set) {
		return ErrKeyExpired
	}
	if ok := this.setIndex.indexes.SMove(string(src), string(dst), member); ok {
		e := storage.NewEntry(src, member, dst, Set, SetSMove)
		if err := this.store(e); err != nil {
			return err
		}
	}
	return nil
}

func (this *CharmDB) SCard(key []byte) int {
	if err := this.checkKeyValue(key, nil); err != nil {
		return 0
	}
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()
	if this.checkExpired(key, Set) {
		return 0
	}
	return this.setIndex.indexes.SCard(string(key))
}

func (this *CharmDB) SMembers(key []byte) (val [][]byte) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()
	if this.checkExpired(key, Set) {
		return
	}
	return this.setIndex.indexes.SMembers(string(key))
}

func (this *CharmDB) SUnion(keys ...[]byte) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range keys {
		if this.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}
	return this.setIndex.indexes.SUnion(validKeys...)

}

func (this *CharmDB) SDiff(keys ...[]byte) (val [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()

	var validKeys []string
	for _, k := range keys {
		if this.checkExpired(k, Set) {
			continue
		}
		validKeys = append(validKeys, string(k))
	}

	return this.setIndex.indexes.SDiff(validKeys...)
}

func (this *CharmDB) SKeyExists(key []byte) (ok bool) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()
	if this.checkExpired(key, Set) {
		return
	}
	ok = this.setIndex.indexes.SKeyExists(string(key))
	return
}

func (this *CharmDB) SClear(key []byte) (err error) {
	if !this.SKeyExists(key) {
		return ErrKeyNotExist
	}
	this.setIndex.mu.Lock()
	defer this.setIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, Set, SetSClear)
	if err = this.store(e); err != nil {
		return
	}
	this.setIndex.indexes.SClear(string(key))
	return
}

func (this *CharmDB) SExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !this.SKeyExists(key) {
		return ErrKeyNotExist
	}
	this.setIndex.mu.Lock()
	defer this.setIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, Set, SetSExpire)
	if err = this.store(e); err != nil {
		return
	}
	this.expires[Set][string(key)] = deadline
	return
}

func (this *CharmDB) STTL(key []byte) (ttl int64) {
	this.setIndex.mu.RLock()
	defer this.setIndex.mu.RUnlock()
	if this.checkExpired(key, Set) {
		return
	}
	deadline, exist := this.expires[Set][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}