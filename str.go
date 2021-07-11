package charmdb

import (
	"bytes"
	"charmdb/index"
	"charmdb/storage"
	"strings"
	"sync"
	"time"
)

type StrIdx struct {
	mu      sync.RWMutex
	idxList *index.SkipList
}

func newStrIdx() *StrIdx {
	return &StrIdx{idxList: index.NewSkipList()}
}

func (this *CharmDB) Set(key, value []byte) error {
	return this.doSet(key, value)
}

func (this *CharmDB) SetNx(key, value []byte) (res uint32, err error) {
	if exist := this.StrExists(key); exist {
		return
	}
	if err = this.Set(key, value); err == nil {
		res = 1
	}
	return
}

func (this *CharmDB) GetSet(key, val []byte) (res []byte, err error) {
	if res, err = this.Get(key); err != nil {
		return
	}
	if err = this.Set(key, val); err != nil {
		return
	}
	return
}

func (this *CharmDB) Append(key, value []byte) error {
	if err := this.checkKeyValue(key, value); err != nil {
		return err
	}
	existVal, err := this.Get(key)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return err
	}
	if len(existVal) > 0 {
		existVal = append(existVal, value...)
	} else {
		existVal = value
	}
	return this.doSet(key, existVal)
}

func (this *CharmDB) StrLen(key []byte) int {
	if err := this.checkKeyValue(key, nil); err != nil {
		return 0
	}
	this.strIndex.mu.RLock()
	defer this.strIndex.mu.RUnlock()

	e := this.strIndex.idxList.Get(key)
	if e != nil {
		if this.checkExpired(key, String) {
			return 0
		}
		idx := e.Value().(*index.Index)
		return int(idx.Meta.ValueSize)
	}
	return 0
}

func (this *CharmDB) StrExists(key []byte) bool {
	if err := this.checkKeyValue(key, nil); err != nil {
		return false
	}
	this.strIndex.mu.RLock()
	defer this.strIndex.mu.RUnlock()
	exist := this.strIndex.idxList.Exist(key)
	if exist && !this.checkExpired(key, String) {
		return true
	}
	return false
}

func (this *CharmDB) StrRem(key []byte) error {
	if err := this.checkKeyValue(key, nil); err != nil {
		return err
	}
	this.strIndex.mu.Lock()
	defer this.strIndex.mu.Unlock()
	e := storage.NewEntryNoExtra(key, nil, String, StringRem)
	if err := this.store(e); err != nil {
		return err
	}
	this.incrReclaimableSpace(key)
	this.strIndex.idxList.Remove(key)
	delete(this.expires[String], string(key))
	return nil
}

func (this *CharmDB) PrefixScan(prefix string, limit, offset int) (val [][]byte, err error) {
	if limit == 0 {
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = this.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}

	this.strIndex.mu.RLock()
	defer this.strIndex.mu.RUnlock()
	e := this.strIndex.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}
	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*index.Index)
		var value []byte
		if this.config.IdxMode == KeyOnlyMemMode {
			value, err = this.Get(e.Key())
			if err != nil {
				return
			}
		} else {
			if item != nil {
				value = item.Meta.Value
			}
		}

		expired := this.checkExpired(e.Key(), String)
		if !expired {
			val = append(val, value)
			e = e.Next()
		}
		if limit > 0 && !expired {
			limit--
		}
	}
	return
}

func (this *CharmDB) RangeScan(start, end []byte) (val [][]byte, err error) {
	node := this.strIndex.idxList.Get(start)
	this.strIndex.mu.RLock()
	defer this.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), end) <= 0 {
		if this.checkExpired(node.Key(), String) {
			node = node.Next()
			continue
		}

		var value []byte
		if this.config.IdxMode == KeyOnlyMemMode {
			value, err = this.Get(node.Key())
			if err != nil && err != ErrKeyNotExist {
				return nil, err
			}
		} else {
			value = node.Value().(*index.Index).Meta.Value
		}
		val = append(val, value)
		node = node.Next()
	}
	return
}

func (this *CharmDB) Expire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !this.StrExists(key) {
		return ErrKeyNotExist
	}
	this.strIndex.mu.Lock()
	defer this.strIndex.mu.Unlock()
	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, String, StringExpire)
	if err = this.store(e); err != nil {
		return err
	}
	this.expires[String][string(key)] = deadline
	return
}

func (this *CharmDB) Persist(key []byte) (err error) {
	val, err := this.Get(key)
	if err != nil {
		return err
	}
	this.strIndex.mu.Lock()
	defer this.strIndex.mu.Unlock()
	e := storage.NewEntryNoExtra(key, val, String, StringPersist)
	if err = this.store(e); err != nil {
		return
	}
	delete(this.expires[String], string(key))
	return
}

func (this *CharmDB) TTL(key []byte) (ttl int64) {
	this.strIndex.mu.Lock()
	defer this.strIndex.mu.Unlock()

	deadline, exist := this.expires[String][string(key)]
	if !exist {
		if expired := this.checkExpired(key, String); expired {
			return
		}
	}
	return deadline - time.Now().Unix()
}

func (this *CharmDB) doSet(key, value []byte) (err error) {
	if err = this.checkKeyValue(key, value); err != nil {
		return
	}
	if this.config.IdxMode == KeyValueMemMode {
		if existVal, _ := this.Get(key); existVal != nil && bytes.Compare(existVal, value) == 0 {
			return
		}
	}

	this.strIndex.mu.Lock()
	defer this.strIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, value, String, StringSet)
	if err := this.store(e); err != nil {
		return err
	}
	this.incrReclaimableSpace(key)
	if _, ok := this.expires[String][string(key)]; ok {
		delete(this.expires[String], string(key))
	}
	idx := &index.Index{
		Meta: &storage.Meta{
			KeySize:   uint32(len(e.Meta.Key)),
			Key:       e.Meta.Key,
			ValueSize: uint32(len(e.Meta.Value)),
		},
		FileId:    this.ActiveFileIds[String],
		EntrySize: e.Size(),
		Offset:    this.activeFile[String].Offset - int64(e.Size()),
	}
	if this.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = e.Meta.Value
	}
	this.strIndex.idxList.Put(idx.Meta.Key, idx)
	return
}

func (this *CharmDB) incrReclaimableSpace(key []byte) {
	oldIdx := this.strIndex.idxList.Get(key)
	if oldIdx != nil {
		index := oldIdx.Value().(*index.Index)
		if index != nil {
			space := int64(index.EntrySize)
			this.meta.ReclaimableSpace[index.FileId] += space
		}
	}
}

func (this *CharmDB) Get(key []byte) ([]byte, error) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil, err
	}
	node := this.strIndex.idxList.Get(key)
	if node == nil {
		return nil, ErrKeyNotExist
	}
	idx := node.Value().(*index.Index)
	if idx == nil {
		return nil, ErrNilIndexer
	}
	this.strIndex.mu.RLock()
	defer this.strIndex.mu.RUnlock()
	if this.checkExpired(key, String) {
		return nil, ErrKeyExpired
	}
	if this.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}
	if this.config.IdxMode == KeyOnlyMemMode {
		df := this.activeFile[String]
		if idx.FileId != this.ActiveFileIds[String] {
			df = this.archFiles[String][idx.FileId]
		}
		e, err := df.Read(idx.Offset)
		if err != nil {
			return nil, err
		}
		return e.Meta.Value, nil
	}
	return nil, ErrKeyNotExist
}