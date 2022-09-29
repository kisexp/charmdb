package charmdb

import (
	"bytes"
	"github.com/kisexp/charmdb/data/list"
	"github.com/kisexp/charmdb/storage"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ListIdx struct {
	mu      sync.RWMutex
	indexes *list.List
}

func newListIdx() *ListIdx {
	return &ListIdx{indexes: list.New()}
}

func (this *CharmDB) LPush(key []byte, values ...[]byte) (res int, err error) {
	if err = this.checkKeyValue(key, values...); err != nil {
		return 0, err
	}
	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListLPush)
		if err = this.store(e); err != nil {
			return
		}
		res = this.listIndex.indexes.LPush(string(key), val)
	}
	return
}

func (this *CharmDB) RPush(key []byte, values ...[]byte) (res int, err error) {
	if err = this.checkKeyValue(key, values...); err != nil {
		return
	}
	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, List, ListRPush)
		if err = this.store(e); err != nil {
			return
		}
		res = this.listIndex.indexes.RPush(string(key), val)
	}
	return
}

func (this *CharmDB) LPop(key []byte) ([]byte, error) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil, err
	}

	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()
	if this.checkExpired(key, List) {
		return nil, ErrKeyExpired
	}

	val := this.listIndex.indexes.LPop(string(key))
	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListLPop)
		if err := this.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

func (this *CharmDB) RPop(key []byte) ([]byte, error) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil, err
	}
	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	if this.checkExpired(key, List) {
		return nil, ErrKeyExpired
	}

	val := this.listIndex.indexes.RPop(string(key))
	if val != nil {
		e := storage.NewEntryNoExtra(key, val, List, ListRPop)
		if err := this.store(e); err != nil {
			return nil, err
		}
	}
	return val, nil
}

func (this *CharmDB) LIndex(key []byte, idx int) []byte {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.listIndex.mu.RLock()
	defer this.listIndex.mu.RUnlock()

	return this.listIndex.indexes.LIndex(string(key), idx)
}

func (this *CharmDB) LRem(key, value []byte, count int) (int, error) {
	if err := this.checkKeyValue(key, value); err != nil {
		return 0, err
	}
	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	if this.checkExpired(key, List) {
		return 0, ErrKeyExpired
	}

	res := this.listIndex.indexes.LRem(string(key), value, count)
	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(key, value, []byte(c), List, ListLRem)
		if err := this.store(e); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (this *CharmDB) LInsert(key string, option list.InsertOption, pivot, val []byte) (count int, err error) {
	if err = this.checkKeyValue([]byte(key), val); err != nil {
		return
	}
	if strings.Contains(string(pivot), ExtraSeparator) {
		return 0, ErrExtraContainsSeparator
	}

	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	count = this.listIndex.indexes.LInsert(key, option, pivot, val)
	if count != -1 {
		var buf bytes.Buffer
		buf.Write(pivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))

		e := storage.NewEntry([]byte(key), val, buf.Bytes(), List, ListLInsert)
		if err = this.store(e); err != nil {
			return
		}
	}
	return
}

func (this *CharmDB) LSet(key []byte, idx int, val []byte) (ok bool, err error) {
	if err := this.checkKeyValue(key, val); err != nil {
		return false, err
	}

	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	if ok = this.listIndex.indexes.LSet(string(key), idx, val); ok {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(key, val, []byte(i), List, ListLSet)
		if err := this.store(e); err != nil {
			return false, err
		}
	}
	return
}

func (this *CharmDB) LTrim(key []byte, start, end int) error {
	if err := this.checkKeyValue(key, nil); err != nil {
		return err
	}

	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()
	if this.checkExpired(key, List) {
		return ErrKeyExpired
	}
	if res := this.listIndex.indexes.LTrim(string(key), start, end); res {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))
		e := storage.NewEntry(key, nil, buf.Bytes(), List, ListLTrim)
		if err := this.store(e); err != nil {
			return err
		}
	}
	return nil
}

func (this *CharmDB) LRange(key []byte, start, end int) ([][]byte, error) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil, err
	}
	this.listIndex.mu.RLock()
	defer this.listIndex.mu.RUnlock()
	return this.listIndex.indexes.LRange(string(key), start, end), nil
}

func (this *CharmDB) LLen(key []byte) int {
	if err := this.checkKeyValue(key, nil); err != nil {
		return 0
	}

	this.listIndex.mu.RLock()
	defer this.listIndex.mu.RUnlock()
	return this.listIndex.indexes.LLen(string(key))
}

func (this *CharmDB) LKeyExists(key []byte) (ok bool) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}

	this.listIndex.mu.RLock()
	defer this.listIndex.mu.RUnlock()
	if this.checkExpired(key, List) {
		return false
	}
	ok = this.listIndex.indexes.LKeyExists(string(key))
	return
}

func (this *CharmDB) LValExists(key []byte, val []byte) (ok bool) {
	if err := this.checkKeyValue(key, val); err != nil {
		return
	}
	this.listIndex.mu.RLock()
	defer this.listIndex.mu.RUnlock()

	if this.checkExpired(key, List) {
		return false
	}
	ok = this.listIndex.indexes.LValExists(string(key), val)
	return
}

func (this *CharmDB) LClear(key []byte) (err error) {
	if err = this.checkKeyValue(key, nil); err != nil {
		return
	}
	if !this.LKeyExists(key) {
		return ErrKeyNotExist
	}
	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(key, nil, List, ListLClear)
	if err = this.store(e); err != nil {
		return err
	}
	this.listIndex.indexes.LClear(string(key))
	delete(this.expires[List], string(key))
	return
}

func (this *CharmDB) LExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !this.LKeyExists(key) {
		return ErrKeyNotExist
	}
	this.listIndex.mu.Lock()
	defer this.listIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, List, ListLExpire)
	if err = this.store(e); err != nil {
		return err
	}
	this.expires[List][string(key)] = deadline
	return
}

func (this *CharmDB) LTTL(key []byte) (ttl int64) {
	this.listIndex.mu.RLock()
	defer this.listIndex.mu.RUnlock()

	if this.checkExpired(key, List) {
		return
	}

	deadline, exist := this.expires[List][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
