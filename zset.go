package charmdb

import (
	"charmdb/data/zset"
	"charmdb/storage"
	"charmdb/utils"
	"math"
	"sync"
	"time"
)

type ZsetIdx struct {
	mu      sync.RWMutex
	indexes *zset.SortedSet
}

func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{indexes: zset.New()}
}

func (this *CharmDB) ZAdd(key []byte, score float64, member []byte) error {
	if err := this.checkKeyValue(key, member); err != nil {
		return err
	}

	if oldScore := this.ZScore(key, member); oldScore == score {
		return nil
	}
	this.zsetIndex.mu.Lock()
	defer this.zsetIndex.mu.Unlock()
	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntry(key, member, extra, ZSet, ZSetZAdd)
	if err := this.store(e); err != nil {
		return err
	}
	this.zsetIndex.indexes.ZAdd(string(key), score, string(member))
	return nil
}

func (this *CharmDB) ZScore(key, member []byte) float64 {
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return math.MinInt64
	}
	return this.zsetIndex.indexes.ZScore(string(key), string(member))
}

func (this *CharmDB) ZCard(key []byte) int {
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()

	if this.checkExpired(key, ZSet) {
		return 0
	}
	return this.zsetIndex.indexes.ZCard(string(key))
}

func (this *CharmDB) ZRank(key, member []byte) int64 {
	if err := this.checkKeyValue(key, member); err != nil {
		return -1
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return -1
	}
	return this.zsetIndex.indexes.ZRank(string(key), string(member))
}

func (this *CharmDB) ZIncrBy(key []byte, increment float64, member []byte) (float64, error) {
	if err := this.checkKeyValue(key, member); err != nil {
		return increment, err
	}
	this.zsetIndex.mu.Lock()
	defer this.zsetIndex.mu.Unlock()
	increment = this.zsetIndex.indexes.ZIncrBy(string(key), increment, string(member))
	extra := utils.Float64ToStr(increment)
	e := storage.NewEntry(key, member, []byte(extra), ZSet, ZSetZAdd)
	if err := this.store(e); err != nil {
		return increment, err
	}
	return increment, nil
}

func (this *CharmDB) ZRange(key []byte, start, stop int) []interface{} {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZRange(string(key), start, stop)
}

func (this *CharmDB) ZRangeWithScores(key []byte, start, stop int) []interface{} {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()

	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZRangeWithScores(string(key), start, stop)
}

func (this *CharmDB) ZRevRange(key []byte, start, stop int) []interface{} {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return nil
	}

	return this.zsetIndex.indexes.ZRevRange(string(key), start, stop)
}

func (this *CharmDB) ZRevRangeWithScores(key []byte, start, stop int) []interface{} {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()

	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZRevRangeWithScores(string(key), start, stop)
}

func (this *CharmDB) ZRem(key, member []byte) (ok bool, err error) {
	if err = this.checkKeyValue(key, member); err != nil {
		return
	}

	this.zsetIndex.mu.Lock()
	defer this.zsetIndex.mu.Unlock()

	if ok = this.zsetIndex.indexes.ZRem(string(key), string(member)); ok {
		e := storage.NewEntryNoExtra(key, member, ZSet, ZSetZRem)
		if err = this.store(e); err != nil {
			return
		}
	}
	return
}

func (this *CharmDB) ZGetByRank(key []byte, rank int) []interface{} {
	this.zsetIndex.mu.RLock()
	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZGetByRank(string(key), rank)
}

func (this *CharmDB) ZScoreRange(key []byte, min, max float64) []interface{} {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZScoreRange(string(key), min, max)
}

func (this *CharmDB) ZRevScoreRange(key []byte, max, min float64) []interface{} {
	if err := this.checkKeyValue(key, nil); err != nil {
		return nil
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()

	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZRevScoreRange(string(key), max, min)
}

func (this *CharmDB) ZKeyExists(key []byte) (ok bool) {
	if err := this.checkKeyValue(key, nil); err != nil {
		return
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()

	if this.checkExpired(key, ZSet) {
		return
	}

	ok = this.zsetIndex.indexes.ZKeyExists(string(key))
	return
}

func (this *CharmDB) ZClear(key []byte) (err error) {
	if !this.ZKeyExists(key) {
		return ErrKeyNotExist
	}
	this.zsetIndex.mu.Lock()
	defer this.zsetIndex.mu.Unlock()
	e := storage.NewEntryNoExtra(key, nil, ZSet, ZSetZClear)
	if err = this.store(e); err != nil {
		return
	}
	this.zsetIndex.indexes.ZClear(string(key))
	return
}

func (this *CharmDB) ZExpire(key []byte, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTL
	}
	if !this.ZKeyExists(key) {
		return ErrKeyNotExist
	}
	this.zsetIndex.mu.Lock()
	defer this.zsetIndex.mu.Unlock()
	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(key, nil, deadline, ZSet, ZSetZExpire)
	if err = this.store(e); err != nil {
		return err
	}
	this.expires[ZSet][string(key)] = deadline
	return
}

func (this *CharmDB) ZTTL(key []byte) (ttl int64) {
	if !this.ZKeyExists(key) {
		return
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	deadline, exist := this.expires[ZSet][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}

func (this *CharmDB) ZRevRank(key, member []byte) int64 {
	if err := this.checkKeyValue(key, member); err != nil {
		return -1
	}
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return -1
	}
	return this.zsetIndex.indexes.ZRevRank(string(key), string(member))
}

func (this *CharmDB) ZRevGetByRank(key []byte, rank int) []interface{} {
	this.zsetIndex.mu.RLock()
	defer this.zsetIndex.mu.RUnlock()
	if this.checkExpired(key, ZSet) {
		return nil
	}
	return this.zsetIndex.indexes.ZRevGetByRank(string(key), rank)
}
