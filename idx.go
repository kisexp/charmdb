package charmdb

import (
	"github.com/kisexp/charmdb/data/list"
	"github.com/kisexp/charmdb/index"
	"github.com/kisexp/charmdb/storage"
	"github.com/kisexp/charmdb/utils"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DataType = uint16

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)

const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListLRem
	ListLInsert
	ListLSet
	ListLTrim
	ListLClear
	ListLExpire
)

const (
	HashHSet uint16 = iota
	HashHDel
	HashHClear
	HashHExpire
)

const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

const (
	ZSetZAdd uint16 = iota
	ZSetZRem
	ZSetZClear
	ZSetZExpire
)

func (this *CharmDB) buildStringIndex(idx *index.Index, entry *storage.Entry) {
	if this.listIndex == nil || idx == nil {
		return
	}

	switch entry.GetMark() {
	case StringSet:
		this.strIndex.idxList.Put(idx.Meta.Key, idx)
	case StringRem:
		this.strIndex.idxList.Remove(idx.Meta.Key)
	case StringExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			this.strIndex.idxList.Remove(idx.Meta.Key)
		} else {
			this.expires[String][string(idx.Meta.Key)] = int64(entry.Timestamp)
		}
	case StringPersist:
		this.strIndex.idxList.Put(idx.Meta.Key, idx)
		delete(this.expires[String], string(idx.Meta.Key))
	}
}

func (this *CharmDB) buildListIndex(idx *index.Index, entry *storage.Entry) {
	if this.listIndex == nil || idx == nil {
		return
	}
	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case ListLPush:
		this.listIndex.indexes.LPush(key, idx.Meta.Value)
	case ListLPop:
		this.listIndex.indexes.LPop(key)
	case ListRPush:
		this.listIndex.indexes.RPush(key, idx.Meta.Value)
	case ListRPop:
		this.listIndex.indexes.RPop(key)
	case ListLRem:
		if count, err := strconv.Atoi(string(idx.Meta.Extra)); err != nil {
			this.listIndex.indexes.LRem(key, idx.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err != nil {
				this.listIndex.indexes.LInsert(string(idx.Meta.Key), list.InsertOption(opt), pivot, idx.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(idx.Meta.Extra)); err != nil {
			this.listIndex.indexes.LSet(key, i, idx.Meta.Value)
		}
	case ListLTrim:
		extra := string(idx.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])
			this.listIndex.indexes.LTrim(string(idx.Meta.Key), start, end)
		}
	case ListLExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			this.listIndex.indexes.LClear(key)
		} else {
			this.expires[List][key] = int64(entry.Timestamp)
		}
	case ListLClear:
		this.listIndex.indexes.LClear(key)
	}
}

func (this *CharmDB) buildHashIndex(idx *index.Index, entry *storage.Entry) {
	if this.hashIndex == nil || idx == nil {
		return
	}
	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case HashHSet:
		this.hashIndex.indexes.HSet(key, string(idx.Meta.Extra), idx.Meta.Value)
	case HashHDel:
		this.hashIndex.indexes.HDel(key, string(idx.Meta.Extra))
	case HashHClear:
		this.hashIndex.indexes.HClear(key)
	case HashHExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			this.hashIndex.indexes.HClear(key)
		} else {
			this.expires[Hash][key] = int64(entry.Timestamp)
		}
	}
}

func (this *CharmDB) buildSetIndex(idx *index.Index, entry *storage.Entry) {
	if this.hashIndex == nil || idx == nil {
		return
	}
	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case SetSAdd:
		this.setIndex.indexes.SAdd(key, idx.Meta.Value)
	case SetSRem:
		this.setIndex.indexes.SRem(key, idx.Meta.Value)
	case SetSMove:
		extra := idx.Meta.Extra
		this.setIndex.indexes.SMove(key, string(extra), idx.Meta.Value)
	case SetSClear:
		this.setIndex.indexes.SClear(key)
	case SetSExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			this.setIndex.indexes.SClear(key)
		} else {
			this.expires[Set][key] = int64(entry.Timestamp)
		}
	}
}

func (this *CharmDB) buildZsetIndex(idx *index.Index, entry *storage.Entry) {
	if this.hashIndex == nil || idx == nil {
		return
	}
	key := string(idx.Meta.Key)
	switch entry.GetMark() {
	case ZSetZAdd:
		if score, err := utils.StrToFloat64(string(idx.Meta.Extra)); err != nil {
			this.zsetIndex.indexes.ZAdd(key, score, string(idx.Meta.Value))
		}
	case ZSetZRem:
		this.zsetIndex.indexes.ZRem(key, string(idx.Meta.Value))
	case ZSetZClear:
		this.zsetIndex.indexes.ZClear(key)
	case ZSetZExpire:
		if entry.Timestamp < uint64(time.Now().Unix()) {
			this.zsetIndex.indexes.ZClear(key)
		} else {
			this.expires[ZSet][key] = int64(entry.Timestamp)
		}
	}
}

func (this *CharmDB) loadIdxFromFiles() error {
	if this.archFiles == nil && this.activeFile == nil {
		return nil
	}
	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for dataType := 0; dataType < DataStructureNum; dataType++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)
			for k, v := range this.archFiles[dType] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}
			dbFile[this.ActiveFileIds[dType]] = this.activeFile[dType]
			fileIds = append(fileIds, int(this.ActiveFileIds[dType]))

			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df := dbFile[fid]
				var offset int64 = 0

				for offset <= this.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Index{
							Meta:      e.Meta,
							FileId:    fid,
							EntrySize: e.Size(),
							Offset:    offset,
						}
						offset += int64(e.Size())
						if len(e.Meta.Key) > 0 {
							if err := this.buildIndex(e, idx); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}
				}
			}
		}(uint16(dataType))
	}
	wg.Wait()
	return nil
}
