package charmdb

import (
	"bytes"
	"charmdb/index"
	"charmdb/storage"
	"charmdb/utils"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

var (
	// ErrEmptyKey the key is empty
	ErrEmptyKey = errors.New("charmdb: the key is empty")

	// ErrKeyNotExist key not exist
	ErrKeyNotExist = errors.New("charmdb: key not exist")

	// ErrKeyTooLarge the key too large
	ErrKeyTooLarge = errors.New("charmdb: key exceeded the max length")

	// ErrValueTooLarge the value too large
	ErrValueTooLarge = errors.New("charmdb: value exceeded the max length")

	// ErrNilIndexer the indexer is nil
	ErrNilIndexer = errors.New("charmdb: indexer is nil")

	// ErrCfgNotExist the config is not exist
	ErrCfgNotExist = errors.New("charmdb: the config file not exist")

	// ErrReclaimUnreached not ready to reclaim
	ErrReclaimUnreached = errors.New("charmdb: unused space not reach the threshold")

	// ErrExtraContainsSeparator extra contains separator
	ErrExtraContainsSeparator = errors.New("charmdb: extra contains separator \\0")

	// ErrInvalidTTL ttl is invalid
	ErrInvalidTTL = errors.New("charmdb: invalid ttl")

	// ErrKeyExpired the key is expired
	ErrKeyExpired = errors.New("charmdb: key is expired")

	// ErrDBisReclaiming reclaim and single reclaim can`t execute at the same time.
	ErrDBisReclaiming = errors.New("charmdb: can`t do reclaim and single reclaim at the same time")
)

const (
	configSaveFile   = string(os.PathSeparator) + "DB.CFG"
	dbMetaSaveFile   = string(os.PathSeparator) + "DB.META"
	reclaimPath      = string(os.PathSeparator) + "charmdb_reclaim"
	ExtraSeparator   = "\\0"
	DataStructureNum = 5
)

type (
	CharmDB struct {
		activeFile         ActiveFiles
		ActiveFileIds      ActiveFileIds
		archFiles          ArchivedFiles
		strIndex           *StrIdx
		listIndex          *ListIdx
		hashIndex          *HashIdex
		setIndex           *SetIdx
		zsetIndex          *ZsetIdx
		config             Config
		mu                 sync.RWMutex
		meta               *storage.DBMeta
		expires            Expires
		isReclaiming       bool
		isSingleReclaiming bool
	}
	ActiveFiles   map[DataType]*storage.DBFile
	ActiveFileIds map[DataType]uint32
	ArchivedFiles map[DataType]map[uint32]*storage.DBFile
	Expires       map[DataType]map[string]int64
)

func Open(config Config) (*CharmDB, error) {
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)
	if err != nil {
		return nil, err
	}

	activeFiles := make(ActiveFiles)
	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles[dataType] = file
	}

	meta := storage.LoadMeta(config.DirPath + dbMetaSaveFile)
	for dataType, file := range activeFiles {
		file.Offset = meta.ActiveWriteOff[dataType]
	}
	db := &CharmDB{
		activeFile:    activeFiles,
		ActiveFileIds: activeFileIds,
		archFiles:     archFiles,
		config:        config,
		strIndex:      newStrIdx(),
		meta:          meta,
		listIndex:     newListIdx(),
		hashIndex:     newHashIdx(),
		setIndex:      newSetIdx(),
		zsetIndex:     newZsetIdx(),
		expires:       make(Expires),
	}

	for i := 0; i < DataStructureNum; i++ {
		db.expires[uint16(i)] = make(map[string]int64)
	}

	if err := db.loadIdxFromFiles(); err != nil {
		return nil, err
	}

	return db, err
}

func Reopen(path string) (*CharmDB, error) {
	if exist := utils.Exist(path + configSaveFile); !exist {
		return nil, ErrCfgNotExist
	}

	var config Config
	b, err := ioutil.ReadFile(path + configSaveFile)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &config); err != nil {
		return nil, err
	}
	return Open(config)
}

func (this *CharmDB) Close() error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if err := this.saveConfig(); err != nil {
		return err
	}
	if err := this.saveMeta(); err != nil {
		return err
	}

	for _, file := range this.activeFile {
		if err := file.Close(true); err != nil {
			return err
		}
	}

	for _, archFile := range this.archFiles {
		for _, file := range archFile {
			if err := file.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (this *CharmDB) Sync() error {
	if this == nil || this.activeFile == nil {
		return nil
	}

	this.mu.RLock()
	defer this.mu.RUnlock()

	for _, file := range this.activeFile {
		if err := file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (this *CharmDB) Reclaim() (err error) {
	if this.isSingleReclaiming {
		return ErrDBisReclaiming
	}
	var reclaimable bool
	for _, archFiles := range this.archFiles {
		if len(archFiles) >= this.config.ReclaimThreshold {
			reclaimable = true
			break
		}
	}
	if !reclaimable {
		return ErrReclaimUnreached
	}

	reclaimPath := this.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	this.mu.Lock()
	defer func() {
		this.isSingleReclaiming = false
		this.mu.Unlock()
	}()
	this.isReclaiming = true

	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}
	wg := sync.WaitGroup{}
	wg.Add(DataStructureNum)
	for i := 0; i < DataStructureNum; i++ {
		go func(dType uint16) {
			defer func() {
				wg.Done()
			}()

			if len(this.archFiles[dType]) < this.config.ReclaimThreshold {
				newArchivedFiles.Store(dType, this.archFiles[dType])
				return
			}

			var (
				df        *storage.DBFile
				fileId    uint32
				archFiles = make(map[uint32]*storage.DBFile)
				fileIds   []int
			)
			sort.Ints(fileIds)

			for _, fid := range fileIds {
				file := this.archFiles[dType][uint32(fid)]
				var offset int64 = 0
				var reclaimEntries []*storage.Entry

				for {
					if e, err := file.Read(offset); err == nil {
						if this.validEntry(e, offset, file.Id) {
							reclaimEntries = append(reclaimEntries, e)
						}
						offset += int64(e.Size())
					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("err occurred when read the entry: %+v", err)
						return
					}
				}

				for _, entry := range reclaimEntries {
					if df == nil || int64(entry.Size())+df.Offset > this.config.BlockSize {
						df, err = storage.NewDBFile(reclaimPath, fileId, this.config.RwMethod, this.config.BlockSize, dType)
						if err != nil {
							log.Fatalf("err occurred when create new db file: %+v", err)
							return
						}
						archFiles[fileId] = df
						fileId += 1
					}

					if err = df.Write(entry); err != nil {
						log.Fatalf("err occurred when write the entry: %+v", err)
						return
					}

					if dType == String {
						item := this.strIndex.idxList.Get(entry.Meta.Key)
						idx := item.Value().(*index.Index)
						idx.Offset = df.Offset - int64(entry.Size())
						idx.FileId = fileId
						this.strIndex.idxList.Put(idx.Meta.Key, idx)
					}
				}
			}
			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)
		}(uint16(i))
	}
	wg.Wait()

	dbArchivedFiles := make(ArchivedFiles)
	for i := 0; i < DataStructureNum; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after reclaiming.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	for dataType, files := range this.archFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, this.config.DirPath+name)
			}
		}
	}

	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[dataType], f.Id)
				os.Rename(reclaimPath+name, this.config.DirPath+name)
			}
		}
	}

	this.archFiles = dbArchivedFiles
	return
}

func (this *CharmDB) SingleReclaim() (err error) {
	if this.isReclaiming {
		return ErrDBisReclaiming
	}

	reclaimPath := this.config.DirPath + reclaimPath
	if err := os.MkdirAll(reclaimPath, os.ModePerm); err != nil {
		return err
	}
	defer os.RemoveAll(reclaimPath)

	this.mu.Lock()
	defer func() {
		this.isSingleReclaiming = false
		this.mu.Unlock()
	}()

	this.isSingleReclaiming = true
	var fileIds []int
	for _, file := range this.archFiles[String] {
		fileIds = append(fileIds, int(file.Id))
	}
	sort.Ints(fileIds)

	for _, fid := range fileIds {
		file := this.archFiles[String][uint32(fid)]
		if this.meta.ReclaimableSpace[file.Id] < this.config.SingleReclaimThreshold {
			continue
		}

		var (
			readOff      int64
			validEntries []*storage.Entry
		)

		for {
			entry, err := file.Read(readOff)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			if this.validEntry(entry, readOff, uint32(fid)) {
				validEntries = append(validEntries, entry)
			}
			readOff += int64(entry.Size())
		}

		if len(validEntries) == 0 {
			os.Remove(file.File.Name())
			delete(this.meta.ReclaimableSpace, uint32(fid))
			delete(this.archFiles[String], uint32(fid))
			continue
		}

		df, err := storage.NewDBFile(reclaimPath, uint32(fid), this.config.RwMethod, this.config.BlockSize, String)
		if err != nil {
			return err
		}
		for _, e := range validEntries {
			if err := df.Write(e); err != nil {
				return err
			}
			item := this.strIndex.idxList.Get(e.Meta.Key)
			idx := item.Value().(*index.Index)
			idx.Offset = df.Offset - int64(e.Size())
			idx.FileId = uint32(fid)
			this.strIndex.idxList.Put(idx.Meta.Key, idx)
		}

		os.Remove(file.File.Name())
		name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormatNames[String], fid)
		os.Rename(reclaimPath+name, this.config.DirPath+name)

		this.meta.ReclaimableSpace[uint32(fid)] = 0
		this.archFiles[String][uint32(fid)] = df
	}
	return
}

func (this *CharmDB) Backup(dir string) (err error) {
	if utils.Exist(this.config.DirPath) {
		err = utils.CopyDir(this.config.DirPath, dir)
	}
	return
}

func (this *CharmDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}
	config := this.config
	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}
	return nil
}

func (this *CharmDB) saveConfig() (err error) {
	path := this.config.DirPath + configSaveFile
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	b, err := json.Marshal(this.config)
	_, err = file.Write(b)
	err = file.Close()
	return
}

func (this *CharmDB) saveMeta() error {
	methPath := this.config.DirPath + dbMetaSaveFile
	return this.meta.Store(methPath)
}

func (this *CharmDB) buildIndex(entry *storage.Entry, idx *index.Index) error {
	if this.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		this.buildStringIndex(idx, entry)
	case storage.List:
		this.buildListIndex(idx, entry)
	case storage.Hash:
		this.buildHashIndex(idx, entry)
	case storage.Set:
		this.buildSetIndex(idx, entry)
	case storage.ZSet:
		this.buildZsetIndex(idx, entry)
	}
	return nil
}

func (this *CharmDB) store(e *storage.Entry) error {
	config := this.config
	if this.activeFile[e.GetType()].Offset+int64(e.Size()) > config.BlockSize {
		if err := this.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
		activeFileId := this.ActiveFileIds[e.GetType()]
		this.archFiles[e.GetType()][activeFileId] = this.activeFile[e.GetType()]
		activeFileId = activeFileId + 1

		newDbFile, err := storage.NewDBFile(config.DirPath, activeFileId, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		this.activeFile[e.GetType()] = newDbFile
		this.ActiveFileIds[e.GetType()] = activeFileId
		this.meta.ActiveWriteOff[e.GetType()] = 0
	}

	if err := this.activeFile[e.GetType()].Write(e); err != nil {
		return err
	}
	this.meta.ActiveWriteOff[e.GetType()] = this.activeFile[e.GetType()].Offset

	if config.Sync {
		if err := this.activeFile[e.GetType()].Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (this *CharmDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	if e == nil {
		return false
	}
	mark := e.GetMark()
	switch e.GetType() {
	case String:
		deadline, exist := this.expires[String][string(e.Meta.Key)]
		now := time.Now().Unix()
		if mark == StringExpire {
			if exist && deadline <= now {
				return true
			}
		}
		if mark == StringSet || mark == StringPersist {
			if exist && deadline <= now {
				return false
			}

			node := this.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			index := node.Value().(*index.Index)
			if bytes.Compare(index.Meta.Key, e.Meta.Key) == 0 {
				if index != nil && index.FileId == fileId && index.Offset == offset {
					return true
				}
			}
		}
	case List:
		if mark == ListLExpire {
			deadline, exist := this.expires[List][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			if this.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case Hash:
		if mark == HashHExpire {
			deadline, exist := this.expires[Hash][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == HashHSet {
			if val := this.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}
	case Set:
		if mark == SetSExpire {
			deadline, exist := this.expires[Set][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == SetSMove {
			if this.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
		if mark == SetSAdd {
			if this.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case ZSet:
		if mark == ZSetZExpire {
			deadline, exist := this.expires[ZSet][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == ZSetZAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err != nil {
				score := this.ZScore(e.Meta.Key, e.Meta.Value)
				if score == val {
					return true
				}
			}
		}
	}
	return false
}

func (this *CharmDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := this.expires[dType][string(key)]
	if !exist {
		return
	}
	if time.Now().Unix() > deadline {
		expired = true
		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, String, StringRem)
			if ele := this.strIndex.idxList.Remove(key); ele != nil {
				this.incrReclaimableSpace(key)
			}
		case List:
			e = storage.NewEntryNoExtra(key, nil, List, ListLClear)
			this.listIndex.indexes.LClear(string(key))
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, Hash, HashHClear)
			this.listIndex.indexes.LClear(string(key))
		case Set:
			e = storage.NewEntryNoExtra(key, nil, Set, SetSClear)
			this.zsetIndex.indexes.ZClear(string(key))
		}
		if err := this.store(e); err != nil {
			log.Println("checkExpired: store entry err:", err)
			return
		}
		delete(this.expires[dType], string(key))
	}
	return

}
