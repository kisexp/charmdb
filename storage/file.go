package storage

import (
	"errors"
	"fmt"
	"github.com/roseduan/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	// FilePerm 默认的创建文件权限
	FilePerm      = 0644
	PathSeparator = string(os.PathSeparator)
)

var (
	// DBFileFormatNames 默认数据文件名称格式化
	DBFileFormatNames = map[uint16]string{
		0: "%09d.data.str",
		1: "%09d.data.list",
		2: "%09d.data.hash",
		3: "%09d.data.set",
		4: "%09d.data.zset",
	}
	DBFileSuffixName = []string{"str", "list", "hash", "set", "zset"}
)

var (
	ErrEmptyEntry = errors.New("storage/db_file: entry or the Key of entry is empty")
)

type FileRWMethod uint8

const (
	// FileIO 表示文件数据读写使用系统标准IO
	FileIO FileRWMethod = iota
	// MMap 表示文件数据读写使用Mmap
	// MMap指的是将文件或其他设备映射至内存
	MMap
)

// DBFile rosedb数据文件定义
type DBFile struct {
	Id     uint32
	path   string
	File   *os.File
	mmap   mmap.MMap
	Offset int64
	method FileRWMethod
}

// NewDBFile 新建一个数据读写文件，如果是MMap，则需要Truncate文件并进行加载
func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64, eType uint16) (*DBFile, error) {
	filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatNames[eType], fileId)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
	if err != nil {
		return nil, err
	}

	df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}
	if method == FileIO {
		df.File = file
	} else {
		if err = file.Truncate(blockSize); err != nil {
			return nil, err
		}
		m, err := mmap.Map(file, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		df.mmap = m
	}
	return df, nil
}

// Read 从数据文件中读数据，offset是读的起始位置
func (this *DBFile) Read(offset int64) (e *Entry, err error) {
	var buf []byte
	if buf, err = this.readBuf(offset, int64(entryHeaderSize)); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}
	offset += entryHeaderSize
	if e.Meta.KeySize > 0 {
		var key []byte
		if key, err = this.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
			return
		}
		e.Meta.Key = key
	}
	offset += int64(e.Meta.KeySize)
	if e.Meta.ValueSize > 0 {
		var val []byte
		if val, err = this.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
			return
		}
		e.Meta.Value = val
	}
	offset += int64(e.Meta.ValueSize)
	if e.Meta.ExtraSize > 0 {
		var val []byte
		if val, err = this.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
			return
		}
		e.Meta.Extra = val
	}
	checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
	if checkCrc != e.crc32 {
		return nil, ErrInvalidCrc
	}
	return
}

func (this *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
	buf := make([]byte, n)
	if this.method == FileIO {
		_, err := this.File.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}
	}
	if this.method == MMap && offset <= int64(len(this.mmap)) {
		copy(buf, this.mmap[offset:])
	}
	return buf, nil
}

// Write 从文件的offset处开始写数据
func (this *DBFile) Write(e *Entry) error {
	if e == nil || e.Meta.KeySize == 0 {
		return ErrEmptyEntry
	}

	method := this.method
	writeOff := this.Offset
	encVal, err := e.Encode()
	if err != nil {
		return err
	}

	if method == FileIO {
		if _, err := this.File.WriteAt(encVal, writeOff); err != nil {
			return err
		}
	}
	if method == MMap {
		copy(this.mmap[writeOff:], encVal)
	}
	this.Offset += int64(e.Size())
	return nil
}

// Close 读写后进行关闭操作
// sync 关闭前是否持久化数据
func (this *DBFile) Close(sync bool) (err error) {
	if sync {
		err = this.Sync()
	}
	if this.File != nil {
		err = this.File.Close()
	}
	if this.mmap != nil {
		err = this.mmap.Unmap()
	}
	return
}

// Sync 数据持久化
func (this *DBFile) Sync() (err error) {
	if this.File != nil {
		err = this.File.Sync()
	}
	if this.mmap != nil {
		err = this.mmap.Flush()
	}
	return
}

// Build 加载数据文件
func Build(path string, method FileRWMethod, blockSize int64) (map[uint16]map[uint32]*DBFile, map[uint16]uint32, error) {
	dir, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, nil, err
	}
	fileIdsMap := make(map[uint16][]int)
	for _, d := range dir {
		if strings.Contains(d.Name(), ".data") {
			splitNames := strings.Split(d.Name(), ".")
			id, _ := strconv.Atoi(splitNames[0])
			switch splitNames[2] {
			case DBFileSuffixName[0]:
				fileIdsMap[0] = append(fileIdsMap[0], id)
			case DBFileSuffixName[1]:
				fileIdsMap[1] = append(fileIdsMap[1], id)
			case DBFileSuffixName[2]:
				fileIdsMap[2] = append(fileIdsMap[2], id)
			case DBFileSuffixName[3]:
				fileIdsMap[3] = append(fileIdsMap[3], id)
			case DBFileSuffixName[4]:
				fileIdsMap[4] = append(fileIdsMap[4], id)
			}
		}
	}

	activeFileIds := make(map[uint16]uint32)
	archFiles := make(map[uint16]map[uint32]*DBFile)
	var dataType uint16 = 0
	for ; dataType < 5; dataType++ {
		fileIDs := fileIdsMap[dataType]
		sort.Ints(fileIDs)
		files := make(map[uint32]*DBFile)
		var activeFileId uint32 = 0
		if len(fileIDs) > 0 {
			activeFileId = uint32((fileIDs[len(fileIDs)-1]))
			for i := 0; i < len(fileIDs)-1; i++ {
				id := fileIDs[i]
				file, err := NewDBFile(path, uint32(id), method, blockSize, dataType)
				if err != nil {
					return nil, nil, err
				}
				files[uint32(id)] = file
			}
		}
		archFiles[dataType] = files
		activeFileIds[dataType] = activeFileId
	}
	return archFiles, activeFileIds, nil
}
