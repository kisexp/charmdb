package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
	//KeySize, ValueSize, ExtraSize, crc32 is uint32 type，4 bytes each
	//Type and Mark are 2 + 2 = 4 bytes
	//4 + 4 + 4 + 4 + 2 + 2 = 20
	entryHeaderSize = 20
)

// Value的数据结构类型
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type (
	// Entry 数据entry定义
	Entry struct {
		Meta  *Meta
		Type  uint16 // 数据类型
		Mark  uint16 // 数据操作类型
		crc32 uint32 // 校验和
	}

	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte // 操作Entry所需的额外信息
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	return &Entry{
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32((len(value))),
			ExtraSize: uint32(len(extra)),
		},
		Type: t,
		Mark: mark,
	}
}

func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

func (this *Entry) Size() uint32 {
	return entryHeaderSize + this.Meta.KeySize + this.Meta.ValueSize + this.Meta.ExtraSize
}

// Encode 对Entry进行编码，返回字节数组
func (this *Entry) Encode() ([]byte, error) {
	if this == nil || this.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := this.Meta.KeySize, this.Meta.ValueSize
	es := this.Meta.ExtraSize
	buf := make([]byte, this.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:18], es)
	binary.BigEndian.PutUint16(buf[16:18], this.Type)
	binary.BigEndian.PutUint16(buf[18:20], this.Mark)
	copy(buf[entryHeaderSize:entryHeaderSize+ks], this.Meta.Key)
	copy(buf[entryHeaderSize+ks:(entryHeaderSize+ks+vs)], this.Meta.Value)

	if es > 0 {
		copy(buf[(entryHeaderSize+ks+vs):(entryHeaderSize+ks+vs+es)], this.Meta.Extra)

	}

	crc := crc32.ChecksumIEEE(this.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)
	return buf, nil
}

// Decode 解码字节数组，返回Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	t := binary.BigEndian.Uint16(buf[16:18])
	mark := binary.BigEndian.Uint16(buf[18:20])
	crc := binary.BigEndian.Uint32(buf[0:4])

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		Type:  t,
		Mark:  mark,
		crc32: crc,
	}, nil
}
