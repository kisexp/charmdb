package index

import "github.com/kisexp/charmdb/storage"

// Index 数据索引定义
type Index struct {
	Meta      *storage.Meta // 元数据信息
	FileId    uint32        // 存储数据的文件id
	EntrySize uint32        // 数据条目(Entry)的大小
	Offset    int64         // Entry数据的查询起始位置
}
