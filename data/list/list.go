package list

import (
	"container/list"
	"reflect"
)

type InsertOption uint8

const (
	Before InsertOption = iota
	After
)

var existFlag = struct{}{}

type (
	List struct {
		record Record
		values map[string]map[string]struct{}
	}
	Record map[string]*list.List
)

func New() *List {
	return &List{
		make(Record),
		make(map[string]map[string]struct{}),
	}
}

// LPush 在列表的头部添加元素，返回添加后的列表长度
func (this *List) LPush(key string, val ...[]byte) int {
	return this.push(true, key, val...)
}

// LPop 取出列表头部的元素
func (this *List) LPop(key string) []byte {
	return this.pop(true, key)
}

// RPush 在列表的尾部添加元素，返回添加后的列表长度
func (this *List) RPush(key string, val ...[]byte) int {
	return this.push(false, key, val...)
}

// RPop 取出列表尾部的元素
func (this *List) RPop(key string) []byte {
	return this.pop(false, key)
}

// LIndex 返回列表在index处的值，如果不存在则返回nil
func (this *List) LIndex(key string, index int) []byte {
	ok, newIndex := this.validIndex(key, index)
	if !ok {
		return nil
	}
	index = newIndex
	var val []byte
	e := this.index(key, index)
	if e != nil {
		val = e.Value.([]byte)
	}
	return val
}

// LRem 根据参数 count 的值，移除列表中与参数 value 相等的元素
// count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count
// count < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值
// count = 0 : 移除列表中所有与 value 相等的值
// 返回成功删除的元素个数
func (this *List) LRem(key string, val []byte, count int) int {
	item := this.record[key]
	if item == nil {
		return 0
	}
	var ele []*list.Element
	if count == 0 {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	} else if count > 0 {
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	} else if count < 0 {
		for p := item.Back(); p != nil && len(ele) < -count; p = p.Prev() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				ele = append(ele, p)
			}
		}
	}

	for _, e := range ele {
		item.Remove(e)
	}
	length := len(ele)
	ele = nil

	if this.values[key] != nil {
		delete(this.values[key], string(val))
	}
	return length
}

// LInsert 将值 val 插入到列表 key 当中，位于值 pivot 之前或之后
// 如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到 pivot ，返回 -1
func (this *List) LInsert(key string, option InsertOption, pivot, val []byte) int {
	e := this.find(key, pivot)
	if e == nil {
		return -1
	}
	item := this.record[key]
	switch option {
	case Before:
		item.InsertBefore(val, e)
	case After:
		item.InsertAfter(val, e)
	}
	if this.values[key] == nil {
		this.values[key] = make(map[string]struct{})
	}
	this.values[key][string(val)] = existFlag
	return item.Len()
}

// LSet 将列表 key 下标为 index 的元素的值设置为 val
// bool返回值表示操作是否成功
func (this *List) LSet(key string, index int, val []byte) bool {
	e := this.index(key, index)
	if e == nil {
		return false
	}
	if this.values[key] == nil {
		this.values[key] = make(map[string]struct{})
	}
	if e.Value != nil {
		delete(this.values[key], string(e.Value.([]byte)))
	}
	e.Value = val
	this.values[key][string(val)] = existFlag
	return true
}

// LRange 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 end 指定
// 如果 start 下标比列表的最大下标(len-1)还要大，那么 LRange 返回一个空列表
// 如果 end 下标比 len 还要大，则将 end 的值设置为 len - 1
func (this *List) LRange(key string, start, end int) [][]byte {
	var val [][]byte
	item := this.record[key]
	if item == nil || item.Len() <= 0 {
		return val
	}
	length := item.Len()
	start, end = this.handleIndex(length, start, end)
	if start > end || start >= length {
		return val
	}
	mid := length >> 1

	//从左往右遍历
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag <= end; p, flag = p.Next(), flag+1 {
			if flag >= start {
				val = append(val, p.Value.([]byte))
			}
		}
	} else { // 否则从右往左遍历
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}
		if len(val) > 0 {
			for i, j := 0, len(val)-1; i < j; i, j = i+1, j-1 {
				val[i], val[j] = val[j], val[i]
			}
		}
	}
	return val
}

// LTrim 对一个列表进行修剪(trim)，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除
func (this *List) LTrim(key string, start, end int) bool {
	item := this.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}
	length := item.Len()
	start, end = this.handleIndex(length, start, end)

	// start小于等于左边界，end大于等于右边界，不处理
	if start <= 0 && end >= length-1 {
		return false
	}

	if start > end || start >= length {
		this.record[key] = nil
		this.record[key] = nil
		return true
	}

	startEle, endEle := this.index(key, start), this.index(key, end)
	if end-start+1 < (length >> 1) {
		newList := list.New()
		newValuesMap := make(map[string]struct{})
		for p := startEle; p != endEle.Next(); p.Next() {
			newList.PushBack(p.Value)
			if p.Value != nil {
				newValuesMap[string(p.Value.([]byte))] = existFlag
			}
		}
		item = nil
		this.record[key] = newList
		this.values[key] = newValuesMap
	} else {
		var ele []*list.Element
		for p := item.Front(); p != startEle; p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p.Next() {
			ele = append(ele, p)
		}

		for _, e := range ele {
			item.Remove(e)
			if this.values[key] != nil && e.Value != nil {
				delete(this.values[key], string(e.Value.([]byte)))
			}
		}
		ele = nil
	}
	return true
}

// LLen 返回指定key的列表中的元素个数
func (this *List) LLen(key string) int {
	length := 0
	if this.record[key] != nil {
		length = this.record[key].Len()
	}
	return length
}

func (this *List) LKeyExists(key string) (ok bool) {
	_, ok = this.record[key]
	return
}

func (this *List) LValExists(key string, val []byte) (ok bool) {
	if this.values[key] != nil {
		_, ok = this.values[key][string(val)]
	}
	return
}

func (this *List) find(key string, val []byte) *list.Element {
	item := this.record[key]
	var e *list.Element
	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), val) {
				e = p
				break
			}
		}
	}
	return e
}

func (this *List) index(key string, index int) *list.Element {
	ok, newIndex := this.validIndex(key, index)
	if !ok {
		return nil
	}
	index = newIndex
	item := this.record[key]
	var e *list.Element
	if item != nil && item.Len() > 0 {
		if index <= (item.Len() >> 1) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i++ {
				val = val.Prev()
			}
			e = val
		}
	}
	return e
}

// 校验index是否有效，并返回新的index
func (this *List) validIndex(key string, index int) (bool, int) {
	item := this.record[key]
	if item == nil || item.Len() <= 0 {
		return false, index
	}
	length := item.Len()
	if index < 0 {
		index += length
	}
	return index >= 0 && index < length, index
}

func (this *List) push(front bool, key string, val ...[]byte) int {
	if this.record[key] == nil {
		this.record[key] = list.New()
	}
	if this.values[key] == nil {
		this.values[key] = make(map[string]struct{})
	}

	for _, v := range val {
		if front {
			this.record[key].PushFront(v)
		} else {
			this.record[key].PushBack(v)
		}
		this.values[key][string(v)] = existFlag
	}
	return this.record[key].Len()
}

func (this *List) pop(front bool, key string) []byte {
	item := this.record[key]
	var val []byte
	if item != nil && item.Len() > 0 {
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}

		val = e.Value.([]byte)
		item.Remove(e)
		if this.values[key] != nil {
			delete(this.values[key], string(val))
		}
	}
	return val
}

// 处理start和end的值(负数和边界情况)
func (this *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	return start, end
}
