package index

import (
	"bytes"
	"math"
	"math/rand"
	"time"
)

const (
	// 跳表索引最大层数，可根据实际情况进行调整
	maxLevel    int     = 18
	probability float64 = 1 / math.E
)

type handleEle func(e *Element) bool

type (
	Node struct {
		next []*Element
	}
	Element struct {
		Node
		key   []byte
		value interface{}
	}
	SkipList struct {
		Node
		maxLevel       int
		Len            int
		randSource     rand.Source
		probability    float64
		probTable      []float64
		prevNodesCache []*Node
	}
)

func NewSkipList() *SkipList {
	return &SkipList{
		Node:           Node{next: make([]*Element, maxLevel)},
		prevNodesCache: make([]*Node, maxLevel),
		maxLevel:       maxLevel,
		randSource:     rand.New(rand.NewSource(time.Now().UnixNano())),
		probability:    probability,
		probTable:      probabilityTable(probability, maxLevel),
	}
}

func (this *Element) Key() []byte {
	return this.key
}

func (this *Element) Value() interface{} {
	return this.value
}

func (this *Element) SetValue(val interface{}) {
	this.value = val
}

// Next 跳表的第一层索引是原始数据，有序排列，可根据Next方法获取一个串联所有数据的链表
func (this *Element) Next() *Element {
	return this.next[0]
}

// Front 获取跳表头元素，获取到之后，可向后遍历得到所有的数据
func (this *SkipList) Front() *Element {
	return this.next[0]
}

// Put 方法存储一个元素至跳表中，如果key已经存在，则会更新其对应的value
// 因此此跳表的实现暂不支持相同的key
func (this *SkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := this.backNodes(key)
	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, this.randomLevel()),
		},
		key:   key,
		value: value,
	}

	for i := range element.next {
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}
	this.Len++
	return element
}

// Get 方法根据 key 查找对应的 Element 元素
// 未找到则返回nil
func (this *SkipList) Get(key []byte) *Element {
	var prev = &this.Node
	var next *Element

	for i := this.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}
	return nil
}

// Exist 判断跳表是否存在对应的key
func (this *SkipList) Exist(key []byte) bool {
	return this.Get(key) != nil
}

// Remove Remove方法根据key删除跳表中的元素，返回删除后的元素指针
func (this *SkipList) Remove(key []byte) *Element {
	prev := this.backNodes(key)
	if element := prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		for k, v := range element.next {
			prev[k].next[k] = v
		}
		this.Len--
		return element
	}
	return nil
}

// Foreach 遍历跳表中的每个元素
func (this *SkipList) Foreach(fun handleEle) {
	for p := this.Front(); p != nil; p = p.Next() {
		if ok := fun(p); !ok {
			break
		}
	}
}

// FindPrefix 找到第一个和前缀匹配的Element
func (this *SkipList) FindPrefix(prefix []byte) *Element {
	var prev = &this.Node
	var next *Element
	for i := this.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && bytes.Compare(prefix, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next == nil {
		next = this.Front()
	}
	return next
}

// 生成索引随机层数
func (this *SkipList) randomLevel() (level int) {
	r := float64(this.randSource.Int63()) / (1 << 63)
	level = 1
	for level < this.maxLevel && r < this.probTable[level] {
		level++
	}
	return
}

// 找到key对应的前一个节点索引的信息
func (this *SkipList) backNodes(key []byte) []*Node {
	var prev = &this.Node
	var next *Element

	prevs := this.prevNodesCache

	for i := this.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
		prevs[i] = prev
	}
	return prevs
}

func probabilityTable(probability float64, maxLevel int) (table []float64) {
	for i := 1; i <= maxLevel; i++ {
		prob := math.Pow(probability, float64(i-1))
		table = append(table, prob)
	}
	return table
}
