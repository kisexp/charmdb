package zset

import (
	"math"
	"math/rand"
)

const (
	maxLevel    = 32
	probability = 0.25
)

type (
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}

	sklLevel struct {
		forward *sklNode
		span    uint64
	}

	sklNode struct {
		member   string
		score    float64
		backward *sklNode
		level    []*sklLevel
	}

	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int64
		level  int16
	}
)

func New() *SortedSet {
	return &SortedSet{
		make(map[string]*SortedSetNode),
	}
}

// ZAdd 将 member 元素及其 score 值加入到有序集 key 当中
func (this *SortedSet) ZAdd(key string, score float64, member string) {
	if !this.exist(key) {
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		this.record[key] = node
	}

	item := this.record[key]
	v, exist := item.dict[member]
	var node *sklNode
	if exist {
		if score != v.score {
			item.skl.sklDelete(v.score, member)
			node = item.skl.sklInsert(score, member)
		}
	} else {
		node = item.skl.sklInsert(score, member)
	}

	if node != nil {
		item.dict[member] = node
	}
}

// ZScore 返回集合key中对应member的score值，如果不存在则返回负无穷
func (this *SortedSet) ZScore(key string, member string) float64 {
	if this.exist(key) {
		return math.MinInt64
	}

	node, exist := this.record[key].dict[member]
	if !exist {
		return math.MinInt64
	}
	return node.score
}

// ZCard 返回指定集合key中的元素个数
func (this *SortedSet) ZCard(key string) int {
	if !this.exist(key) {
		return 0
	}
	return len(this.record[key].dict)
}

// ZRank 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列
// 排名以 0 为底，也就是说， score 值最小的成员排名为 0
func (this *SortedSet) ZRank(key, member string) int64 {
	if !this.exist(key) {
		return -1
	}
	v, exits := this.record[key].dict[member]
	if !exits {
		return -1
	}

	rank := this.record[key].skl.skGetRank(v.score, member)
	rank--

	return rank
}

// ZRevRank 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序
// 排名以 0 为底，也就是说， score 值最大的成员排名为 0
func (this *SortedSet) ZRevRank(key, member string) int64 {
	if !this.exist(key) {
		return -1
	}
	v, exits := this.record[key].dict[member]
	if !exits {
		return -1
	}

	rank := this.record[key].skl.skGetRank(v.score, member)
	return this.record[key].skl.length - rank
}

// ZIncrBy 为有序集 key 的成员 member 的 score 值加上增量 increment
// 当 key 不存在，或 member 不是 key 的成员时，ZIncrBy 等同于 ZAdd
func (this *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if this.exist(key) {
		node, exist := this.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}
	this.ZAdd(key, increment, member)
	return increment
}

// ZRange 返回有序集 key 中，指定区间内的成员，其中成员的位置按 score 值递增(从小到大)来排序
// 具有相同 score 值的成员按字典序(lexicographical order )来排列
func (this *SortedSet) ZRange(key string, start, stop int) []interface{} {
	if !this.exist(key) {
		return nil
	}
	return this.findRange(key, int64(start), int64(stop), false, false)
}

// ZRangeWithScores 返回有序集 key 中，指定区间内的成员以及 score 值,其中成员的位置按 score 值递增(从小到大)来排序
// 具有相同 score 值的成员按字典序(lexicographical order )来排列
func (this *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{} {
	if !this.exist(key) {
		return nil
	}
	return this.findRange(key, int64(start), int64(stop), false, true)
}

// ZRevRange 返回有序集 key 中，指定区间内的成员，其中成员的位置按 score 值递减(从大到小)来排列
// 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order)排列
func (this *SortedSet) ZRevRange(key string, start, stop int) []interface{} {
	if !this.exist(key) {
		return nil
	}
	return this.findRange(key, int64(start), int64(stop), true, false)
}

// ZRem 移除有序集 key 中的 member 成员，不存在则将被忽略
func (this *SortedSet) ZRem(key, member string) bool {
	if !this.exist(key) {
		return false
	}
	v, exist := this.record[key].dict[member]
	if exist {
		this.record[key].skl.sklDelete(v.score, member)
		delete(this.record[key].dict, member)
		return true
	}
	return false
}

// ZGetByRank 根据排名获取member及分值信息，从小到大排列遍历，即分值最低排名为0，依次类推
func (this *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !this.exist(key) {
		return
	}
	member, score := this.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

// ZRevGetByRank 根据排名获取member及分值信息，从大到小排列遍历，即分值最高排名为0，依次类推
func (this *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !this.exist(key) {
		return
	}
	member, score := this.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}

// ZScoreRange 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员
// 有序集成员按 score 值递增(从小到大)次序排列
func (this *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !this.exist(key) || min > max {
		return
	}
	item := this.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward
	for p != nil {
		if p.score > max {
			break
		}
		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}
	return
}

// ZRevScoreRange 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员
// 有序集成员按 score 值递减(从大到小)的次序排列
func (this *SortedSet) ZRevScoreRange(key string, max, min float64) (val []interface{}) {
	if !this.exist(key) || max < min {
		return
	}
	item := this.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}
	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}

	for p != nil {
		if p.score < min {
			break
		}

		val = append(val, p.member, p.score)
		p = p.backward
	}
	return
}

func (this *SortedSet) ZKeyExists(key string) bool {
	return this.exist(key)
}

func (this *SortedSet) ZClear(key string) {
	if this.ZKeyExists(key) {
		delete(this.record, key)
	}
}

func (this *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {
	skl := this.record[key].skl
	if rank < 0 || rank > skl.length {
		return "", math.MinInt64
	}
	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}
	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}
	node := this.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}
	return node.member, node.score
}

// ZRevRange 返回有序集 key 中，指定区间内的成员以及 score 值，其中成员的位置按 score 值递减(从大到小)来排列
// 具有相同 score 值的成员按字典序的逆序(reverse lexicographical order)排列
func (this *SortedSet) ZRevRangeWithScores(key string, start, stop int) []interface{} {
	if !this.exist(key) {
		return nil
	}
	return this.findRange(key, int64(start), int64(stop), true, true)
}

func (this *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	skl := this.record[key].skl
	length := skl.length

	if start <= 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += length
	}

	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1
	var node *sklNode
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	if span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	var traversed uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && (traversed+p.level[i].span) <= rank {
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank {
			return p
		}
	}
	return nil
}

func (skl *skipList) skGetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {

			rank += p.level[i].span
			p = p.level[i].forward
		}

		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	updates := make([]*sklNode, maxLevel)
	rank := make([]uint64, maxLevel)
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if p.level[i] != nil {
			for p.level[i].forward != nil &&
				(p.level[i].forward.score < score ||
					(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
				rank[i] += p.level[i].span
				p = p.level[i].forward
			}
		}
		updates[i] = p
	}

	level := randomLevel()
	if level > skl.level {
		for i := skl.level; i < level; i++ {
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	p = sklNewNode(level, score, member)
	for i := int16(0); i < level; i++ {
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == skl.head {
		p.backward = nil
	} else {
		p.backward = updates[0]
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p
	} else {
		skl.tail = p
	}

	skl.length++
	return p

}

func randomLevel() int16 {
	var level int16 = 1
	for float32(rand.Int31()&0xFFFF) < (probability * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}
	return maxLevel
}

func (skl *skipList) sklDelete(score float64, member string) {
	update := make([]*sklNode, maxLevel)
	p := skl.head
	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		update[i] = p
	}

	p = p.level[0].forward
	if p != nil && score == p.score && p.member == member {
		skl.sklDeleteNode(p, update)
		return
	}
}

func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		if updates[i].level[i].forward == p {
			updates[i].level[i].span += p.level[i].span - 1
			updates[i].level[i].forward = p.level[i].forward
		} else {
			updates[i].level[i].span--
		}
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p.backward
	} else {
		skl.tail = p.backward
	}

	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}
	skl.length--
}

func newSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""),
	}
}

func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(sklLevel)
	}
	return node
}

func (this *SortedSet) exist(key string) bool {
	_, exist := this.record[key]
	return exist
}
