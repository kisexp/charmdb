package set

type (
	Set struct {
		record Record
	}
	Record map[string]map[string]bool
)

func New() *Set {
	return &Set{make(Record)}
}

func (this *Set) SAdd(key string, member []byte) int {
	if !this.exist(key) {
		this.record[key] = make(map[string]bool)
	}
	this.record[key][string(member)] = true
	return len(this.record[key])
}

func (this *Set) SPop(key string, count int) [][]byte {
	var val [][]byte
	if !this.exist(key) || count <= 0 {
		return val
	}
	for k := range this.record[key] {
		delete(this.record[key], k)
		val = append(val, []byte(k))
		count--
		if count == 0 {
			break
		}
	}
	return val
}

func (this *Set) SIsMember(key string, member []byte) bool {
	if !this.exist(key) {
		return false
	}
	return this.record[key][string(member)]
}

func (this *Set) SRandMember(key string, count int) [][]byte {
	var val [][]byte
	if !this.exist(key) || count == 0 {
		return val
	}
	if count > 0 {
		for k := range this.record[key] {
			val = append(val, []byte(k))
			if len(val) == count {
				break
			}
		}
	} else {
		count = -count
		randomVal := func() []byte {
			for k := range this.record[key] {
				return []byte(k)
			}
			return nil
		}
		for count > 0 {
			val = append(val, randomVal())
			count--
		}
	}
	return val
}

func (this *Set) SRem(key string, member []byte) bool {
	if !this.exist(key) {
		return false
	}
	if ok := this.record[key][string(member)]; ok {
		delete(this.record[key], string(member))
		return true
	}
	return false
}

func (this *Set) SMove(src, dst string, member []byte) bool {
	if !this.exist(src) || !this.record[src][string(member)] {
		return false
	}
	if !this.exist(dst) {
		this.record[dst] = make(map[string]bool)
	}
	delete(this.record[src], string(member))
	this.record[dst][string(member)] = true
	return true
}

func (this *Set) SCard(key string) int {
	if !this.exist(key) {
		return 0
	}
	return len(this.record[key])
}

func (this *Set) SMembers(key string) (val [][]byte) {
	if !this.exist(key) {
		return
	}
	for k := range this.record[key] {
		val = append(val, []byte(k))
	}
	return
}

func (this *Set) SUnion(keys ...string) (val [][]byte) {
	m := make(map[string]bool)
	for _, k := range keys {
		if this.exist(k) {
			for v := range this.record[k] {
				m[v] = true
			}
		}
	}
	for v := range m {
		val = append(val, []byte(v))
	}
	return
}

func (this *Set) SDiff(keys ...string) (val [][]byte) {
	if !this.exist(keys[0]) {
		return
	}
	for v := range this.record[keys[0]] {
		flag := true
		for i := 1; i < len(keys); i++ {
			if this.SIsMember(keys[i], []byte(v)) {
				flag = false
			}
		}
		if flag {
			val = append(val, []byte(v))
		}
	}
	return
}

// exist key对应的集合是否存在
func (this *Set) exist(key string) bool {
	_, exist := this.record[key]
	return exist
}
