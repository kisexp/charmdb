package hash

type (
	Hash struct {
		record Record
	}
	Record map[string]map[string][]byte
)

func New() *Hash {
	return &Hash{make(Record)}
}

// HSet 将哈希表 hash 中域 field 的值设置为 value
// 如果给定的哈希表并不存在， 那么一个新的哈希表将被创建并执行 HSet 操作
// 如果域 field 已经存在于哈希表中， 那么它的旧值将被新值 value 覆盖
// 返回新增的field的数量,如果覆盖不算新增
func (this *Hash) HSet(key string, field string, value []byte) int {
	if !this.exist(key) {
		this.record[key] = make(map[string][]byte)
	}
	if this.record[key][field] != nil {
		this.record[key][field] = value
		return 0
	} else {
		this.record[key][field] = value
		return 1
	}
}

func (this *Hash) exist(key string) bool {
	_, exist := this.record[key]
	return exist
}

// HSetNx 当且仅当域 field 尚未存在于哈希表的情况下， 将它的值设置为 value
// 如果给定域已经存在于哈希表当中， 那么命令将放弃执行设置操作
func (this *Hash) HSetNx(key string, field string, value []byte) int {
	if !this.exist(key) {
		this.record[key] = make(map[string][]byte)
	}
	if _, exist := this.record[key][field]; !exist {
		this.record[key][field] = value
		return 1
	}
	return 0
}

// HGet 返回哈希表中给定域的值
func (this *Hash) HGet(key, field string) []byte {
	if !this.exist(key) {
		return nil
	}
	return this.record[key][field]
}

// HGetAll 返回哈希表 key 中，所有的域和值
func (this *Hash) HGetAll(key string) (res [][]byte) {
	if !this.exist(key) {
		return
	}
	for k, v := range this.record[key] {
		res = append(res, []byte(k), v)
	}

	return
}

// HDel 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略
// 返回成功删除的filed数,不存在的field不算
func (this *Hash) HDel(key, field string) int {
	if !this.exist(key) {
		return 0
	}
	if _, exist := this.record[key][field]; exist {
		delete(this.record[key], field)
		return 1
	}
	return 0
}

func (this *Hash) HKeyExists(key string) bool  {
	return this.exist(key)
}

// HExists 检查给定域 field 是否存在于key对应的哈希表中
func (this *Hash) HExists(key, field string) int {
	if !this.exist(key) {
		return 0
	}
	_, exist := this.record[key][field]
	if exist {
		return 1
	} else {
		return 0
	}
}


// HLen 返回哈希表 key 中域的数量
func (this *Hash) HLen(key string) int {
	if !this.exist(key) {
		return 0
	}
	return len(this.record[key])

}

// HKeys 返回哈希表 key 中的所有域
func (this *Hash) HKeys(key string) (val []string) {
	if !this.exist(key) {
		return
	}
	for k := range this.record[key] {
		val = append(val, k)
	}
	return
}

// HVals 返回哈希表 key 中的所有域对应的值
func (this *Hash) HVals(key string) (val [][]byte) {
	if !this.exist(key) {
		return
	}
	for _, v := range this.record[key] {
		val = append(val, v)
	}
	return
}

func (this *Hash) HClear(key string) {
	if !this.exist(key) {
		return
	}
	delete(this.record, key)
}