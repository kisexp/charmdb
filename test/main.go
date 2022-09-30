package main

import (
	"fmt"
	"github.com/kisexp/charmdb"
	"github.com/kisexp/charmdb/storage"
	"log"
)

func CharmDBConfig() charmdb.Config {
	return charmdb.Config{
		Addr:                   "127.0.0.1:5200",                     //服务器地址
		DirPath:                "/opt/server/charmdb/charmdb-server", //数据库数据存储目录
		BlockSize:              16 * 1024 * 1024,                     //每个数据块文件的大小：16MB
		RwMethod:               storage.FileIO,                       //数据读写模式
		IdxMode:                charmdb.KeyValueMemMode,              // 数据索引模式
		MaxKeySize:             uint32(128),                          // 默认的key最大值 128字节
		MaxValueSize:           uint32(1 * 1024 * 1024),              // 默认的value最大值 1MB
		Sync:                   false,                                // 每次写数据是否持久化
		ReclaimThreshold:       4,                                    //回收磁盘空间的阈值
		SingleReclaimThreshold: 4 * 1024 * 1024,
	}
}

func main() {
	db, err := charmdb.Open(CharmDBConfig())
	if err != nil {
		log.Fatal(err)
	}
	// 别忘记关闭数据库哦！
	defer db.Close()

	res, err := db.SetNx([]byte("hullop"), []byte("123"))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("res -> ", res)
	err = db.StrRem([]byte("hullop"))
	fmt.Println("err -> ", err)
	if err != nil {
		fmt.Println(err)
		return
	}

	ok_hull := db.StrExists([]byte("hullop"))
	fmt.Println("ok_hullop -> ", ok_hull)
	val, err := db.Get([]byte("hullop"))
	if err != nil {
		fmt.Println(err)
		return
	}

	ttl := db.TTL([]byte("hullop"))
	fmt.Println(string(val))
	fmt.Println("ttl -> ", ttl)

}
