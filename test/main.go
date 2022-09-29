package main

import (
	"fmt"
	"github.com/kisexp/charmdb"
	"log"
)

func main() {
	config := charmdb.DefaultConfig()
	db, err := charmdb.Open(config)

	if err != nil {
		log.Fatal(err)
	}
	// 别忘记关闭数据库哦！
	defer db.Close()

	res, err := db.SetNx([]byte("hull"), []byte("123"))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("res -> ", res)

	val, err := db.Get([]byte("hull"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(val))

}
