package main

import (
	"charmdb"
	"charmdb/cmd"
	"flag"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var config = flag.String("config", "", "the config file for charmdb")
var dirPath = flag.String("dir_path", "", "the dir path for to database")

func main() {
	flag.Parse()
	var cfg charmdb.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
		cfg = charmdb.DefaultConfig()
	} else {
		c, err := newConfigFromFile(*config)
		if err != nil {
			log.Printf("load config err %+v\n", err)
			return
		}
		cfg = *c
	}
	if *dirPath == "" {
		log.Println("no dir path set, using the os tmp dir.")
	} else {
		cfg.DirPath = *dirPath
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	server, err := cmd.NewServer(cfg)
	if err != nil {
		log.Printf("create charmdb server err: %+v\n", err)
		return
	}
	go server.Listen(cfg.Addr)
	<-sig
	server.Stop()
	log.Println("charmdb is ready to exit bye...")
}

func newConfigFromFile(config string) (*charmdb.Config, error) {
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}
	var cfg = new(charmdb.Config)
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
