package cmd

import (
	"charmdb"
	"fmt"
	"github.com/tidwall/redcon"
	"log"
	"strings"
	"sync"
)

type ExecCmdFunc func(*charmdb.CharmDB, []string) (interface{}, error)

var ExecCmd = make(map[string]ExecCmdFunc)

func addExecCommand(cmd string, cmdFunc ExecCmdFunc) {
	ExecCmd[strings.ToLower(cmd)] = cmdFunc
}

type Server struct {
	server *redcon.Server
	db     *charmdb.CharmDB
	closed bool
	mu     sync.Mutex
}

func NewServer(config charmdb.Config) (*Server, error) {
	db, err := charmdb.Open(config)
	if err != nil {
		return nil, err
	}
	return &Server{db: db}, nil
}

func (this *Server) Listen(addr string) {
	srv := redcon.NewServerNetwork("tcp", addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			this.handleCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {

		},
	)
	this.server = srv
	log.Println("charmdb is running, ready to accept connections.")
	if err := srv.ListenAndServe(); err != nil {
		log.Printf("listen and serve ocuurs error: %+v", err)
	}
}

func (this *Server) Stop() {
	if this.closed {
		return
	}
	this.mu.Lock()
	this.closed = true
	if err := this.server.Close(); err != nil {
		log.Printf("close redcon err: %+v\n", err)
	}
	if err := this.db.Close(); err != nil {
		log.Printf("close charmdb err: %+v\n", err)
	}
	this.mu.Unlock()
}

func (this *Server) handleCmd(conn redcon.Conn, cmd redcon.Command) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic when handle the cmd: %+v", r)
		}
	}()
	command := strings.ToLower(string(cmd.Args[0]))
	exec, exist := ExecCmd[command]
	if !exist {
		conn.WriteError(fmt.Sprintf("err unknown command '%s'", command))
		return
	}
	args := make([]string, 0, len(cmd.Args)-1)
	for i, bytes := range cmd.Args {
		if i == 0 {
			continue
		}
		args = append(args, string(bytes))
	}

	reply, err := exec(this.db, args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteAny(reply)
}
