package cluster

import (
	"github.com/goraft/raft"

	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"path/filepath"
	"time"
)

type Node struct {
	name string
	host string
	port int
	path string
	db   *DB
	raft raft.Server
}

type NodeState struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Commit uint64 `json:"commit"`
	Path   string `json:"path"`
	Uri    string `json:"uri"`
}

func NewNode(path, host string, port int) (n *Node) {
	n = &Node{
		host: host,
		port: port,
		path: path,
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		n.name = string(b)
	} else {
		n.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(n.name), 0644); err != nil {
			log.Fatal(err)
		}
	}

	return
}

func (n *Node) Start(join string) (err error) {
	log.Printf("Initializing Raft Server: %s", n.path)

	if err = Connect(n, join); err != nil {
		log.Fatal(err)
	}

	log.Println("Initializing HTTP server")

	return RestServer(n)
}

func (n *Node) Event(body []byte, indexes map[string]string) error {
	rpc := &NodeRPC{n}
	return rpc.Event(NewEventCommand(body, indexes, int(time.Now().Unix())), &EventCommandResponse{})
}

func (n *Node) RemoveFromCluster(name string) error {
	rpc := &NodeRPC{n}
	return rpc.RemoveFromCluster(raft.DefaultLeaveCommand{
		Name: name,
	}, &NoResponse{})
}

func (n *Node) State() NodeState {
	return NodeState{
		n.raft.Name(),
		n.raft.State(),
		n.raft.CommitIndex(),
		n.path,
		fmt.Sprintf("http://%s:%d", n.host, n.port),
	}
}

func (n *Node) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	http.HandleFunc(pattern, handler)
}
