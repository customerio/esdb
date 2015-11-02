package cluster

import (
	"github.com/jrallison/raft"

	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var NOT_LEADER_ERROR = errors.New("Not current leader")
var NO_LEADER_ERROR = errors.New("No current leader")

type Node struct {
	name        string
	host        string
	port        int
	path        string
	db          *DB
	raft        raft.Server
	Rest        *RestServer
	WriteTimer  Timer
	RotateTimer Timer
}

type NodeState struct {
	Name   string `json:"name"`
	State  string `json:"state"`
	Commit uint64 `json:"commit"`
	Path   string `json:"path"`
	Uri    string `json:"uri"`
}

type Metadata struct {
	Peers      []string `json:"peers"`
	Closed     []uint64 `json:"closed"`
	Current    uint64   `json:"current"`
	MostRecent int64    `json:"recent"`
}

func NewNode(path, host string, port int) (n *Node) {
	if err := os.MkdirAll(filepath.Join(path, "stream"), 0744); err != nil {
		log.Fatalf("Unable to create stream directory: %v", err)
	}

	n = &Node{
		host: host,
		port: port,
		path: path,
		db:   NewDb(filepath.Join(path, "stream")),
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

	n.Rest = NewRestServer(n)

	return n.Rest.Start()
}

func (n *Node) Stop() {
	if n.Rest != nil {
		n.Rest.Stop()
	}

	if n.raft != nil {
		n.raft.Stop()
	}

	http.DefaultServeMux = http.NewServeMux()
}

func (n *Node) SetWriteTimer(t Timer) {
	n.db.wtimer = t
}

func (n *Node) SetRotateTimer(t Timer) {
	n.db.rtimer = t
}

func (n *Node) SetRotateThreshold(size int64) {
	n.db.RotateThreshold = size
}

func (n *Node) SetSnapshotBuffer(count uint64) {
	n.db.SnapshotBuffer = count
}

func (n *Node) Event(body []byte, indexes map[string]string) (err error) {
	if n.raft == nil {
		return errors.New("Raft not yet initialized")
	}

	if n.raft.State() == "leader" {
		_, err = n.raft.Do(NewEventCommand(body, indexes, time.Now().UnixNano()))
	} else {
		err = NOT_LEADER_ERROR
	}

	return
}

func (n *Node) Events(bodies [][]byte, indexes []map[string]string) (err error) {
	if n.raft == nil {
		return errors.New("Raft not yet initialized")
	}

	if n.raft.State() == "leader" {
		_, err = n.raft.Do(NewEventsCommand(bodies, indexes, time.Now().UnixNano()))
	} else {
		err = NOT_LEADER_ERROR
	}

	return
}

func (n *Node) Compress(start, stop uint64) (err error) {
	if n.raft == nil {
		return errors.New("Raft not yet initialized")
	}

	if n.raft.State() == "leader" {
		_, err = n.raft.Do(NewCompressCommand(start, stop))
	} else {
		err = NOT_LEADER_ERROR
	}

	return
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

func (n *Node) Metadata() Metadata {
	return Metadata{
		Peers:      n.db.peerConnectionStrings(),
		Closed:     n.db.closed,
		Current:    n.db.current,
		MostRecent: n.db.MostRecent,
	}
}

func (n *Node) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	http.HandleFunc(pattern, handler)
}

func (n *Node) LeaderConnectionString() (string, error) {
	leader := n.raft.Leader()

	if node, ok := n.raft.Peers()[leader]; ok {
		return node.ConnectionString, nil
	} else {
		return "", NO_LEADER_ERROR
	}
}

func (n *Node) ClusterConnectionStrings() []string {
	return append(n.db.peerConnectionStrings(), fmt.Sprintf("http://%s:%d", n.host, n.port))
}
