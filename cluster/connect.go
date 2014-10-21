package cluster

import (
	"github.com/jrallison/raft"

	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func Connect(n *Node, existing string) error {
	r, err := initRaft(n)
	if err != nil {
		return err
	}

	n.raft = r

	if existing != "" {
		err = joinCluster(n, existing)
	} else if n.raft.IsLogEmpty() {
		err = createCluster(n)
	} else {
		log.Println("Recovered from log")
	}

	if err == nil {
		err = n.db.RecoverStreams()
	}

	return err
}

func initRaft(n *Node) (raft.Server, error) {
	raft.RegisterCommand(&EventCommand{})

	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)

	if err := os.MkdirAll(filepath.Join(n.path, "stream"), 0744); err != nil {
		log.Fatalf("Unable to create stream directory: %v", err)
	}

	n.db = NewDb(filepath.Join(n.path, "stream"))

	s, err := raft.NewServer(n.name, n.path, transporter, n.db, n.db, fmt.Sprint("http://", n.host, ":", n.port))
	if err != nil {
		return nil, err
	}

	n.db.raft = s

	if err := os.MkdirAll(filepath.Join(n.path, "snapshot"), 0744); err != nil {
		log.Fatalf("Unable to create stream directory: %v", err)
	}

	if err = s.LoadSnapshot(); err != nil {
		return nil, err
	}

	transporter.Install(s, n)

	return s, s.Start()
}

func joinCluster(n *Node, existing string) error {
	log.Println("Attempting to join cluster:", existing)

	if !n.raft.IsLogEmpty() {
		return errors.New("Cannot join with an existing log")
	}

	return executeOn(existing, "Node.JoinCluster", &raft.DefaultJoinCommand{
		Name:             n.raft.Name(),
		ConnectionString: fmt.Sprintf("http://%s:%d", n.host, n.port),
	})
}

func createCluster(n *Node) error {
	log.Println("Initializing new cluster")

	_, err := n.raft.Do(&raft.DefaultJoinCommand{
		Name:             n.raft.Name(),
		ConnectionString: fmt.Sprintf("http://%s:%d", n.host, n.port),
	})

	return err
}
