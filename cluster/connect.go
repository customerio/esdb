package cluster

import (
	"github.com/goraft/raft"

	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
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

	return err
}

func initRaft(n *Node) (raft.Server, error) {
	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)

	if err := os.MkdirAll(n.path+"/db", 0744); err != nil {
		log.Fatalf("Unable to create db directory: %v", err)
	}

	s, err := raft.NewServer(n.name, n.path, transporter, nil, nil, "")
	if err != nil {
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

	client, err := rpc.DialHTTP("tcp", existing)
	if err != nil {
		return err
	}

	defer client.Close()

	command := raft.DefaultJoinCommand{
		Name:             n.raft.Name(),
		ConnectionString: fmt.Sprintf("http://%s:%d", n.host, n.port),
	}

	return client.Call("Node.Join", command, &NoResponse{})
}

func createCluster(n *Node) error {
	log.Println("Initializing new cluster")

	_, err := n.raft.Do(&raft.DefaultJoinCommand{
		Name:             n.raft.Name(),
		ConnectionString: fmt.Sprintf("http://%s:%d", n.host, n.port),
	})

	return err
}
