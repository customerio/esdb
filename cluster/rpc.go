package cluster

import (
	"github.com/jrallison/raft"

	"errors"
	"net/rpc"
	"strings"
)

type NodeRPC struct {
	node *Node
}

type NoArgs struct {
}

type NoResponse struct {
}

func (n *NodeRPC) State(args NoArgs, reply *NodeState) error {
	*reply = n.node.State()
	return nil
}

func (n *NodeRPC) JoinCluster(command raft.DefaultJoinCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.JoinCluster", &command)
}

func (n *NodeRPC) RemoveFromCluster(command raft.DefaultLeaveCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.RemoveFromCluster", &command)
}

func executeOnLeader(n *Node, message string, command raft.Command) (err error) {
	if n.raft.State() == "leader" {
		_, err = n.raft.Do(command)
		return
	} else {
		leader := n.raft.Leader()

		if node, ok := n.raft.Peers()[leader]; ok {
			host := strings.Replace(node.ConnectionString, "http://", "", 1)
			err = executeOn(host, message, command)
			return
		} else {
			return errors.New("No current leader.")
		}
	}
}

func executeOn(host string, message string, command raft.Command) error {
	client, err := rpc.DialHTTP("tcp", host)
	if err != nil {
		return err
	}

	defer client.Close()

	return client.Call(message, command, &NoResponse{})
}
