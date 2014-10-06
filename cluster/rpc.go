package cluster

import (
	"github.com/goraft/raft"

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

func (n *NodeRPC) Join(command raft.DefaultJoinCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.Join", &command, &NoResponse{})
}

func (n *NodeRPC) Remove(command raft.DefaultLeaveCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.Remove", &command, &NoResponse{})
}

func executeOnLeader(n *Node, message string, command raft.Command, reply interface{}) error {
	if n.raft.State() == "leader" {
		_, err := n.raft.Do(command)
		return err
	} else {
		leader := n.raft.Leader()

		if node, ok := n.raft.Peers()[leader]; ok {
			host := strings.Replace(node.ConnectionString, "http://", "", 1)

			client, err := rpc.DialHTTP("tcp", host)
			if err != nil {
				return err
			}

			defer client.Close()

			return client.Call(message, command, reply)
		} else {
			return errors.New("No current leader.")
		}
	}
}
