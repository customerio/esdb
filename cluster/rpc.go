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

func (n *NodeRPC) JoinCluster(command raft.DefaultJoinCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.JoinCluster", &command)
}

func (n *NodeRPC) RemoveFromCluster(command raft.DefaultLeaveCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.RemoveFromCluster", &command)
}

func (n *NodeRPC) Event(command *EventCommand, reply *EventCommandResponse) error {
	err := executeEventCommandOnLeader(n.node, "Node.Event", command, reply)
	if err != nil {
		return err
	}

	if err == nil && reply.Rotate && n.node.raft.State() == "leader" {
		err = n.Rotate(NewRotateCommand(command.Timestamp), &NoResponse{})
	}

	return err
}

func (n *NodeRPC) Rotate(command *RotateCommand, reply *NoResponse) error {
	return executeOnLeader(n.node, "Node.Rotate", command)
}

func executeEventCommandOnLeader(n *Node, message string, command raft.Command, reply *EventCommandResponse) (err error) {
	if n.raft.State() == "leader" {
		res, err := n.raft.Do(command)

		if ecr, ok := res.(EventCommandResponse); ok {
			*reply = ecr
		}

		return err
	} else {
		leader := n.raft.Leader()

		if node, ok := n.raft.Peers()[leader]; ok {
			host := strings.Replace(node.ConnectionString, "http://", "", 1)
			err = executeEventCommandOn(host, message, command, reply)
			return
		} else {
			return errors.New("No current leader.")
		}
	}
}

func executeEventCommandOn(host string, message string, command raft.Command, reply *EventCommandResponse) error {
	client, err := rpc.DialHTTP("tcp", host)
	if err != nil {
		return err
	}

	defer client.Close()

	return client.Call(message, command, reply)
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
