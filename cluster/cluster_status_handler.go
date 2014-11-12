package cluster

import (
	"github.com/jrallison/raft"

	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

func (n *Node) clusterStatusHandler(w http.ResponseWriter, req *http.Request) {
	body := make(map[string]interface{})

	if n.raft.Running() {
		reachable := 1 // local node

		statuses := make(map[string]interface{})

		for name, peer := range n.raft.Peers() {
			client, call, err := ping(peer)

			if err == nil {
				defer client.Close()

				reachable += 1

				select {
				case <-call.Done:
					if call.Error == nil {
						statuses[name] = call.Reply
					} else {
						err = call.Error
					}
				case <-time.After(100 * time.Millisecond):
					err = errors.New("timeout")
				}
			}

			if err != nil {
				statuses[name] = "error: " + err.Error()
			}
		}

		statuses[n.raft.Name()] = n.State()

		body["_self"] = n.raft.Name()

		status := "available"

		if reachable < n.raft.QuorumSize() {
			status = "unavailable"
		}

		body["cluster"] = map[string]interface{}{
			"term":   n.raft.Term(),
			"status": fmt.Sprint(status, " (", reachable, "/", n.raft.MemberCount(), " nodes reachable)"),
			"nodes":  statuses,
		}
	} else {
		body["_self"] = "not connected"
	}

	w.Header().Set("Cluster-Nodes", strings.Join(n.ClusterConnectionStrings(), ","))

	js, _ := json.MarshalIndent(body, "", "  ")
	w.Write(js)
}

func ping(peer *raft.Peer) (*rpc.Client, *rpc.Call, error) {
	host := strings.Replace(peer.ConnectionString, "http://", "", 1)

	client, err := rpc.DialHTTP("tcp", host)
	if err != nil {
		return nil, nil, err
	}

	state := new(NodeState)

	call := client.Go("Node.State", NoArgs{}, state, nil)

	return client, call, nil
}
