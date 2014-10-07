package cluster

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
)

func RestServer(n *Node) error {
	rpc.RegisterName("Node", &NodeRPC{n})
	rpc.HandleHTTP()

	n.HandleFunc("/cluster/status", n.clusterStatusHandler)
	n.HandleFunc("/cluster/remove/", n.clusterRemoveHandler)

	n.HandleFunc("/events", n.eventHandler)

	log.Println("Listening at:", fmt.Sprintf("http://%s:%d", n.host, n.port))

	return http.ListenAndServe(fmt.Sprintf("%s:%d", n.host, n.port), nil)
}
