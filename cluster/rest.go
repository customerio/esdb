package cluster

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
)

type RestServer struct {
	listen string
	stop   chan bool
}

func NewRestServer(n *Node) *RestServer {
	rpc.RegisterName("Node", &NodeRPC{n})
	rpc.HandleHTTP()

	n.HandleFunc("/cluster/status", n.clusterStatusHandler)
	n.HandleFunc("/cluster/remove/", n.clusterRemoveHandler)

	n.HandleFunc("/events", n.eventHandler)
	n.HandleFunc("/events/meta", n.metaEventsHandler)
	n.HandleFunc("/events/archive/", n.archiveEventsHandler)

	n.HandleFunc("/stream/", n.recoverHandler)

	return &RestServer{
		fmt.Sprintf("%s:%d", n.host, n.port),
		make(chan bool),
	}
}

func (s *RestServer) Start() error {
	log.Println("Listening at:", "http://"+s.listen)

	return http.ListenAndServe(s.listen, nil)
}

func (s *RestServer) Stop() {
}
