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

func Log(handler func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.URL)
		handler(w, r)
	}
}

func NewRestServer(n *Node) *RestServer {
	rpc.RegisterName("Node", &NodeRPC{n})
	rpc.HandleHTTP()

	n.HandleFunc("/cluster/status", Log(n.clusterStatusHandler))
	n.HandleFunc("/cluster/remove/", Log(n.clusterRemoveHandler))

	n.HandleFunc("/events", n.eventHandler)
	n.HandleFunc("/events/meta", Log(n.metaEventsHandler))
	n.HandleFunc("/events/offset", Log(n.offsetEventsHandler))
	n.HandleFunc("/events/compress/", Log(n.compressEventsHandler))

	n.HandleFunc("/stream/", Log(n.recoverHandler))

	n.HandleFunc("/", Log(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(404)
	}))

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
