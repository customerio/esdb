package cluster

import (
	"net/http"
	"strconv"
	"strings"
)

func (n *Node) compressEventsHandler(w http.ResponseWriter, req *http.Request) {
	req.Body.Close()

	if req.Method != "POST" {
		w.WriteHeader(404)
		return
	}

	name := strings.Replace(req.URL.Path, "/events/compress/", "", 1)

	parts := strings.Split(name, "/")

	var err error

	if len(parts) == 2 {
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		stop, _ := strconv.ParseInt(parts[1], 10, 64)

		err = n.Compress(uint64(start), uint64(stop))

		if err == NOT_LEADER_ERROR {
			var uri string
			uri, err = n.LeaderConnectionString()
			w.Header().Set("Cluster-Leader", uri)
			w.WriteHeader(400)
			return
		}
	} else {
		w.WriteHeader(404)
		return
	}

	if err != nil {
		w.WriteHeader(500)
	}

	w.Write([]byte("\n"))
}
