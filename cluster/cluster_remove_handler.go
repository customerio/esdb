package cluster

import (
	"github.com/goraft/raft"

	"encoding/json"
	"net/http"
	"strings"
)

func (n *Node) clusterRemoveHandler(w http.ResponseWriter, req *http.Request) {
	name := strings.Replace(req.URL.Path, "/cluster/remove/", "", 1)

	err := executeOnLeader(n, "Node.Remove", &raft.DefaultLeaveCommand{
		Name: name,
	}, &NoResponse{})

	body := make(map[string]interface{})

	if err != nil {
		body["error"] = err.Error()
		w.WriteHeader(500)
	} else {
		body["status"] = "Node removed."
	}

	js, _ := json.MarshalIndent(body, "", "  ")
	w.Write(js)
}
