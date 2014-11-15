package cluster

import (
	"encoding/json"
	"net/http"
)

func (n *Node) closedEventsHandler(w http.ResponseWriter, req *http.Request) {
	req.Body.Close()

	res := map[string]interface{}{"closed": n.db.closed}

	js, _ := json.MarshalIndent(res, "", "  ")
	w.Write(js)
	w.Write([]byte("\n"))
}
