package cluster

import (
	"encoding/json"
	"net/http"
)

func (n *Node) metaEventsHandler(w http.ResponseWriter, req *http.Request) {
	req.Body.Close()

	js, _ := json.MarshalIndent(n.Metadata(), "", "  ")
	w.Write(js)
	w.Write([]byte("\n"))
}
