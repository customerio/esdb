package cluster

import (
	"encoding/json"
	"net/http"
)

func (n *Node) offsetEventsHandler(w http.ResponseWriter, req *http.Request) {
	req.Body.Close()

	meta := Metadata{
		Archived: n.db.archived,
		Closed:   n.db.closed,
		Current:  n.db.current,
	}

	index := req.FormValue("index")
	value := req.FormValue("value")

	js, _ := json.MarshalIndent(map[string]interface{}{
		"meta":         meta,
		"continuation": n.db.Continuation(index, value),
	}, "", "  ")

	w.Write(js)
	w.Write([]byte("\n"))
}
