package cluster

import (
	"encoding/json"
	"net/http"
)

type Metadata struct {
	Archived []uint64 `json:"archived"`
	Closed   []uint64 `json:"closed"`
	Current  uint64   `json:"current"`
}

func (n *Node) metaEventsHandler(w http.ResponseWriter, req *http.Request) {
	req.Body.Close()

	res := Metadata{
		Archived: n.db.archived,
		Closed:   n.db.closed,
		Current:  n.db.current,
	}

	js, _ := json.MarshalIndent(res, "", "  ")
	w.Write(js)
	w.Write([]byte("\n"))
}
