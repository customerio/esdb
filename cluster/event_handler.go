package cluster

import (
	"github.com/customerio/esdb/stream"

	"encoding/json"
	"io/ioutil"
	"net/http"
)

type event struct {
	Body    interface{}       `json:"body"`
	Indexes map[string]string `json:"indexes"`
}

func (n *Node) eventHandler(w http.ResponseWriter, req *http.Request) {
	res := make(map[string]interface{})
	var err error

	switch req.Method {
	case "POST":
		res, err = index(n, w, req)
	case "GET":
		res, err = scan(n, w, req)
	default:
		w.WriteHeader(404)
	}

	if err != nil {
		res["error"] = err.Error()
	}

	js, _ := json.MarshalIndent(res, "", "  ")
	w.Write(js)
	w.Write([]byte("\n"))
}

func index(n *Node, w http.ResponseWriter, req *http.Request) (map[string]interface{}, error) {
	data := &event{}

	body, err := ioutil.ReadAll(req.Body)
	if err == nil {
		err = json.Unmarshal(body, data)
	} else {
		w.WriteHeader(400)
	}

	if err == nil {
		b, _ := json.Marshal(data.Body)
		err = n.Event(b, data.Indexes)
	} else {
		w.WriteHeader(500)
	}

	if err != nil {
		return map[string]interface{}{}, err
	} else {
		return map[string]interface{}{
			"event":   data.Body,
			"indexes": data.Indexes,
		}, nil
	}
}

func scan(n *Node, w http.ResponseWriter, req *http.Request) (map[string]interface{}, error) {
	events := make([]string, 0)

	n.db.stream.Iterate(func(e *stream.Event) bool {
		events = append(events, string(e.Data))
		return true
	})

	return map[string]interface{}{
		"events": events,
	}, nil
}
