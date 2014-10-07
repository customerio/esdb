package cluster

import (
	"github.com/customerio/esdb/stream"

	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
)

type event struct {
	Body    string            `json:"body"`
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
		err = n.Event([]byte(data.Body), data.Indexes)
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
	var count int

	index := req.FormValue("index")
	value := req.FormValue("value")
	limit, _ := strconv.Atoi(req.FormValue("limit"))

	events := make([]string, 0, limit)

	if limit == 0 {
		limit = 20
	}

	if index != "" {
		n.db.stream.ScanIndex(index, value, func(e *stream.Event) bool {
			count += 1
			events = append(events, string(e.Data))
			return count < limit
		})
	} else {
		n.db.stream.Iterate(func(e *stream.Event) bool {
			count += 1
			events = append(events, string(e.Data))
			return count < limit
		})
	}

	return map[string]interface{}{
		"events": events,
	}, nil
}
