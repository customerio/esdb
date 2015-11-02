package cluster

import (
	"log"

	"github.com/customerio/esdb/stream"

	"encoding/json"
	"errors"
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

	log.Println(req.Method, req.URL)

	switch req.Method {
	case "POST":
		res, err = index(n, w, req)
	case "GET":
		res, err = scan(n, w, req)
	default:
		log.Println(req.Method, req.URL, 404)
		w.WriteHeader(404)
	}

	if err != nil {
		log.Println(req.Method, req.URL, 500, err)
		w.WriteHeader(500)
		res["error"] = err.Error()
	}

	req.Body.Close()

	js, _ := json.MarshalIndent(res, "", "  ")
	w.Write(js)
	w.Write([]byte("\n"))
}

func index(n *Node, w http.ResponseWriter, req *http.Request) (map[string]interface{}, error) {
	var data []*event

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return map[string]interface{}{}, errors.New("Error reading request body")
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Println(req.Method, req.URL, 400, "Malformed body:", string(body), err)
		w.WriteHeader(400)
		return map[string]interface{}{}, nil
	}

	bodies := make([][]byte, len(data))
	indexes := make([]map[string]string, len(data))
	events := make([]map[string]interface{}, len(data))

	for i, d := range data {
		bodies[i] = []byte(d.Body)
		indexes[i] = d.Indexes
		events[i] = map[string]interface{}{
			"event":   d.Body,
			"indexes": d.Indexes,
		}
	}

	err = n.Events(bodies, indexes)

	if err == NOT_LEADER_ERROR {
		var uri string
		uri, err = n.LeaderConnectionString()
		w.Header().Set("Cluster-Leader", uri)

		if err != nil {
			return map[string]interface{}{}, err
		} else {
			log.Println(req.Method, req.URL, 400, "Not leader")
			w.WriteHeader(400)
			return map[string]interface{}{}, nil
		}
	}

	if err != nil {
		return map[string]interface{}{}, err
	} else {
		return map[string]interface{}{
			"events": events,
		}, nil
	}
}

func scan(n *Node, w http.ResponseWriter, req *http.Request) (map[string]interface{}, error) {
	var count int
	var err error

	index := req.FormValue("index")
	value := req.FormValue("value")
	after, _ := strconv.ParseInt(req.FormValue("after"), 10, 64)
	continuation := req.FormValue("continuation")
	limit, _ := strconv.Atoi(req.FormValue("limit"))

	events := make([]string, 0, limit)

	if limit == 0 {
		limit = 20
	}

	if index != "" {
		continuation, err = n.db.Scan(index, value, uint64(after), continuation, func(e *stream.Event) bool {
			count += 1
			events = append(events, string(e.Data))
			return count < limit
		})
	} else {
		continuation, err = n.db.Iterate(uint64(after), continuation, func(e *stream.Event) bool {
			count += 1
			events = append(events, string(e.Data))
			return count < limit
		})
	}

	return map[string]interface{}{
		"events":       events,
		"continuation": continuation,
		"most_recent":  n.db.MostRecent,
	}, err
}
