package cluster

import (
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
	data := &event{}

	body, err := ioutil.ReadAll(req.Body)
	if err == nil {
		err = json.Unmarshal(body, data)
	}

	if err == nil {
		b, _ := json.Marshal(data.Body)
		err = n.Event(b, data.Indexes)
	}

	if err != nil {
		res["error"] = err.Error()
		w.WriteHeader(400)
	} else {
		res["event"] = data.Body
		res["indexes"] = data.Indexes
	}

	js, _ := json.MarshalIndent(res, "", "  ")
	w.Write(js)
}
