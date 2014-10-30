package cluster

import (
	"github.com/customerio/esdb/stream"

	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (n *Node) recoverHandler(w http.ResponseWriter, req *http.Request) {
	file := strings.Replace(req.URL.Path, "/stream/", "", 1)

	path := filepath.Join(n.db.dir, file)

	s, err := stream.Open(path)
	if err != nil {
		w.WriteHeader(500)
	} else if !s.Closed() {
		w.WriteHeader(404)
	} else {
		if f, err := os.Open(path); err != nil {
			w.WriteHeader(500)
		} else {
			io.Copy(w, f)
			f.Close()
		}
	}
}
