package cluster

import (
	"github.com/customerio/esdb/stream"
	"github.com/jrallison/raft"

	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func RecoverStream(r raft.Server, dir, file string) (stream.Stream, error) {
	for _, node := range r.Peers() {
		if s, err := readStream(node.ConnectionString, dir, file); err == nil {
			return s, err
		} else {
			log.Println("RECOVER STREAM: Error", err)
		}
	}

	return nil, errors.New("couldn't recover stream " + file + " from any peer.")
}

func readStream(host, dir, file string) (stream.Stream, error) {
	log.Println("RECOVER STREAM: Recovering file", file, "from", host)

	resp, err := http.Get(host + "/stream/" + file)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprint("Non successfully response from host: ", resp.StatusCode))
	}

	path := filepath.Join(dir, file)

	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return nil, err
	}

	return stream.Open(path)
}
