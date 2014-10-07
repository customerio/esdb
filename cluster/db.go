package cluster

import (
	"github.com/customerio/esdb/stream"

	"log"
	"os"
	"path/filepath"
	"strings"
)

type DB struct {
	dir    string
	stream stream.Stream
}

func NewDb(path string) *DB {
	streampath := filepath.Join(path, "events.stream")

	err := os.Remove(streampath)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	s, err := stream.New(streampath)
	if err != nil {
		log.Fatal(err)
	}

	return &DB{
		dir:    path,
		stream: s,
	}
}

func (db *DB) Write(body []byte, indexes map[string]string) (err error) {
	_, err = db.stream.Write(body, indexes)
	return
}
