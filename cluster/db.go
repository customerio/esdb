package cluster

import (
	"github.com/customerio/esdb/stream"

	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type DB struct {
	dir     string
	closed  []int
	current int
	stream  stream.Stream
}

func NewDb(path string) *DB {
	return &DB{
		dir: path,
	}
}

func (db *DB) Offset() int64 {
	return db.stream.Offset()
}

func (db *DB) Rotate(timestamp int) error {
	streampath := filepath.Join(db.dir, fmt.Sprint("events.", timestamp, ".stream"))

	s, err := stream.Open(streampath)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	if s != nil && s.Closed() {
		db.closed = append(db.closed, timestamp)
	} else {
		if db.stream != nil {
			err = db.stream.Close() // TODO async close?
			if err != nil {
				log.Fatal(err)
			}

			db.closed = append(db.closed, db.current)
		}

		err = os.Remove(streampath)
		if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
			log.Fatal(err)
		}

		s, err = stream.New(streampath)
		if err != nil {
			log.Fatal(err)
		}

		db.current = timestamp
		db.stream = s
	}

	return nil
}

func (db *DB) Write(body []byte, indexes map[string]string) error {
	if db.stream == nil {
		log.Fatal(errors.New("No stream open."))
	}

	_, err := db.stream.Write(body, indexes)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}
