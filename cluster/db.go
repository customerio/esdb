package cluster

import (
	"github.com/customerio/esdb/stream"

	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
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

func (db *DB) Scan(name, value, continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	ts, offset := db.parseContinuation(continuation, true)

	for !stopped && ts > 0 {
		s, err := stream.Open(db.path(ts))
		if err != nil {
			return "", err
		}

		err = s.ScanIndex(name, value, offset, func(e *stream.Event) bool {
			offset = e.Next(name, value)

			if offset == 0 {
				ts = db.prevTimestamp(ts)
			}

			stopped = !scanner(e)
			return !stopped
		})

		if err != nil {
			return "", err
		}
	}

	return buildContinuation(ts, offset), nil
}

func (db *DB) Iterate(continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	ts, offset := db.parseContinuation(continuation, false)

	for !stopped && ts > 0 {
		s, err := stream.Open(db.path(ts))
		if err != nil {
			return "", err
		}

		offset, err = s.Iterate(offset, func(e *stream.Event) bool {
			stopped = !scanner(e)
			return !stopped
		})

		if err != nil {
			return "", err
		}

		if !stopped {
			ts = db.nextTimestamp(ts)
			offset = 0
		}
	}

	return buildContinuation(ts, offset), nil
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

func (db *DB) path(ts int) string {
	return filepath.Join(db.dir, fmt.Sprint("events.", ts, ".stream"))
}

func (db *DB) prevTimestamp(timestamp int) int {
	var result int

	for _, ts := range db.closed {
		if ts < timestamp && ts > result {
			result = ts
		}
	}

	return result
}

func (db *DB) nextTimestamp(timestamp int) int {
	result := 9999999999

	for _, ts := range db.closed {
		if ts > timestamp && ts < result {
			result = ts
		}
	}

	if db.current > timestamp && db.current < result {
		result = db.current
	}

	if result == 9999999999 {
		result = 0
	}

	return result
}

func (db *DB) parseContinuation(continuation string, reverse bool) (int, int64) {
	timestamp := db.current

	if !reverse && len(db.closed) > 0 {
		timestamp = db.closed[0]
	}

	var offset int64

	if continuation != "" {
		parts := strings.SplitN(continuation, ":", 2)

		if len(parts) == 2 {
			timestamp, _ = strconv.Atoi(parts[0])
			offset, _ = strconv.ParseInt(parts[1], 10, 64)
		}
	}

	return timestamp, offset
}

func buildContinuation(ts int, offset int64) string {
	if ts > 0 {
		return fmt.Sprint(ts, ":", offset)
	} else {
		return ""
	}
}
