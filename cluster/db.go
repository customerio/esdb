package cluster

import (
	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/stream"
	"github.com/jrallison/raft"

	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var RETRIEVED_OPEN_STREAM = errors.New("Retrieved a stream that's still open.")

type DB struct {
	dir          string
	closed       []uint64
	current      uint64
	stream       stream.Stream
	raft         raft.Server
	snapshotting sync.RWMutex
}

var slock sync.Mutex

func NewDb(path string) *DB {
	return &DB{dir: path}
}

func (db *DB) Write(commit uint64, body []byte, indexes map[string]string) error {
	if db.current == 0 || commit <= db.current {
		// old commit
		return nil
	}

	println("writing commit:", commit, "current:", db.current)

	db.snapshotting.RLock()
	defer db.snapshotting.RUnlock()

	if db.stream == nil {
		log.Fatal(errors.New("No stream open."))
	}

	_, err := db.stream.Write(body, indexes)
	if err != nil {
		log.Fatal(err)
	}

	return nil
}

func (db *DB) Rotate(commit, term uint64) error {
	s, err := db.retrieveStream(commit, false)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	if s != nil && s.Closed() {
		db.addClosed(commit)
		db.stream = nil
		db.current = 0
	} else {
		if db.stream != nil {
			start := time.Now()

			err = db.stream.Close() // TODO async close?
			if err != nil {
				log.Fatal(err)
			}

			db.addClosed(db.current)

			log.Println("STREAM: Closed", db.current, "in", time.Since(start))

			db.snapshot(commit, term)
		}

		db.setCurrent(commit)
	}

	return nil
}

func (db *DB) Scan(name, value, continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	commit, offset := db.parseContinuation(continuation, true)

	for !stopped && commit > 0 {
		s, err := db.retrieveStream(commit, true)
		if err != nil {
			return "", err
		}

		err = s.ScanIndex(name, value, offset, func(e *stream.Event) bool {
			offset = e.Next(name, value)

			if offset == 0 {
				commit = db.prev(commit)
			}

			stopped = !scanner(e)
			return !stopped
		})

		if err != nil {
			return "", err
		}
	}

	return buildContinuation(commit, offset), nil
}

func (db *DB) Iterate(continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	commit, offset := db.parseContinuation(continuation, false)

	for !stopped && commit > 0 {
		s, err := db.retrieveStream(commit, true)
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
			commit = db.next(commit)
			offset = 0
		}
	}

	return buildContinuation(commit, offset), nil
}

func (db *DB) SaveAt(index, term uint64) ([]byte, error) {
	return db.Save()
}

func (db *DB) Save() ([]byte, error) {
	buf := &bytes.Buffer{}

	binary.WriteInt64(buf, int64(db.current))

	binary.WriteUvarint(buf, len(db.closed))

	for _, commit := range db.closed {
		binary.WriteInt64(buf, int64(commit))
	}

	return buf.Bytes(), nil
}

func (db *DB) Recovery(b []byte) error {
	buf := bytes.NewBuffer(b)

	db.setCurrent(uint64(binary.ReadInt64(buf)))

	count := int(binary.ReadUvarint(buf))

	for i := 0; i < count; i++ {
		db.addClosed(uint64(binary.ReadInt64(buf)))
	}

	return nil
}

func (db *DB) RecoverStreams() error {
	for _, commit := range db.closed {
		if _, err := db.retrieveStream(commit, true); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) path(commit uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("events.%024v.stream", commit))
}

func (db *DB) addClosed(commit uint64) {
	for _, existing := range db.closed {
		if existing == commit {
			return
		}
	}

	db.closed = append(db.closed, commit)
}

func (db *DB) setCurrent(commit uint64) {
	db.current = commit

	err := os.Remove(db.path(commit))
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	s, err := stream.New(db.path(commit))
	if err != nil {
		log.Fatal(err)
	}

	db.stream = s

	log.Println("STREAM: Creating", db.current)
}

func (db *DB) snapshot(index, term uint64) {
	log.Println("RAFT SNAPSHOT: Starting...")

	start := time.Now()

	go (func() {
		if err := db.raft.TakeSnapshotFrom(index, term); err != nil {
			panic(err)
		}

		log.Println("RAFT SNAPSHOT: Complete in", time.Since(start))
	})()
}

func (db *DB) prev(commit uint64) uint64 {
	var result uint64

	for _, c := range db.closed {
		if c < commit && c > result {
			result = c
		}
	}

	return result
}

func (db *DB) next(commit uint64) uint64 {
	var result uint64

	result = math.MaxUint64

	for _, c := range db.closed {
		if c > commit && c < result {
			result = c
		}
	}

	if db.current > commit && db.current < result {
		result = db.current
	}

	if result == math.MaxUint64 {
		result = 0
	}

	return result
}

func (db *DB) retrieveStream(commit uint64, fetchMissing bool) (stream.Stream, error) {
	if db.current == commit && db.stream != nil {
		return db.stream, nil
	}

	var s stream.Stream
	var err error
	var missing bool

	s, err = stream.Open(db.path(commit))

	if err != nil && strings.Contains(err.Error(), "no such file or directory") {
		missing = true
	}

	if s != nil && !s.Closed() {
		println("found open stream:", commit)
		missing = true
	}

	if missing && fetchMissing {
		return RecoverStream(db.raft, db.dir, fmt.Sprintf("events.%024v.stream", commit))
	} else {
		return s, err
	}
}

func (db *DB) parseContinuation(continuation string, reverse bool) (uint64, int64) {
	commit := db.current

	if !reverse && len(db.closed) > 0 {
		commit = db.closed[0]
	}

	var offset int64

	if continuation != "" {
		parts := strings.SplitN(continuation, ":", 2)

		if len(parts) == 2 {
			commit, _ = strconv.ParseUint(parts[0], 10, 64)
			offset, _ = strconv.ParseInt(parts[1], 10, 64)
		}
	}

	return commit, offset
}

func buildContinuation(commit uint64, offset int64) string {
	if commit > 0 {
		return fmt.Sprint(commit, ":", offset)
	} else {
		return ""
	}
}
