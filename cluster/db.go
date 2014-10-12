package cluster

import (
	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/stream"
	"github.com/goraft/raft"

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

type DB struct {
	dir          string
	closed       []int64
	current      int64
	stream       stream.Stream
	raft         raft.Server
	snapshotting sync.RWMutex
}

var slock sync.Mutex

func NewDb(path string) *DB {
	return &DB{
		dir: path,
	}
}

func (db *DB) Offset() int64 {
	if db.stream != nil {
		return db.stream.Offset()
	} else {
		return 0
	}
}

func (db *DB) Write(body []byte, indexes map[string]string) error {
	if db.current == 0 {
		// reading through an old stream, ignore
		return nil
	}

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

func (db *DB) Rotate(timestamp int64) error {
	s, err := stream.Open(db.path(timestamp))
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	if s != nil && s.Closed() {
		db.addClosed(timestamp)
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

			db.snapshot()
		}

		db.setCurrent(timestamp)
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

func (db *DB) Save() ([]byte, error) {
	buf := &bytes.Buffer{}

	binary.WriteInt64(buf, db.current)

	binary.WriteUvarint(buf, len(db.closed))

	for _, ts := range db.closed {
		binary.WriteInt64(buf, ts)
	}

	return buf.Bytes(), nil
}

func (db *DB) Recovery(b []byte) error {
	buf := bytes.NewBuffer(b)

	db.setCurrent(binary.ReadInt64(buf))

	count := int(binary.ReadUvarint(buf))

	for i := 0; i < count; i++ {
		db.addClosed(binary.ReadInt64(buf))
	}

	return nil
}

func (db *DB) path(ts int64) string {
	return filepath.Join(db.dir, fmt.Sprint("events.", ts, ".stream"))
}

func (db *DB) addClosed(ts int64) {
	for _, existing := range db.closed {
		if existing == ts {
			return
		}
	}

	db.closed = append(db.closed, ts)
}

func (db *DB) setCurrent(timestamp int64) {
	err := os.Remove(db.path(timestamp))
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	s, err := stream.New(db.path(timestamp))
	if err != nil {
		log.Fatal(err)
	}

	db.current = timestamp
	db.stream = s

	log.Println("STREAM: Creating", db.current)
}

func (db *DB) snapshot() {
	db.snapshotting.Lock()

	log.Println("RAFT SNAPSHOT: Starting...")

	start := time.Now()

	go (func() {
		if err := db.raft.TakeSnapshot(); err != nil {
			panic(err)
		}

		log.Println("RAFT SNAPSHOT: Complete in", time.Since(start))
		db.snapshotting.Unlock()
	})()
}

func (db *DB) prevTimestamp(timestamp int64) int64 {
	var result int64

	for _, ts := range db.closed {
		if ts < timestamp && ts > result {
			result = ts
		}
	}

	return result
}

func (db *DB) nextTimestamp(timestamp int64) int64 {
	var result int64

	result = math.MaxInt64

	for _, ts := range db.closed {
		if ts > timestamp && ts < result {
			result = ts
		}
	}

	if db.current > timestamp && db.current < result {
		result = db.current
	}

	if result == math.MaxInt64 {
		result = 0
	}

	return result
}

func (db *DB) parseContinuation(continuation string, reverse bool) (int64, int64) {
	timestamp := db.current

	if !reverse && len(db.closed) > 0 {
		timestamp = db.closed[0]
	}

	var offset int64

	if continuation != "" {
		parts := strings.SplitN(continuation, ":", 2)

		if len(parts) == 2 {
			timestamp, _ = strconv.ParseInt(parts[0], 10, 64)
			offset, _ = strconv.ParseInt(parts[1], 10, 64)
		}
	}

	return timestamp, offset
}

func buildContinuation(ts int64, offset int64) string {
	if ts > 0 {
		return fmt.Sprint(ts, ":", offset)
	} else {
		return ""
	}
}
