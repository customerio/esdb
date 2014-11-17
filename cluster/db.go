package cluster

import (
	"github.com/customerio/esdb"
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

const (
	DEFAULT_ROTATE_THRESHOLD = 536870912
	DEFAULT_SNAPSHOT_BUFFER  = 500
)

var RETRIEVED_OPEN_STREAM = errors.New("Retrieved a stream that's still open.")

type DB struct {
	dir             string
	archived        []uint64
	closed          []uint64
	current         uint64
	MostRecent      int64
	RotateThreshold int64
	SnapshotBuffer  uint64
	wtimer          Timer
	rtimer          Timer
	stream          stream.Stream
	streams         map[uint64]stream.Stream
	archives        map[uint64]*esdb.Db
	mockoffset      int64
	raft            raft.Server
}

var streamlock sync.Mutex

func NewDb(path string) *DB {
	db := &DB{
		dir:             path,
		wtimer:          NilTimer{},
		rtimer:          NilTimer{},
		streams:         make(map[uint64]stream.Stream),
		archives:        make(map[uint64]*esdb.Db),
		RotateThreshold: DEFAULT_ROTATE_THRESHOLD,
		SnapshotBuffer:  DEFAULT_SNAPSHOT_BUFFER,
	}

	db.Rotate(1, 0)

	return db

}

func (db *DB) Offset() int64 {
	if db.stream == nil {
		return db.mockoffset
	} else {
		return db.stream.Offset()
	}
}

func (db *DB) Write(commit uint64, body []byte, indexes map[string]string, timestamp int64) error {
	if commit <= db.current {
		// old commit
		return nil
	}

	if db.stream == nil {
		bytes, _ := stream.Serialize(body, indexes, map[string]int64{})
		db.mockoffset += int64(len(bytes))
		return nil

	}

	db.wtimer.Time(func() {
		_, err := db.stream.Write(body, indexes)
		if err != nil {
			log.Fatal(err)
		}
	})

	if timestamp > db.MostRecent {
		db.MostRecent = timestamp
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
		db.mockoffset = 10
		db.current = commit
	} else {
		if db.stream != nil {
			start := time.Now()

			db.rtimer.Time(func() {
				err = db.stream.Close() // TODO async close?
				if err != nil {
					log.Fatal(err)
				}

				db.addClosed(db.current)

				log.Println("STREAM: Closed", db.current, "in", time.Since(start))
			})

			db.snapshot(commit, term)
		}

		db.setCurrent(commit)
	}

	return nil
}

func (db *DB) Scan(name, value, continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	commit, offset, archive := db.parseContinuation(continuation, true)

	for !archive && !stopped && commit > 0 {
		s, err := db.retrieveStream(commit, true)
		if err != nil {
			return "", err
		}

		err = s.ScanIndex(name, value, offset, func(e *stream.Event) bool {
			offset = e.Next(name, value)
			stopped = !scanner(e)
			return !stopped
		})

		if err != nil {
			return "", err
		}

		if !stopped {
			commit = db.prev(commit)
			offset = 0
		}
	}

	if stopped {
		return buildContinuation(commit, offset, false), nil
	}

	commit = db.prevArchive(math.MaxUint64)

	for !stopped && commit > 0 {
		d, err := db.retrieveArchive(commit)
		if err != nil {
			return "", err
		}

		if space := d.Find([]byte("a")); space != nil {
			events := make([]*stream.Event, 0)

			space.Scan("", func(e *esdb.Event) bool {
				events = append(events, stream.NewEvent(e.Data, map[string]int64{}))
				return true
			})

			for i := len(events); i > 0; i-- {
				if !scanner(events[i-1]) {
					stopped = true
				}
			}
		}

		commit = db.prevArchive(commit)
	}

	return buildContinuation(commit, 0, true), nil
}

func (db *DB) Iterate(continuation string, scanner stream.Scanner) (string, error) {
	var stopped bool

	commit, offset, archive := db.parseContinuation(continuation, false)

	if archive {
		for commit > 0 {
			d, err := db.retrieveArchive(commit)
			if err != nil {
				return "", err
			}

			if space := d.Find([]byte("a")); space != nil {
				space.Scan("", func(e *esdb.Event) bool {
					if !scanner(stream.NewEvent(e.Data, map[string]int64{})) {
						stopped = true
					}

					return true
				})
			}

			if stopped {
				return buildContinuation(commit, 0, true), nil
			}

			commit = db.nextArchive(commit)
		}

		commit = db.next(0)
		offset = 0
	}

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

	return buildContinuation(commit, offset, false), nil
}

func (db *DB) Archive(start, stop uint64) {
	newclosed := make([]uint64, 0, len(db.closed))

	for _, commit := range db.closed {
		if commit < start || commit > stop {
			newclosed = append(newclosed, commit)
		}
	}

	db.archived = append(db.archived, start)
	db.closed = newclosed
}

func (db *DB) SaveAt(index, term uint64) ([]byte, error) {
	return db.Save()
}

func (db *DB) Save() ([]byte, error) {
	buf := &bytes.Buffer{}

	binary.WriteInt64(buf, int64(db.current))
	binary.WriteInt64(buf, db.MostRecent)

	binary.WriteUvarint(buf, len(db.closed))

	for _, commit := range db.closed {
		binary.WriteInt64(buf, int64(commit))
	}

	return buf.Bytes(), nil
}

func (db *DB) Recovery(b []byte) error {
	buf := bytes.NewBuffer(b)

	db.setCurrent(uint64(binary.ReadInt64(buf)))
	db.MostRecent = binary.ReadInt64(buf)

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

func (db *DB) archivepath(commit uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("events.%024v.esdb", commit))
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
	db.mockoffset = 10

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
		if index > db.SnapshotBuffer {
			index = index - db.SnapshotBuffer
		} else {
			index = 0
		}

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

func (db *DB) prevArchive(commit uint64) uint64 {
	var result uint64

	for _, c := range db.archived {
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

func (db *DB) nextArchive(commit uint64) uint64 {
	var result uint64

	result = math.MaxUint64

	for _, c := range db.archived {
		if c > commit && c < result {
			result = c
		}
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

	if db.streams[commit] == nil {
		streamlock.Lock()
		defer streamlock.Unlock()

		if db.streams[commit] == nil {
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
				s = nil
			}

			if missing && fetchMissing {
				s, err = RecoverStream(db.raft, db.dir, fmt.Sprintf("events.%024v.stream", commit))
			}

			if err != nil {
				return nil, err
			}

			db.streams[commit] = s
		}
	}

	return db.streams[commit], nil
}

func (db *DB) retrieveArchive(commit uint64) (*esdb.Db, error) {
	if db.archives[commit] == nil {
		streamlock.Lock()
		defer streamlock.Unlock()

		if db.archives[commit] == nil {
			var d *esdb.Db
			var err error

			d, err = esdb.Open(db.archivepath(commit))
			if err != nil {
				return nil, err
			}

			db.archives[commit] = d
		}
	}

	return db.archives[commit], nil
}

func (db *DB) parseContinuation(continuation string, reverse bool) (uint64, int64, bool) {
	commit := db.current
	archive := false

	if !reverse {
		if len(db.archived) > 0 {
			commit = db.archived[0]
			archive = true
		} else if len(db.closed) > 0 {
			commit = db.closed[0]
		}
	}

	var offset int64

	if continuation != "" {
		parts := strings.SplitN(continuation, ":", 2)

		if len(parts) == 2 {
			commit, _ = strconv.ParseUint(parts[0], 10, 64)
			offset, _ = strconv.ParseInt(parts[1], 10, 64)
			archive = false
		} else if len(parts) == 1 {
			commit, _ = strconv.ParseUint(parts[0], 10, 64)
			archive = true
		}
	}

	return commit, offset, archive
}

func buildContinuation(commit uint64, offset int64, archive bool) string {
	if archive && commit > 0 {
		return fmt.Sprint(commit)
	} else if commit > 0 {
		return fmt.Sprint(commit, ":", offset)
	} else {
		return ""
	}
}
