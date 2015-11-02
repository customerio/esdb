package cluster

import (
	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/stream"
	"github.com/jrallison/raft"

	"bytes"
	"errors"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	DEFAULT_ROTATE_THRESHOLD = 536870912
	DEFAULT_SNAPSHOT_BUFFER  = 500
)

var RETRIEVED_OPEN_STREAM = errors.New("Retrieved a stream that's still open.")

type OffsetSlice []uint64

func (p OffsetSlice) Len() int           { return len(p) }
func (p OffsetSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p OffsetSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type DB struct {
	dir             string
	reader          *Reader
	closed          []uint64
	current         uint64
	MostRecent      int64
	RotateThreshold int64
	SnapshotBuffer  uint64
	wtimer          Timer
	rtimer          Timer
	stream          stream.Stream
	mockoffset      int64
	raft            raft.Server
}

func NewDb(path string) *DB {
	db := &DB{
		dir:             path,
		reader:          NewReader(path),
		wtimer:          NilTimer{},
		rtimer:          NilTimer{},
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

func (db *DB) setRaft(r raft.Server) {
	db.raft = r
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

func (db *DB) WriteAll(commit uint64, bodies [][]byte, indexes []map[string]string, timestamp int64) error {
	if commit <= db.current {
		// old commit
		return nil
	}

	if db.stream == nil {
		for i, body := range bodies {
			bytes, _ := stream.Serialize(body, indexes[i], map[string]int64{})
			db.mockoffset += int64(len(bytes))
		}

		return nil
	}

	db.wtimer.Time(func() {
		for i, body := range bodies {
			_, err := db.stream.Write(body, indexes[i])
			if err != nil {
				log.Fatal(err)
			}
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

func (db *DB) ScanAll(name, value string, after uint64, scanner stream.Scanner) error {
	db.reader.Update(db.peerConnectionStrings(), db.closed, db.current, db.stream)
	return db.reader.ScanAll(name, value, after, scanner)
}

func (db *DB) Scan(name, value string, after uint64, continuation string, scanner stream.Scanner) (string, error) {
	db.reader.Update(db.peerConnectionStrings(), db.closed, db.current, db.stream)
	return db.reader.Scan(name, value, after, continuation, scanner)
}

func (db *DB) Iterate(after uint64, continuation string, scanner stream.Scanner) (string, error) {
	db.reader.Update(db.peerConnectionStrings(), db.closed, db.current, db.stream)
	return db.reader.Iterate(after, continuation, scanner)
}

func (db *DB) Continuation(name, value string) string {
	if db.stream != nil {
		if offset, err := db.stream.First(name, value); err == nil && offset > 0 {
			return db.reader.buildContinuation(db.current, offset)
		}
	}

	db.reader.Update(db.peerConnectionStrings(), db.closed, db.current, db.stream)
	return db.reader.buildContinuation(db.reader.Prev(math.MaxUint64), 0)
}

func (db *DB) Compress(start, stop uint64) {
	newclosed := make([]uint64, 0, len(db.closed))

	for _, commit := range db.closed {
		if commit < start || commit > stop {
			newclosed = append(newclosed, commit)
		}
	}

	newclosed = append(newclosed, start)

	sort.Sort(OffsetSlice(newclosed))

	if _, err := os.Open(db.reader.compressedpath(start)); !os.IsNotExist(err) {
		if err := os.Rename(db.reader.compressedpath(start), db.reader.Path(start)); err != nil {
			log.Fatal(err)
		}
	}

	db.closed = newclosed
}

func (db *DB) retrieveStream(commit uint64, fetchMissing bool) (stream.Stream, error) {
	if db.current == commit && db.stream != nil {
		return db.stream, nil
	}

	return db.reader.retrieveStream(commit, fetchMissing)
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

func (db *DB) peerConnectionStrings() []string {
	peers := make([]string, 0, len(db.raft.Peers()))

	for _, peer := range db.raft.Peers() {
		peers = append(peers, peer.ConnectionString)
	}

	return peers
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

	err := os.Remove(db.reader.Path(commit))
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		log.Fatal(err)
	}

	s, err := stream.New(db.reader.Path(commit))
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
