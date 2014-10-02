package esdb

import (
	"bytes"
	"os"
	"sync"

	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/bounded"
	"github.com/customerio/esdb/sst"
)

// TODO Verify(file string) bool

type Db struct {
	file          *os.File
	index         *sst.Reader
	locations     map[string][]int64
	calcLocations sync.Once
}

// Opens a .esdb file for reading.
func Open(path string) (*Db, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	st, err := findIndex(file)
	if err != nil {
		return nil, err
	}

	return &Db{
		file:  file,
		index: st,
	}, nil
}

// Finds and returns a space by it's id.
func (db *Db) Find(id []byte) *Space {
	if val, err := db.index.Get(id); err == nil {
		b := bytes.NewReader(val)

		// The entry in the SSTable index is
		// the offset and length of the space
		// within the file.
		offset := binary.ReadInt64(b)
		length := binary.ReadInt64(b)

		return openSpace(
			db.file,
			id,
			offset,
			length,
		)
	}

	return nil
}

// Iterates and returns each defined space.
func (db *Db) Iterate(process func(s *Space) bool) error {
	if iter, err := db.index.Find([]byte("")); err == nil {

		for iter.Next() {
			if !process(db.Find(iter.Key())) {
				break
			}
		}

		return nil
	} else {
		return err
	}
}

func (db *Db) Close() {
	if db.file != nil {
		db.file.Close()
	}
}

func findIndex(f *os.File) (*sst.Reader, error) {
	// The last 8 bytes in the file is the length
	// of the SSTable spaces index.
	f.Seek(-8, 2)
	indexLen := binary.ReadInt64(f)

	return sst.NewReader(bounded.New(f, -8-indexLen, -8), indexLen)
}
