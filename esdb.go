package esdb

import (
	"bytes"
	"os"
	"sync"

	"github.com/customerio/esdb/sst"
)

// Verify(file string) bool

type Db struct {
	file          *os.File
	index         *sst.Reader
	locations     map[string][]int64
	calcLocations sync.Once
}

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

func (db *Db) Find(id []byte) *Space {
	if location, err := db.index.Get(id); err == nil {
		loc := readLocation(location)
		return openSpace(
			db.file,
			id,
			loc[0],
			loc[1],
		)
	}

	return nil
}

func (db *Db) Close() {
	if db.file != nil {
		db.file.Close()
	}
}

func findIndex(f *os.File) (*sst.Reader, error) {
	f.Seek(-8, 2)
	indexLen := readInt64(f)

	f.Seek(-8-indexLen, 2)
	index := readBytes(f, indexLen)

	return sst.NewReader(bytes.NewReader(index), indexLen)
}

func readLocation(data []byte) []int64 {
	r := bytes.NewReader(data)

	offset := readInt64(r)
	length := readInt64(r)

	return []int64{offset, length}
}
