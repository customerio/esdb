package esdb

import (
	"bytes"
	"encoding/binary"
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

func (db *Db) Find(id []byte) *Block {
	if location, err := db.index.Get(id); err == nil {
		loc := readLocation(location)
		return block(
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

	var indexLength int64
	binary.Read(f, binary.LittleEndian, &indexLength)

	f.Seek(-8-indexLength, 2)
	index := make([]byte, indexLength)
	f.Read(index)

	return sst.NewReader(bytes.NewReader(index), indexLength)
}

func readLocation(data []byte) []uint64 {
	var offset, length uint64

	binary.Read(bytes.NewReader(data[:8]), binary.LittleEndian, &offset)
	binary.Read(bytes.NewReader(data[8:]), binary.LittleEndian, &length)

	return []uint64{offset, length}
}
