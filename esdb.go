package esdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

// Verify(file string) bool

type Db struct {
	file          *os.File
	locations     map[string][]int64
	calcLocations sync.Once
}

func Open(path string) (*Db, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Db{
		file: file,
	}, nil
}

func (db *Db) Find(id []byte) *Block {
	if loc := db.findBlockLocation(id); loc != nil {
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

func (db *Db) findBlockLocation(id []byte) []int64 {
	if db.locations == nil {
		db.calcLocations.Do(func() {
			db.file.Seek(-8, 2)

			var indexLength int64
			binary.Read(db.file, binary.LittleEndian, &indexLength)

			db.file.Seek(-8-indexLength, 2)

			indexes := make([]byte, indexLength)
			db.file.Read(indexes)

			db.locations = make(map[string][]int64)

			for index := 0; index < len(indexes); {
				idLen, n := binary.Uvarint(indexes[index:])
				index += n

				id := string(indexes[index : index+int(idLen)])
				index += int(idLen)

				var offset int64
				binary.Read(bytes.NewReader(indexes[index:index+8]), binary.LittleEndian, &offset)
				index += 8

				var length int64
				binary.Read(bytes.NewReader(indexes[index:index+8]), binary.LittleEndian, &length)
				index += 8

				db.locations[id] = []int64{offset, length}
			}
		})
	}

	return db.locations[string(id)]
}
