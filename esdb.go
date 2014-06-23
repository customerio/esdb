package esdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"sync"
)

// Verify(file string) bool

type Db struct {
	file          *os.File
	blocks        map[string]*Block
	written       bool
	offset        int64
	locations     map[string][]int64
	calcLocations sync.Once
}

func Open(path string) (*Db, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &Db{
		file:    file,
		blocks:  make(map[string]*Block),
		written: true,
	}, nil
}

func Create(path string) (*Db, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return &Db{
		file:    file,
		blocks:  make(map[string]*Block),
		written: false, // to be explicit...
	}, nil
}

func (db *Db) Find(id []byte) *Block {
	if loc := db.findBlockLocation(id); loc != nil {
		return newBlock(
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

func (db *Db) Add(id []byte, timestamp int, data []byte, index string, secondaries []string) error {
	if db.written {
		return errors.New("Cannot add to database. We're immutable and this one has already been written.")
	}

	block := db.blocks[string(id)]

	if block == nil {
		block = createBlock(db.file, id)
		db.blocks[string(id)] = block
	}

	return block.add(timestamp, data, index, secondaries)
}

func (db *Db) Flush(id []byte) (err error) {
	if block := db.blocks[string(id)]; block != nil {
		err = block.write()
		block.offset = db.offset
		db.offset += block.length
	}

	return
}

func (db *Db) Finalize() (err error) {
	for _, block := range db.blocks {
		if err = block.write(); err == nil {
			block.offset = db.offset
			db.offset += block.length
		} else {
			return
		}
	}

	return db.write()
}

func (db *Db) write() error {
	db.written = true

	buf := new(bytes.Buffer)

	blocks := make(sort.StringSlice, 0)
	offsets := make(map[string]int64)
	lengths := make(map[string]int64)

	for _, block := range db.blocks {
		blocks = append(blocks, string(block.Id))
		offsets[string(block.Id)] = block.offset
		lengths[string(block.Id)] = block.length
	}

	blocks.Sort()

	for _, block := range blocks {
		buf.Write(varInt(len(block)))
		buf.Write([]byte(block))
		binary.Write(buf, binary.LittleEndian, offsets[block])
		binary.Write(buf, binary.LittleEndian, lengths[block])
	}

	binary.Write(buf, binary.LittleEndian, int64(buf.Len()))

	_, err := buf.WriteTo(db.file)

	return err
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
