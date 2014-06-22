package esdb

import (
	"errors"
	"os"
)

// Verify(file string) bool

// Open(file string) *Db, error
// db.Close()

// Create(file string) *Db, error
// db.Flush(id []byte) error
// db.Finalize() error

type Db struct {
	file    *os.File
	blocks  map[string]*Block
	written bool
}

func Open(path string) (*Db, error) {
	return &Db{blocks: make(map[string]*Block), written: true}, nil
}

func Create(path string) (*Db, error) {
	return &Db{blocks: make(map[string]*Block), written: false}, nil
}

func (db *Db) Find(id []byte) *Block {
	return db.blocks[string(id)]
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
		block = createBlock(db.file, id, index)
		db.blocks[string(id)] = block
	}

	if block.primary != index {
		return errors.New(`Primary index for this block is '" +
			block.primary +
			'". This event has a primary index of '" +
			index + "'. Cannot have multiple primary indexes." +
			"Move one to a secondary index.`)
	}

	return block.add(timestamp, data, append(secondaries, index))
}

func (db *Db) Flush(id []byte) error {
	if block := db.blocks[string(id)]; block != nil {
		return block.write()
	}

	return nil
}

func (db *Db) Finalize() error {
	for _, block := range db.blocks {
		if err := block.write(); err != nil {
			return err
		}
	}

	return db.write()
}

func (db *Db) write() error {
	db.written = true

	// write block index lookup sst

	return nil
}
