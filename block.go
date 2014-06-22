package esdb

import (
	"errors"
	"os"
	"sort"
)

// block.write

type Scanner func(*Event) bool

type Block struct {
	Id      []byte
	file    *os.File
	offset  int
	size    int
	written bool
	indexes map[string]Events
	primary string
}

func createBlock(file *os.File, id []byte, primary string) *Block {
	return &Block{
		Id:      id,
		file:    file,
		written: false, // to be explicit...
		indexes: make(map[string]Events),
		primary: primary,
	}
}

func newBlock(file *os.File, id []byte, offset, size int) *Block {
	return &Block{
		Id:      id,
		file:    file,
		offset:  offset,
		size:    size,
		written: true,
	}
}

func (b *Block) Scan(index string, scanner Scanner) {
	if b.indexes[index] == nil {
		return
	}

	sort.Sort(b.indexes[index])

	for _, event := range b.indexes[index] {
		scanner(event)
	}
}

func (b *Block) RevScan(index string, scanner Scanner) {
	sort.Sort(sort.Reverse(b.indexes[index]))

	for _, event := range b.indexes[index] {
		scanner(event)
	}
}

func (b *Block) add(timestamp int, data []byte, indexes []string) error {
	if b.written {
		return errors.New("Cannot add to block. We're immutable and this one has already been written.")
	}

	event := &Event{Timestamp: timestamp, Data: data}

	for _, name := range indexes {
		if b.indexes[name] == nil {
			b.indexes[name] = make([]*Event, 0)
		}

		b.indexes[name] = append(b.indexes[name], event)
	}

	return nil
}

func (b *Block) write() error {
	if b.written {
		return nil
	}

	b.written = true

	// sort events
	// build indexes
	// writer block header (including primary index)
	// write all events
	// write index lookup sst
	// write footer

	return nil
}
