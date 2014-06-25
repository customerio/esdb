package esdb

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/customerio/esdb/sst"
)

type Scanner func(*Event) bool

type Block struct {
	Id []byte

	buf    *buffer
	offset uint64
	index  *sst.Reader
}

type index struct {
	key   uint64
	first uint64
	last  uint64
}

func openBlock(reader io.ReadSeeker, id []byte, offset, length uint64) *Block {
	if st, err := findBlockIndex(reader, offset, length); err == nil {
		return &Block{
			Id:     id,
			index:  st,
			buf:    newBuffer(reader, offset, offset+length, 4096),
			offset: offset,
		}
	}

	return nil
}

func (b *Block) Scan(grouping string, scanner Scanner) {
	if index := b.findIndex("g" + grouping); index != nil {
		b.scan(index.first, 0, 4096, scanner, false)
	}
}

func (b *Block) ScanIndex(name, value string, scanner Scanner) {
	if index := b.findIndex("i" + name + ":" + value); index != nil {
		b.scan(index.first, index.key, 0, scanner, false)
	}
}

func (b *Block) RevScanIndex(name, value string, scanner Scanner) {
	if index := b.findIndex("i" + name + ":" + value); index != nil {
		b.scan(index.last, index.key, 0, scanner, true)
	}
}

func (b *Block) scan(offset, key uint64, pageSize int, scanner Scanner, reverse bool) {
	if offset == 0 {
		return
	}

	b.buf.Move(b.offset+offset, pageSize)

	for event := nextEvent(b.buf); event != nil; {
		if scanner(event) {
			if reverse {
				event = event.Prev(b.buf, key)
			} else {
				event = event.Next(b.buf, key)
			}

		} else {
			return
		}
	}
}

func (b *Block) findIndex(name string) *index {
	if metadata, err := b.index.Get([]byte(name)); err == nil {
		//return &index{
		//  key: buf.Uvarint(),
		//  first: buf.Uint64(),
		//  last: buf.Uint64(),
		//}
		key, n := binary.Uvarint(metadata)

		var first uint64
		binary.Read(bytes.NewReader(metadata[n:n+8]), binary.LittleEndian, &first)

		var last uint64
		binary.Read(bytes.NewReader(metadata[n+8:]), binary.LittleEndian, &last)

		return &index{
			key:   key,
			first: first,
			last:  last,
		}
	}

	return nil
}

func findBlockIndex(r io.ReadSeeker, offset, length uint64) (*sst.Reader, error) {
	r.Seek(int64(offset+length-4), 0)

	var indexLength int32
	binary.Read(r, binary.LittleEndian, &indexLength)

	r.Seek(int64(offset+length-4-uint64(indexLength)), 0)
	index := make([]byte, indexLength)
	r.Read(index)

	return sst.NewReader(bytes.NewReader(index), int64(indexLength))
}
