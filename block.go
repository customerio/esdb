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
	key   int
	first uint64
	last  uint64
}

func block(reader io.ReadSeeker, id []byte, offset, length uint64) *Block {
	st, err := findBlockIndex(reader, offset, length)
	if err != nil {
		return nil
	}

	return &Block{
		Id:     id,
		index:  st,
		buf:    newBuffer(reader, offset, offset+length, 4096),
		offset: offset,
	}
}

func (b *Block) Scan(group string, scanner Scanner) error {
	if index := b.findIndex("g" + group); index != nil && index.first > 0 {
		offset := b.offset + index.first

		b.buf.Move(offset, 4096)

		data := eventData(b.buf)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			data = eventData(b.buf)
		}
	}

	return nil
}

func eventData(buf *buffer) []byte {
	return buf.Next(int(buf.Uvarint()))
}

func (b *Block) ScanIndex(index string, scanner Scanner) error {
	if index := b.findIndex("i" + index); index != nil && index.first > 0 {
		offset := b.offset + index.first

		b.buf.Move(offset, 0)

		data := eventData(b.buf)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			if next := event.nextOffsets[index.key]; next > 0 {
				offset = b.offset + next
				b.buf.Move(offset, 0)
				data = eventData(b.buf)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) RevScanIndex(index string, scanner Scanner) error {
	if index := b.findIndex("i" + index); index != nil && index.last > 0 {
		offset := b.offset + index.last

		b.buf.Move(offset, 0)
		data := eventData(b.buf)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			if prev := event.prevOffsets[index.key]; prev > 0 {
				offset = b.offset + prev
				b.buf.Move(offset, 0)
				data = eventData(b.buf)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) findIndex(name string) *index {
	if metadata, err := b.index.Get([]byte(name)); err == nil {
		key, n := binary.Uvarint(metadata)

		var first uint64
		binary.Read(bytes.NewReader(metadata[n:n+8]), binary.LittleEndian, &first)

		var last uint64
		binary.Read(bytes.NewReader(metadata[n+8:]), binary.LittleEndian, &last)

		return &index{
			key:   int(key),
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
