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
	if off := b.firstIndexOffset("g" + group); off > 0 {
		offset := b.offset + off

		b.buf.Move(offset)

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
	if off := b.firstIndexOffset("i" + index); off > 0 {
		offset := b.offset + off

		b.buf.Move(offset)

		data := eventData(b.buf)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			if next := event.nextOffsets[index]; next > 0 {
				offset = b.offset + next
				b.buf.Move(offset)
				data = eventData(b.buf)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) RevScanIndex(index string, scanner Scanner) error {
	if off := b.lastIndexOffset("i" + index); off > 0 {
		offset := b.offset + off

		b.buf.Move(offset)
		data := eventData(b.buf)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			if prev := event.prevOffsets[index]; prev > 0 {
				offset = b.offset + prev
				b.buf.Move(offset)
				data = eventData(b.buf)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) firstIndexOffset(index string) uint64 {
	return b.findIndexOffsets(index)[0]
}

func (b *Block) lastIndexOffset(index string) uint64 {
	return b.findIndexOffsets(index)[1]
}

func (b *Block) findIndexOffsets(index string) []uint64 {
	if offsets, err := b.index.Get([]byte(index)); err == nil {
		var startOffset uint64
		binary.Read(bytes.NewReader(offsets[:8]), binary.LittleEndian, &startOffset)

		var stopOffset uint64
		binary.Read(bytes.NewReader(offsets[8:]), binary.LittleEndian, &stopOffset)

		return []uint64{startOffset, stopOffset}
	}

	return []uint64{0, 0}
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
