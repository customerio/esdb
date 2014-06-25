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
	if offset := b.findGroupingOffset(grouping); offset > 0 {
		b.buf.Move(b.offset+offset, 4096)

		for event := nextEvent(b.buf); event != nil; {
			if !scanner(event) {
				return
			}

			event = nextEvent(b.buf)
		}
	}
}

func (b *Block) ScanIndex(name, value string, scanner Scanner) {
	if buf := b.findIndex(name, value); buf != nil {
		for offset := buf.PullUint64(); offset > 0; {
			b.buf.Move(b.offset+offset, 0)

			if !scanner(nextEvent(b.buf)) {
				return
			}

			offset = buf.PullUint64()
		}
	}
}

func (b *Block) RevScanIndex(name, value string, scanner Scanner) {
	if buf := b.findIndex(name, value); buf != nil {
		for offset := buf.PopUint64(); offset > 0; {
			b.buf.Move(b.offset+offset, 0)

			if !scanner(nextEvent(b.buf)) {
				return
			}

			offset = buf.PopUint64()
		}
	}
}

func (b *Block) findGroupingOffset(name string) uint64 {
	if value, err := b.index.Get([]byte("g" + name)); err == nil {
		return newByteBuffer(value).PullUvarint()
	}

	return 0
}

func (b *Block) findIndex(name, value string) *buffer {
	if val, err := b.index.Get([]byte("i" + name + ":" + value)); err == nil {
		buf := newByteBuffer(val)

		offset := buf.PullUvarint()
		length := buf.PullUvarint()

		return newBuffer(b.buf.reader, b.offset+offset, b.offset+offset+length, 4096)
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
