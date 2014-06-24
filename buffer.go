package esdb

import (
	"bytes"
	"encoding/binary"
	"io"
)

type buffer struct {
	*bytes.Buffer
	reader   io.ReadSeeker
	original uint64
	start    uint64
	limit    uint64
	offset   uint64
	pageSize int
}

func newBuffer(r io.ReadSeeker, start, limit uint64, pageSize int) *buffer {
	return &buffer{bytes.NewBuffer([]byte{}), r, start, start, limit, 0, pageSize}
}

func (b *buffer) Reset() {
	b.start = b.original
	b.offset = 0
	b.Buffer.Reset()
}

func (b *buffer) Move(start uint64, pageSize int) {
	b.start = start
	b.offset = 0
	b.pageSize = pageSize
	b.Buffer.Reset()
}

func (b *buffer) Next(n int) []byte {
	b.ensure(n)
	b.offset += uint64(n)

	if b.offset > b.limit {
		b.offset = b.limit
	}

	return b.Buffer.Next(n)
}

func (b *buffer) Uvarint() uint64 {
	b.ensure(10)

	num, n := binary.Uvarint(b.Bytes()[:10])
	b.Next(n)
	return num
}

func (b *buffer) Uint32() uint32 {
	var num uint32
	binary.Read(bytes.NewReader(b.Next(4)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) Uint64() uint64 {
	var num uint64
	binary.Read(bytes.NewReader(b.Next(8)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) ensure(num int) {
	request := b.pageSize

	if num > request {
		request = num
	}

	for b.Len() < num && b.offset < b.limit-b.start {
		b.ReadFrom(b.nextBlock(request))
	}
}

func (b *buffer) nextBlock(requested int) io.Reader {
	start := b.start + b.offset + uint64(b.Len())

	if start > b.limit {
		start = b.limit
	}

	b.reader.Seek(int64(start), 0)

	length := uint64(requested)

	if b.offset+length > b.limit-b.start {
		length = (b.limit - b.start) - b.offset
	}

	data := make([]byte, length)
	b.reader.Read(data)

	return bytes.NewReader(data)
}
