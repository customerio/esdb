package esdb

import (
	"bytes"
	"encoding/binary"
	"io"
)

type buffer struct {
	reader    io.ReadSeeker
	buf       *bytes.Buffer
	endBuf    *bytes.Buffer
	original  uint64
	start     uint64
	limit     uint64
	offset    uint64
	endOffset uint64
	pageSize  int
}

type writeBuffer struct {
	*bytes.Buffer
}

func newBuffer(r io.ReadSeeker, start, limit uint64, pageSize int) *buffer {
	return &buffer{r, bytes.NewBuffer([]byte{}), bytes.NewBuffer([]byte{}), start, start, limit, 0, limit, pageSize}
}

func newByteBuffer(b []byte) *buffer {
	return newBuffer(bytes.NewReader(b), 0, uint64(len(b)), len(b))
}

func newWriteBuffer(b []byte) *writeBuffer {
	return &writeBuffer{bytes.NewBuffer(b)}
}

func (w *writeBuffer) PushUvarint(num int) {
	b := make([]byte, 8)
	n := binary.PutUvarint(b, uint64(num))
	w.Write(b[:n])
}

func (w *writeBuffer) Push(b []byte) {
	w.Write(b)
}

func (w *writeBuffer) PushUint64(num int) {
	binary.Write(w, binary.LittleEndian, uint64(num))
}

func (w *writeBuffer) PushUint16(num int) {
	binary.Write(w, binary.LittleEndian, uint16(num))
}

func (b *buffer) Reset() {
	b.start = b.original
	b.offset = 0
	b.endOffset = b.limit
	b.buf.Reset()
}

func (b *buffer) Move(start uint64, pageSize int) {
	b.start = start
	b.offset = 0
	b.endOffset = b.limit
	b.pageSize = pageSize
	b.buf.Reset()
}

func (b *buffer) PullUvarint() int {
	return int(b.PullUvarint64())
}

func (b *buffer) PullUvarint64() uint64 {
	b.ensure(10)

	num, n := binary.Uvarint(b.Bytes()[:10])
	b.Pull(n)
	return num
}

func (b *buffer) PullUint16() uint16 {
	var num uint16
	binary.Read(bytes.NewReader(b.Pull(2)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) PopUint16() uint16 {
	var num uint16
	binary.Read(bytes.NewReader(b.Pop(2)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) PullUint32() uint32 {
	var num uint32
	binary.Read(bytes.NewReader(b.Pull(4)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) PopUint32() uint32 {
	var num uint32
	binary.Read(bytes.NewReader(b.Pop(4)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) PullUint64() uint64 {
	var num uint64
	binary.Read(bytes.NewReader(b.Pull(8)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) PopUint64() uint64 {
	var num uint64
	binary.Read(bytes.NewReader(b.Pop(8)), binary.LittleEndian, &num)
	return num
}

func (b *buffer) Len() int {
	return b.buf.Len()
}

func (b *buffer) Bytes() []byte {
	return b.buf.Bytes()
}

func (b *buffer) Pull(n int) []byte {
	b.ensure(n)
	b.offset += uint64(n)

	if b.offset > b.limit {
		b.offset = b.limit
	}

	return b.buf.Next(n)
}

func (b *buffer) Pop(n int) []byte {
	b.ensureEnd(n)

	if b.endOffset >= uint64(n) {
		b.endOffset -= uint64(n)
	}

	if b.endOffset < b.start {
		b.endOffset = b.start
	}

	return reverse(b.endBuf.Next(n))
}

func (b *buffer) ensure(num int) {
	request := b.pageSize

	if num > request {
		request = num
	}

	for b.Len() < num && b.offset < b.limit-b.start {
		b.buf.ReadFrom(b.nextSpace(request))
	}
}

func (b *buffer) ensureEnd(num int) {
	request := b.pageSize

	if num > request {
		request = num
	}

	for b.endBuf.Len() < num && b.endOffset > b.start {
		b.endBuf.ReadFrom(b.prevSpace(request))
	}
}

func (b *buffer) nextSpace(requested int) io.Reader {
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

func (b *buffer) prevSpace(requested int) io.Reader {
	current := b.endOffset - uint64(b.endBuf.Len())

	var start uint64

	if uint64(requested) > current {
		start = b.start
	} else {
		start = current - uint64(requested)
	}

	if start < b.start {
		start = b.start
	}

	b.reader.Seek(int64(start), 0)

	length := uint64(requested)

	if start+length >= current {
		length = current - start
	}

	data := make([]byte, length)
	b.reader.Read(data)

	return bytes.NewReader(reverse(data))
}

func reverse(bytes []byte) []byte {
	for i, j := 0, len(bytes)-1; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}

	return bytes
}
