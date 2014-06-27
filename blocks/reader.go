package blocks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var BadSeek = errors.New("block reader can only seek relative to beginning of file.")

type Reader struct {
	buffer    *bytes.Buffer
	scratch   *bytes.Buffer
	reader    io.ReadSeeker
	blockSize int
}

func NewByteReader(b []byte, blockSize int) *Reader {
	return NewReader(bytes.NewReader(b), blockSize)
}

func NewReader(r io.ReadSeeker, blockSize int) *Reader {
	return &Reader{new(bytes.Buffer), new(bytes.Buffer), r, blockSize}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	e := r.fetch(len(p))

	n, err = r.buffer.Read(p)

	if e != nil && err == nil {
		err = e
	}

	return
}

func (r *Reader) fetch(length int) error {
	for r.buffer.Len() < length {
		block := make([]byte, headerLen(r.blockSize)+r.blockSize)
		n, err := r.reader.Read(block)
		r.scratch.Write(block[:n])

		if n > headerLen(r.blockSize) {
			r.parse()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Reader) parse() {
	head := make([]byte, headerLen(r.blockSize))
	r.scratch.Read(head)

	body := make([]byte, r.parseHeader(head))
	n, _ := r.scratch.Read(body)

	r.buffer.Write(body[:n])
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 {
		return 0, BadSeek
	}

	r.buffer = new(bytes.Buffer)
	return r.reader.Seek(offset, 0)
}

func (r *Reader) parseHeader(head []byte) int {
	n := fixedInt(r.blockSize, 0)

	if num, ok := n.(uint16); ok {
		binary.Read(bytes.NewReader(head), binary.LittleEndian, &num)
		return int(num)
	} else if num, ok := n.(uint32); ok {
		binary.Read(bytes.NewReader(head), binary.LittleEndian, &num)
		return int(num)
	} else if num, ok := n.(uint64); ok {
		binary.Read(bytes.NewReader(head), binary.LittleEndian, &num)
		return int(num)
	}

	return 0
}
