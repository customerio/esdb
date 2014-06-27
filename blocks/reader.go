package blocks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var BadSeek = errors.New("block reader can only seek relative to beginning of file.")

type Reader struct {
	buf       *bytes.Buffer
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
	e := r.buffer(len(p))

	n, err = r.buf.Read(p)

	if e != nil && err == nil {
		err = e
	}

	return
}

func (r *Reader) buffer(length int) error {
	for r.buf.Len() < length {
		block := make([]byte, blockLen(r.blockSize))
		n, err := r.reader.Read(block)
		r.scratch.Write(block)

		if n >= headerLen(r.blockSize) {
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

	var bodyLen int
	n := fixedInt(r.blockSize, 0)
	if num, ok := n.(int16); ok {
		binary.Read(bytes.NewReader(head), binary.LittleEndian, &num)
		bodyLen = int(num)
	} else if num, ok := n.(int32); ok {
		binary.Read(bytes.NewReader(head), binary.LittleEndian, &num)
		bodyLen = int(num)
	} else if num, ok := n.(int64); ok {
		binary.Read(bytes.NewReader(head), binary.LittleEndian, &num)
		bodyLen = int(num)
	}

	body := make([]byte, bodyLen)
	r.scratch.Read(body)

	r.buf.Write(body)
}

func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 {
		return 0, BadSeek
	}

	r.buf = new(bytes.Buffer)
	r.scratch = new(bytes.Buffer)
	return r.reader.Seek(offset, 0)
}
