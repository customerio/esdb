package blocks

import (
	"bytes"
	"errors"
	"io"

	"github.com/golang/snappy"
)

var BadSeek = errors.New("block reader can only seek relative to beginning of file.")

// Reader has the ability to uncompress and read any potentially compressed
// data written via blocks.Writer.
type Reader struct {
	buffer      *bytes.Buffer
	scratch     *bytes.Buffer
	reader      io.Reader
	blockSize   int
	headerLen   int
	parseHeader func(head []byte) (size uint, encoding int)
	block       []byte
	snapbuf     []byte
}

// Transforms a bytestring into a block reader. blockSize must be the same size
// used to originally write the blocks you're attempting to read.  Otherwise
// you'll definitely get incorrect results.
func NewByteReader(b []byte, blockSize int) *Reader {
	return NewReader(bytes.NewReader(b), blockSize)
}

// Transforms any object which implements io.Reader and io.Seeker into a block
// reader. blockSize must be the same used to originally write the blocks
// you're attempting to read.  the same size used to write the blocks you're
// attempting to read.  Otherwise you'll definitely get incorrect results.
func NewReader(r io.Reader, blockSize int) *Reader {
	var (
		parseHeader func(head []byte) (size uint, encoding int)
		headerLen   int
	)

	if blockSize < 65536 {
		parseHeader = parseHeader16
		headerLen = 3 // uint16 + byte
	} else if blockSize < 4294967296 {
		parseHeader = parseHeader32
		headerLen = 5 // uint32 + byte
	} else {
		parseHeader = parseHeader64
		headerLen = 9 // uint64 + byte
	}

	return &Reader{
		buffer:      new(bytes.Buffer),
		scratch:     new(bytes.Buffer),
		reader:      r,
		blockSize:   blockSize,
		headerLen:   headerLen,
		parseHeader: parseHeader,
		block:       make([]byte, headerLen+blockSize),
		snapbuf:     make([]byte, 2*blockSize),
	}
}

// Implements io.Reader interface.
func (r *Reader) Read(p []byte) (n int, err error) {
	err = r.ensure(len(p))
	if err != nil {
		return
	}

	return r.buffer.Read(p)
}

// Implements io.ByteReader interface.
func (r *Reader) ReadByte() (c byte, err error) {
	err = r.ensure(1)
	if err != nil {
		return
	}

	b := make([]byte, 1)

	_, err = r.buffer.Read(b)
	c = b[0]

	return
}

// Returns the next n bytes in the buffer, without advancing.
// A following call to Read will return the same bytes.
func (r *Reader) Peek(n int) []byte {
	r.ensure(n)
	return r.buffer.Bytes()[:n]
}

// Implements io.Seeker interface. One limitiation we have is
// the only valid value of whence is 0, overwise our version of
// Seek will return an error.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	seeker, ok := r.reader.(io.Seeker)
	if !ok || whence != 0 {
		return 0, BadSeek
	}

	r.buffer = new(bytes.Buffer)
	r.scratch = new(bytes.Buffer)
	return seeker.Seek(offset, 0)
}

// Ensure the buffer contains at least `length` bytes
func (r *Reader) ensure(length int) (err error) {
	for r.buffer.Len() < length {
		err = r.fetchBlock()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return
		}
	}

	return
}

// Fetches the next block from the underlying reader,
// optionally decompresses it, and adds it to the buffer.
func (r *Reader) fetchBlock() (err error) {
	err = r.ensureScratch(uint(r.headerLen))
	if err != nil {
		return
	}

	head := r.scratch.Next(r.headerLen)
	length, encoding := r.parseHeader(head)
	if length == 0 {
		return
	}

	err = r.ensureScratch(length)
	if err != nil {
		return
	}

	body := r.scratch.Next(int(length))
	if encoding == SNAPPY_COMPRESSION {
		body, _ := snappy.Decode(r.snapbuf, body)
		r.snapbuf = body
		r.buffer.Write(body)
	} else {
		r.buffer.Write(body)
	}

	return
}

// Ensure the raw scratch space contains at least `length` bytes.
func (r *Reader) ensureScratch(length uint) (err error) {
	var n int

	for uint(r.scratch.Len()) < length {
		n, err = r.reader.Read(r.block)
		if err != nil {
			return
		}
		r.scratch.Write(r.block[:n])
	}

	return
}
