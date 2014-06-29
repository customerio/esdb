package blocks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/dgryski/go-csnappy"
)

var BadSeek = errors.New("block reader can only seek relative to beginning of file.")

// Reader has the ability to uncompress and read any potentially compressed
// data written via blocks.Writer.
type Reader struct {
	buffer    *bytes.Buffer
	scratch   *bytes.Buffer
	reader    io.ReadSeeker
	blockSize int
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
func NewReader(r io.ReadSeeker, blockSize int) *Reader {
	return &Reader{new(bytes.Buffer), new(bytes.Buffer), r, blockSize}
}

// Implements io.Reader interface.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.fetch(len(p))
	return r.buffer.Read(p)
}

// Implements io.ByteReader interface.
func (r *Reader) ReadByte() (c byte, err error) {
	r.fetch(1)

	b := make([]byte, 1)

	_, err = r.buffer.Read(b)
	c = b[0]

	return
}

// Returns the next n bytes in the buffer, without advancing.
// A following call to Read will return the same bytes.
func (r *Reader) Peek(n int) []byte {
	r.fetch(n)
	return r.buffer.Bytes()[:n]
}

func (r *Reader) fetch(length int) error {
	for r.buffer.Len() < length {
		block := make([]byte, headerLen(r.blockSize)+r.blockSize)
		n, err := r.reader.Read(block)
		r.scratch.Write(block[:n])

		for r.buffer.Len() < length &&
			r.scratch.Len() > headerLen(r.blockSize) {
			r.parse()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// Parse uncompresses the next block in the
// scratch buffer, and adds the uncompressed
// content to the main reader buffer.
func (r *Reader) parse() {
	head := make([]byte, headerLen(r.blockSize))
	r.scratch.Read(head)

	length, encoding := r.parseHeader(head)

	body := make([]byte, length)
	n, _ := r.scratch.Read(body)

	if encoding == SNAPPY_COMPRESSION {
		body, _ := csnappy.Decode(nil, body[:n])
		r.buffer.Write(body)
	} else {
		r.buffer.Write(body[:n])
	}
}

// Implements io.Seeker interface. One limitiation we have is
// the only valid value of whence is 0, overwise our version of
// Seek will return an error.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 {
		return 0, BadSeek
	}

	r.buffer = new(bytes.Buffer)
	r.scratch = new(bytes.Buffer)
	return r.reader.Seek(offset, 0)
}

// Parses out the header information from a fixed set of bytes.
// The header format is described more fully in the blocks.Writer
// documentation, but basically it's:
//
// [uint16/uint32/uint64:blockLength][int8:encoding]
//
func (r *Reader) parseHeader(head []byte) (size uint, encoding int) {
	buf := bytes.NewReader(head)

	// Read fixed uint block length based on the
	// configured blocksize for the reader. Since
	// the fixed uint varies based on the max
	// block size, we need to parse out how long
	// the fixed int is before reading it from the
	// buffer.
	n := fixedInt(r.blockSize, 0)

	if num, ok := n.(uint16); ok {
		binary.Read(buf, binary.LittleEndian, &num)
		size = uint(num)
	} else if num, ok := n.(uint32); ok {
		binary.Read(buf, binary.LittleEndian, &num)
		size = uint(num)
	} else if num, ok := n.(uint64); ok {
		binary.Read(buf, binary.LittleEndian, &num)
		size = uint(num)
	}

	// Read next byte which is the block encoding.
	b, _ := buf.ReadByte()
	encoding = int(b)

	return
}
