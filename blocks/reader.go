package blocks

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/snappy"
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
	err = r.ensureScratch(uint(headerLen(r.blockSize)))
	if err != nil {
		return
	}

	head := make([]byte, headerLen(r.blockSize))
	n, err := r.scratch.Read(head)
	if n == 0 || err != nil {
		return fmt.Errorf("Error reading block header. %d %v", n, err)
	}

	length, encoding := r.parseHeader(head)

	if length == 0 {
		return
	}

	err = r.ensureScratch(length)
	if err != nil {
		return
	}

	body := make([]byte, length)
	n, err = r.scratch.Read(body)

	if n == 0 || err != nil {
		return fmt.Errorf("Error reading block. %d %d %d %v", length, encoding, n, err)
	}

	if encoding == SNAPPY_COMPRESSION {
		body, _ := snappy.Decode(nil, body[:n])
		r.buffer.Write(body)
	} else {
		r.buffer.Write(body[:n])
	}

	return
}

// Ensure the raw scratch space contains at least `length` bytes.
func (r *Reader) ensureScratch(length uint) (err error) {
	var n int

	if uint(r.scratch.Len()) < length {
		block := make([]byte, headerLen(r.blockSize)+r.blockSize)
		n, err = r.reader.Read(block)
		r.scratch.Write(block[:n])
	}

	return
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
