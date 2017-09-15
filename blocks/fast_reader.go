package blocks

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

type read struct {
	bytes []byte
	err   error
}

// Reader has the ability to uncompress and read any potentially compressed
// data written via blocks.Writer. Reads ahead to decompress future
// blocks before you need them to improve performance.
type FastReader struct {
	buffer          *bytes.Buffer
	scratch         *bytes.Buffer
	reads           chan read
	readErr         error
	reader          io.Reader
	blockSize       int
	readAheadBlocks int
}

// Version of blocks.Reader which reads ahead and decompresses future
// blocks before you need them to improve performance.
func NewFastReader(r io.Reader, blockSize int, readAheadBlocks int) *FastReader {
	reader := &FastReader{new(bytes.Buffer), new(bytes.Buffer), make(chan read, 20), nil, r, blockSize, readAheadBlocks}

	go reader.readAhead()

	return reader
}

// Implements io.Reader interface.
func (r *FastReader) Read(p []byte) (n int, err error) {
	err = r.ensure(len(p))
	if err != nil {
		return
	}

	return r.buffer.Read(p)
}

// Implements io.ByteReader interface.
func (r *FastReader) ReadByte() (c byte, err error) {
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
func (r *FastReader) Peek(n int) []byte {
	r.ensure(n)
	return r.buffer.Bytes()[:n]
}

// Ensure the buffer contains at least `length` bytes
func (r *FastReader) ensure(length int) (err error) {
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
func (r *FastReader) fetchBlock() (err error) {
	err = r.ensureScratch(uint(headerLen(r.blockSize)))
	if err != nil {
		return
	}

	head := make([]byte, headerLen(r.blockSize))
	n, err := r.scratch.Read(head)
	if n == 0 || err != nil {
		return fmt.Errorf("Error reading block header. %d %v", n, err)
	}

	length, encoding := parseHeader(r.blockSize, head)

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
func (r *FastReader) ensureScratch(length uint) (err error) {
	if uint(r.scratch.Len()) < length {
		// If we read with an error, keep returning it
		if r.readErr != nil {
			return r.readErr
		}

		read := <-r.reads
		r.scratch.Write(read.bytes)
		r.readErr = read.err
		err = read.err
	}

	return
}

func (r *FastReader) readAhead() {
	var n int
	var err error

	for err == nil {
		bytes := make([]byte, (headerLen(r.blockSize)+r.blockSize)*r.readAheadBlocks)
		n, err = r.reader.Read(bytes)
		r.reads <- read{bytes[:n], err}
		if err != nil {
			close(r.reads)
		}
	}
}
