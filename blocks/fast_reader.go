package blocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"

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
	buffer        *bytes.Buffer
	scratch       *bytes.Buffer
	reads         chan read
	readErr       error
	reader        io.Reader
	blockSize     int
	snapbuf       []byte
	headerLen     int
	parseHeader   func(head []byte) (size uint, encoding int)
	pool          *sync.Pool
	readAheadWg   sync.WaitGroup
	readAheadDone chan bool // readAhead should exit on write/close
}

// Any FastReader that uses the defaults will use this allocation pool.
var (
	DefaultHeaderLen = 3
	DefaultBlockSize = 65535
	pool             = sync.Pool{
		New: func() interface{} {
			return make([]byte, DefaultHeaderLen+DefaultBlockSize)
		},
	}
)

// Version of blocks.Reader which reads ahead and decompresses future
// blocks before you need them to improve performance.
func NewFastReader(ctx context.Context, r io.Reader, blockSize int) *FastReader {
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

	var p *sync.Pool
	if headerLen == DefaultHeaderLen && blockSize == DefaultBlockSize {
		p = &pool
	}

	reader := &FastReader{
		buffer:        new(bytes.Buffer),
		scratch:       new(bytes.Buffer),
		reader:        r,
		blockSize:     blockSize,
		reads:         make(chan read, 10),
		snapbuf:       make([]byte, 2*blockSize),
		headerLen:     headerLen,
		parseHeader:   parseHeader,
		pool:          p,
		readAheadDone: make(chan bool, 1),
	}

	reader.readAheadWg.Add(1)
	go func() {
		defer reader.readAheadWg.Done()
		reader.readAhead(ctx)
	}()

	return reader
}

func (r *FastReader) Close() {
	close(r.readAheadDone)
	r.readAheadWg.Wait()
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

	c, err = r.buffer.ReadByte()
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
func (r *FastReader) ensureScratch(length uint) (err error) {
	for uint(r.scratch.Len()) < length {
		// If we read with an error, keep returning it
		if r.readErr != nil {
			return r.readErr
		}

		rd, ok := <-r.reads
		if !ok {
			// If the read channel is closed we're done due to a
			// context cancelation. Any other termination case would
			// be a io.EOF error (or an actual reading error).
			return context.Canceled
		}
		if len(rd.bytes) > 0 {
			r.scratch.Write(rd.bytes)
			if r.pool != nil {
				r.pool.Put(rd.bytes)
			}
		}
		r.readErr = rd.err
		err = rd.err
	}

	return
}

func (r *FastReader) readAhead(ctx context.Context) {
	defer close(r.reads)
	for {
		var bytes []byte
		if r.pool != nil {
			bytes = pool.Get().([]byte)
		} else {
			bytes = make([]byte, r.headerLen+r.blockSize)
		}
		if cap(bytes) == 0 {
			bytes = make([]byte, r.headerLen+r.blockSize)
		}
		bytes = bytes[:cap(bytes)]
		n, err := r.reader.Read(bytes)
		bytes = bytes[:n]
		select {
		case r.reads <- read{bytes, err}:
		case <-r.readAheadDone:
			return
		case <-ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// The header consists of two numbers.  The length of the
// encoded/compressed block, and the encoding of the block data.

// Parses out the header information from a fixed set of bytes.
// The header format is described more fully in the blocks.Writer
// documentation, but basically it's:
//
// [uint16/uint32/uint64:blockLength][int8:encoding]
//

func parseHeader16(head []byte) (size uint, encoding int) {
	size = uint(binary.LittleEndian.Uint16(head))

	// Read next byte which is the block encoding.
	encoding = int(head[2])
	return
}

func parseHeader32(head []byte) (size uint, encoding int) {
	size = uint(binary.LittleEndian.Uint32(head))

	// Read next byte which is the block encoding.
	encoding = int(head[4])
	return
}

func parseHeader64(head []byte) (size uint, encoding int) {
	size = uint(binary.LittleEndian.Uint64(head))

	// Read next byte which is the block encoding.
	encoding = int(head[8])
	return
}
