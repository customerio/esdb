package blocks

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
)

// Writer implements the io.Writer interface and is meant to be
// used in place of a io.Writer or bufio.Writer when writing data.
//
// Data is written in blocks or chunks, and optionally
// compressed with snappy compression.
//
// When data is written, if the buffered amount then exceeds the
// configured blockSize, the block is encoded and compressed and
// written to the underlying io.Writer.
//
// Each block can be read using the blocks.Reader object, and
// has the following format:
//
//     [int16/int32/int64:blockLength][int8:encoding][bytes(blockLength):data]
//
// blockLength's type is the smallest fixed length integer size that
// can contain the max configured blockSize.  For instance,
// if blockSize is 4096 bytes, we'll used an uint16. If the blockSize
// is 128 KB, we'll use a uint32, etc.
type Writer struct {
	buffer    *bytes.Buffer
	writer    io.Writer
	Written   int
	Blocks    int
	blockSize int
}

// Tranforms any io.Writer into a block writer using the
// configured max blockSize.
func NewWriter(w io.Writer, blockSize int) *Writer {
	return &Writer{new(bytes.Buffer), w, 0, 0, blockSize}
}

// Implements io.Writer interface.
func (w *Writer) Write(p []byte) (n int, err error) {
	n, err = w.buffer.Write(p)
	if err != nil {
		return
	}

	return w.flush(w.blockSize)
}

// Returns the number of bytes that have been written
// to the blocks.Writer, but haven't yet been
// encoded and written to the underlying io.Writer. This
// should never exceed the configured max blockSize.
func (w *Writer) Buffered() int {
	return w.buffer.Len()
}

// Encodes any remaining buffered data and writes it to
// the underlying io.Writer. Note: you must cause this
// method after finshing writing data, as it's very likely,
// there will be some amount of buffered data left over.
func (w *Writer) Flush() (n int, err error) {
	return w.flush(0)
}

func (w *Writer) flush(size int) (n int, err error) {
	var i int

	// If we have enough buffered bytes, let's compress
	// the data and write it out to the underlying io.Writer.
	for w.buffer.Len() > size {
		block := w.buffer.Next(w.blockSize)
		var encoding = NO_COMPRESSION

		// Data is only encoded if we successfully encode the block.
		// Otherwise the block is identified as uncompressed.
		if encoded := snappy.Encode(nil, block); len(encoded) <= len(block) {
			encoding = SNAPPY_COMPRESSION
			block = encoded
		}

		head := header(w.blockSize, encoding, block)

		i, err = w.writer.Write(head)
		w.Written += i
		n += i

		if err != nil {
			return
		}

		i, err = w.writer.Write(block)
		w.Written += i
		n += i

		if err != nil {
			return
		}

		w.Blocks += 1
	}

	return
}

// The header consists of two numbers.  The length of the
// encoded/compressed block, and the encoding of the block data.
func header(blockSize int, encoding int, block []byte) []byte {
	buf := new(bytes.Buffer)

	// Write the size of the block
	size := fixedInt(blockSize, len(block))
	binary.Write(buf, binary.LittleEndian, size)

	// Write the encoding byte
	buf.Write([]byte{byte(encoding)})

	return buf.Bytes()
}

func headerLen(blockSize int) int {
	return len(header(blockSize, 0, []byte{}))
}

// Since we choose an int16/int32/int64 based
// on the max size of the block (in order to
// ensure we used the same fixed length header
// for every block), this is a bit convoluted.
// Is there a better way?
func fixedInt(blockSize, size int) (ret interface{}) {
	if blockSize < 65536 {
		ret = uint16(size)
	} else if blockSize < 4294967296 {
		ret = uint32(size)
	} else {
		ret = uint64(size)
	}

	return
}
