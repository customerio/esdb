package blocks

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/dgryski/go-csnappy"
)

type Writer struct {
	buffer    *bytes.Buffer
	writer    io.Writer
	Written   int
	Blocks    int
	blockSize int
}

func NewWriter(w io.Writer, blockSize int) *Writer {
	return &Writer{new(bytes.Buffer), w, 0, 0, blockSize}
}

func (w *Writer) Write(p []byte) (n int, err error) {
	n, err = w.buffer.Write(p)
	if err != nil {
		return
	}

	return w.flush(w.blockSize)
}

func (w *Writer) Buffered() int {
	return w.buffer.Len()
}

func (w *Writer) Flush() (n int, err error) {
	return w.flush(0)
}

func (w *Writer) flush(size int) (n int, err error) {
	var encoding = NO_COMPRESSION
	var i int

	for w.buffer.Len() > size {
		block := w.buffer.Next(w.blockSize)

		if encoded, err := csnappy.Encode(nil, block); err == nil && len(encoded) <= len(block) {
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

func headerLen(blockSize int) int {
	return len(header(blockSize, 0, []byte{}))
}

func header(blockSize int, encoding int, block []byte) []byte {
	buf := new(bytes.Buffer)

	size := fixedInt(blockSize, len(block))
	binary.Write(buf, binary.LittleEndian, size)
	buf.Write([]byte{byte(encoding)})

	return buf.Bytes()
}

func fixedInt(blockSize, size int) (ret interface{}) {
	if blockSize <= 65536 {
		ret = uint16(size)
	} else if blockSize <= 4294967296 {
		ret = uint32(size)
	} else {
		ret = uint64(size)
	}

	return
}

func parseInt(size interface{}) (ret uint) {
	if n, ok := size.(uint16); ok {
		return uint(n)
	} else if n, ok := size.(uint32); ok {
		return uint(n)
	} else if n, ok := size.(uint64); ok {
		return uint(n)
	}

	return 0
}
