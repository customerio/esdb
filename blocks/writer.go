package blocks

import (
	"bytes"
	"encoding/binary"
	"io"
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
	var i int

	for w.buffer.Len() > size {
		block := w.buffer.Next(w.blockSize)

		head := header(w.blockSize, block)

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
	return len(header(blockSize, []byte{}))
}

func header(blockSize int, block []byte) []byte {
	size := fixedInt(blockSize, len(block))

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)

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
