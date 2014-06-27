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
		block := w.buffer.Next(maxBodyLen(w.blockSize))
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

func blockLen(blockSize int) int {
	return maxBodyLen(blockSize) + headerLen(blockSize)
}

func maxBodyLen(blockSize int) int {
	n := headerLen(blockSize)

	if blockSize > n {
		return blockSize - n
	} else {
		return blockSize
	}
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
		ret = int16(size)
	} else if blockSize <= 4294967296 {
		ret = int32(size)
	} else {
		ret = int64(size)
	}

	return
}

func parseInt(size interface{}) (ret int) {
	if n, ok := size.(int16); ok {
		return int(n)
	} else if n, ok := size.(int32); ok {
		return int(n)
	} else if n, ok := size.(int64); ok {
		return int(n)
	}

	return 0
}
