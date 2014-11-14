package binary

import (
	"bytes"
	"encoding/binary"
	"io"
)

func WriteUvarint(w io.Writer, num int) {
	b := make([]byte, 8)
	n := binary.PutUvarint(b, uint64(num))
	w.Write(b[:n])
}

func WriteUvarint64(w io.Writer, num int64) {
	b := make([]byte, 8)
	n := binary.PutUvarint(b, uint64(num))
	w.Write(b[:n])
}

func WriteInt16(w io.Writer, num int) {
	binary.Write(w, binary.LittleEndian, uint16(num))
}

func WriteInt32(w io.Writer, num int) {
	binary.Write(w, binary.LittleEndian, uint32(num))
}

func WriteInt32At(w io.WriterAt, num int, offset int64) {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, uint32(num))
	w.WriteAt(buf.Bytes()[:4], offset)
}

func WriteInt64(w io.Writer, num int64) {
	binary.Write(w, binary.LittleEndian, int64(num))
}
