package esdb

import (
	"encoding/binary"
	"io"
)

func readBytes(r io.Reader, num int64) []byte {
	bytes := make([]byte, num)
	n, _ := r.Read(bytes)
	return bytes[:n]
}

func readUvarint(r io.ByteReader) int64 {
	i, _ := binary.ReadUvarint(r)
	return int64(i)
}

func readInt16(r io.Reader) int64 {
	var i uint16
	binary.Read(r, binary.LittleEndian, &i)
	return int64(i)
}

func readInt32(r io.Reader) int64 {
	var i uint32
	binary.Read(r, binary.LittleEndian, &i)
	return int64(i)
}

func readInt64(r io.Reader) int64 {
	var i int64
	binary.Read(r, binary.LittleEndian, &i)
	return int64(i)
}

func writeUvarint(w io.Writer, num int) {
	b := make([]byte, 8)
	n := binary.PutUvarint(b, uint64(num))
	w.Write(b[:n])
}

func writeInt16(w io.Writer, num int) {
	binary.Write(w, binary.LittleEndian, uint16(num))
}

func writeInt32(w io.Writer, num int) {
	binary.Write(w, binary.LittleEndian, uint32(num))
}

func writeInt64(w io.Writer, num int) {
	binary.Write(w, binary.LittleEndian, int64(num))
}
