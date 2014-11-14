package binary

import (
	"bytes"
	"encoding/binary"
	"io"
)

func ReadBytes(r io.Reader, num int64) []byte {
	bytes := make([]byte, num)
	n, _ := r.Read(bytes)
	return bytes[:n]
}

func ReadBytesAt(r io.ReaderAt, num int64, offset int64) []byte {
	bytes := make([]byte, num)
	n, _ := r.ReadAt(bytes, offset)
	return bytes[:n]
}

func ReadUvarint(r io.ByteReader) int64 {
	i, _ := binary.ReadUvarint(r)
	return int64(i)
}

func ReadInt16(r io.Reader) int64 {
	var i uint16
	binary.Read(r, binary.LittleEndian, &i)
	return int64(i)
}

func ReadInt32(r io.Reader) int64 {
	var i uint32
	binary.Read(r, binary.LittleEndian, &i)
	return int64(i)
}

func ReadInt32At(r io.ReaderAt, offset int64) int64 {
	b := ReadBytesAt(r, 4, offset)
	buf := bytes.NewBuffer(b)

	var i uint32
	binary.Read(buf, binary.LittleEndian, &i)
	return int64(i)
}

func ReadInt64(r io.Reader) int64 {
	var i int64
	binary.Read(r, binary.LittleEndian, &i)
	return i
}

func ReadInt64At(r io.ReaderAt, offset int64) int64 {
	b := ReadBytesAt(r, 4, offset)
	buf := bytes.NewBuffer(b)

	var i int64
	binary.Read(buf, binary.LittleEndian, &i)
	return i
}
