package binary

import (
	"encoding/binary"
	"io"
)

func ReadBytes(r io.Reader, num int64) []byte {
	bytes := make([]byte, num)
	n, _ := r.Read(bytes)
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

func ReadInt64(r io.Reader) int64 {
	var i int64
	binary.Read(r, binary.LittleEndian, &i)
	return int64(i)
}
