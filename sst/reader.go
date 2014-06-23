package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
)

type blockHandle struct {
	offset int64
	length int64
}

type Reader struct {
	reader io.ReadSeeker
	length int64
	index  []byte
}

func NewReader(r io.ReadSeeker, length int64) (*Reader, error) {
	footer := make([]byte, FOOTER_SIZE)

	r.Seek(length-int64(FOOTER_SIZE), 0)
	_, err := r.Read(footer)
	if err != nil {
		return nil, err
	}

	if string(footer[FOOTER_SIZE-len(MAGIC):FOOTER_SIZE]) != MAGIC {
		return nil, errors.New("invalid sst format")
	}

	_, n := decodeBlockHandle(footer[:])
	indexBlockHandle, n := decodeBlockHandle(footer[n:])

	reader := &Reader{
		reader: r,
		length: length,
	}

	reader.index, err = reader.readBlock(indexBlockHandle)

	return reader, err
}

func (r *Reader) Get(key []byte) (value []byte, err error) {
	iter, err := r.Find(key)
	if err != nil {
		return nil, err
	}

	if !iter.Next() || string(key) != string(iter.Key()) {
		if err = iter.Close(); err == nil {
			err = errors.New("not found")
		}

		return
	}

	return iter.Value(), iter.Close()
}

func (r *Reader) Find(key []byte) (Iterator, error) {
	index, err := seek(r.index, key)

	if err != nil {
		return nil, err
	}

	iter := &tableIterator{
		reader: r,
		index:  index,
	}

	iter.nextBlock(key)

	return iter, nil
}

func (r *Reader) readBlock(handle blockHandle) ([]byte, error) {
	bytes := make([]byte, handle.length)

	r.reader.Seek(handle.offset, 0)
	if _, err := r.reader.Read(bytes); err != nil {
		return nil, err
	}

	return bytes, nil
}

func seek(data []byte, key []byte) (*iterator, error) {
	numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))

	handleStart := len(data) - 4*numRestarts - 4

	var offset int

	if len(key) > 0 {
		index := sort.Search(numRestarts, func(i int) bool {
			offset := int(binary.LittleEndian.Uint32(data[handleStart+4*i:])) + 1
			// Decode the key at that restart point, and compare it to the key sought.
			keyLen, n := binary.Uvarint(data[offset:])
			_, n1 := binary.Uvarint(data[offset+n:])
			keyOffset := offset + n + n1
			resetKey := data[keyOffset : keyOffset+int(keyLen)]
			return bytes.Compare(resetKey, key) > 0
		})

		if index > 0 {
			offset = int(binary.LittleEndian.Uint32(data[handleStart+4*(index-1):]))
		}
	}

	iter := &iterator{
		data: data[offset:handleStart],
		key:  make([]byte, 0, 256),
	}

	for iter.Next() && bytes.Compare(iter.key, key) < 0 {
	}

	if iter.err != nil {
		return nil, iter.err
	}

	iter.soi = !iter.eoi
	return iter, nil
}

func decodeBlockHandle(src []byte) (blockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	return blockHandle{int64(offset), int64(length)}, n + m
}

func encodeBlockHandle(dst []byte, b blockHandle) int {
	n := binary.PutUvarint(dst, uint64(b.offset))
	m := binary.PutUvarint(dst[n:], uint64(b.length))
	return n + m
}
