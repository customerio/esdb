package sst

import (
	"encoding/binary"
	"errors"
)

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}

type iterator struct {
	data     []byte
	key, val []byte
	err      error
	soi, eoi bool
}

type tableIterator struct {
	reader *Reader
	data   *iterator
	index  *iterator
	err    error
}

func (i *iterator) Next() bool {
	if i.eoi || i.err != nil {
		return false
	}
	if i.soi {
		i.soi = false
		return true
	}
	if len(i.data) == 0 {
		i.Close()
		return false
	}
	v0, n0 := binary.Uvarint(i.data)
	v1, n1 := binary.Uvarint(i.data[n0:])
	v2, n2 := binary.Uvarint(i.data[n0+n1:])
	n := n0 + n1 + n2
	i.key = append(i.key[:v0], i.data[n:n+int(v1)]...)
	i.val = i.data[n+int(v1) : n+int(v1+v2)]
	i.data = i.data[n+int(v1+v2):]
	return true
}

func (i *iterator) Key() []byte {
	if i.soi {
		return nil
	}
	return i.key[:len(i.key):len(i.key)]
}

func (i *iterator) Value() []byte {
	if i.soi {
		return nil
	}
	return i.val[:len(i.val):len(i.val)]
}

func (i *iterator) Close() error {
	i.key = nil
	i.val = nil
	i.eoi = true
	return i.err
}

func (i *tableIterator) nextBlock(key []byte) bool {
	if !i.index.Next() {
		i.err = i.index.err
		return false
	}
	// Load the next block.
	v := i.index.Value()
	h, n := decodeBlockHandle(v)
	if n == 0 || n != len(v) {
		i.err = errors.New("leveldb/table: corrupt index entry")
		return false
	}
	k, err := i.reader.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}
	// Look for the key inside that block.
	data, err := seek(k, key)
	if err != nil {
		i.err = err
		return false
	}
	i.data = data
	return true
}

func (i *tableIterator) Next() bool {
	if i.data == nil {
		return false
	}
	for {
		if i.data.Next() {
			return true
		}
		if i.data.err != nil {
			i.err = i.data.err
			break
		}
		if !i.nextBlock(nil) {
			break
		}
	}
	i.Close()
	return false
}

func (i *tableIterator) Key() []byte {
	if i.data == nil {
		return nil
	}
	return i.data.Key()
}

func (i *tableIterator) Value() []byte {
	if i.data == nil {
		return nil
	}
	return i.data.Value()
}

func (i *tableIterator) Close() error {
	i.data = nil
	return i.err
}
