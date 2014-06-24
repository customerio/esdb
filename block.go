package esdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/customerio/esdb/sst"
)

type Scanner func(*Event) bool

type Block struct {
	Id []byte

	reader      io.ReadSeeker
	offset      int64
	length      int64
	index       *sst.Reader
	offsets     map[string][]int64
	calcOffsets sync.Once
}

func block(reader io.ReadSeeker, id []byte, offset, length int64) *Block {
	st, err := findBlockIndex(reader, offset, length)
	if err != nil {
		return nil
	}

	return &Block{
		Id:     id,
		reader: reader,
		index:  st,
		offset: offset,
		length: length,
	}
}

func (b *Block) Scan(group string, scanner Scanner) error {
	if off := b.firstIndexOffset("g" + group); off > 0 {
		offset := b.offset + off

		b.reader.Seek(offset, 0)
		data := make([]byte, 100)
		b.reader.Read(data)

		b.reader.Seek(offset, 0)
		data, off := eventData(b.reader)
		offset += off

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			b.reader.Seek(offset, 0)
			data, off = eventData(b.reader)
			offset += off
		}
	}

	return nil
}

func eventData(reader io.ReadSeeker) ([]byte, int64) {
	data := make([]byte, 1000)
	reader.Read(data)

	eventLen, n := binary.Uvarint(data)

	if int(eventLen) > 1000-n {
		extra := make([]byte, int(eventLen)-1000+n)
		reader.Read(extra)
		data = append(data, extra...)
	}

	data = data[n : uint64(n)+eventLen]

	return data, int64(n + len(data))
}

func (b *Block) ScanIndex(index string, scanner Scanner) error {
	if off := b.firstIndexOffset("i" + index); off > 0 {
		offset := b.offset + off

		b.reader.Seek(offset, 0)

		data, _ := eventData(b.reader)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			if next := event.nextOffsets[index]; next > 0 {
				offset = b.offset + next
				b.reader.Seek(offset, 0)
				data, _ = eventData(b.reader)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) RevScanIndex(index string, scanner Scanner) error {
	if off := b.lastIndexOffset("i" + index); off > 0 {
		offset := b.offset + off

		b.reader.Seek(offset, 0)
		data, _ := eventData(b.reader)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			if !scanner(event) {
				return nil
			}

			if prev := event.prevOffsets[index]; prev > 0 {
				offset = b.offset + prev
				b.reader.Seek(offset, 0)
				data, _ = eventData(b.reader)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) firstIndexOffset(index string) int64 {
	return b.findIndexOffsets(index)[0]
}

func (b *Block) lastIndexOffset(index string) int64 {
	return b.findIndexOffsets(index)[1]
}

func (b *Block) findIndexOffsets(index string) []int64 {
	if offsets, err := b.index.Get([]byte(index)); err == nil {
		var startOffset int64
		binary.Read(bytes.NewReader(offsets[:8]), binary.LittleEndian, &startOffset)

		var stopOffset int64
		binary.Read(bytes.NewReader(offsets[8:]), binary.LittleEndian, &stopOffset)

		return []int64{startOffset, stopOffset}
	}

	return []int64{0, 0}
}

func findBlockIndex(r io.ReadSeeker, offset, length int64) (*sst.Reader, error) {
	r.Seek(offset+length-4, 0)

	var indexLength int32
	binary.Read(r, binary.LittleEndian, &indexLength)

	r.Seek(offset+length-4-int64(indexLength), 0)
	index := make([]byte, indexLength)
	r.Read(index)

	return sst.NewReader(bytes.NewReader(index), int64(indexLength))
}
