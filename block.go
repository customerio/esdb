package esdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

type Scanner func(*Event) bool

type Block struct {
	Id []byte

	reader      io.ReadSeeker
	offset      int64
	length      int64
	offsets     map[string][]int64
	calcOffsets sync.Once
}

func block(reader io.ReadSeeker, id []byte, offset, length int64) *Block {
	return &Block{
		Id:     id,
		reader: reader,
		offset: offset,
		length: length,
	}
}

func (b *Block) Scan(group string, scanner Scanner) error {
	offset := b.offset + b.findIndexOffsets("g" + group)[0]

	b.reader.Seek(offset, 0)
	data := eventData(b.reader)
	offset += int64(len(data) + 1)

	for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
		scanner(event)

		b.reader.Seek(offset, 0)
		data = eventData(b.reader)
		offset += int64(len(data) + 1)
	}

	return nil
}

func eventData(reader io.ReadSeeker) []byte {
	data := make([]byte, 1000)
	reader.Read(data)

	eventLen, n := binary.Uvarint(data)

	if int(eventLen) > 1000-n {
		extra := make([]byte, int(eventLen)-1000+n)
		reader.Read(extra)
		data = append(data, extra...)
	}

	data = data[n : eventLen+1]

	return data
}

func (b *Block) ScanIndex(index string, scanner Scanner) error {
	if offsets := b.findIndexOffsets("i" + index); offsets != nil {
		offset := b.offset + offsets[0]

		b.reader.Seek(offset, 0)

		data := eventData(b.reader)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			scanner(event)

			if next := event.nextOffsets[index]; next > 0 {
				offset = b.offset + next
				b.reader.Seek(offset, 0)
				data = eventData(b.reader)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) RevScanIndex(index string, scanner Scanner) error {
	if offsets := b.findIndexOffsets("i" + index); offsets != nil {
		offset := b.offset + offsets[1]

		b.reader.Seek(offset, 0)
		data := eventData(b.reader)

		for event := decodeEvent(data); event != nil; event = decodeEvent(data) {
			scanner(event)

			if prev := event.prevOffsets[index]; prev > 0 {
				offset = b.offset + prev
				b.reader.Seek(offset, 0)
				data = eventData(b.reader)
			} else {
				data = []byte{}
			}
		}
	}

	return nil
}

func (b *Block) findIndexOffsets(index string) []int64 {
	if b.offsets == nil {
		b.calcOffsets.Do(func() {
			b.reader.Seek(b.offset+b.length-4, 0)

			var indexLength int32
			binary.Read(b.reader, binary.LittleEndian, &indexLength)

			b.reader.Seek(b.offset+b.length-4-int64(indexLength), 0)

			indexes := make([]byte, indexLength)
			b.reader.Read(indexes)

			b.offsets = make(map[string][]int64)

			for index := 0; index < len(indexes); {
				nameLen, n := binary.Uvarint(indexes[index:])
				index += n

				name := string(indexes[index : index+int(nameLen)])
				index += int(nameLen)

				var startOffset int64
				binary.Read(bytes.NewReader(indexes[index:index+8]), binary.LittleEndian, &startOffset)
				index += 8

				var stopOffset int64
				binary.Read(bytes.NewReader(indexes[index:index+8]), binary.LittleEndian, &stopOffset)
				index += 8

				b.offsets[name] = []int64{startOffset, stopOffset}
			}
		})
	}

	return b.offsets[index]
}
