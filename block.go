package esdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"
)

type Scanner func(*Event) bool

type Block struct {
	Id []byte

	written bool

	reader      io.ReadSeeker
	offset      int64
	length      int64
	offsets     map[string][]int64
	calcOffsets sync.Once

	writer  io.Writer
	groups  map[string]Events
	indexes map[string]Events
}

func createBlock(writer io.Writer, id []byte) *Block {
	return &Block{
		Id:      id,
		writer:  writer,
		written: false, // to be explicit...
		groups:  make(map[string]Events),
		indexes: make(map[string]Events),
	}
}

func newBlock(reader io.ReadSeeker, id []byte, offset, length int64) *Block {
	return &Block{
		Id:      id,
		reader:  reader,
		offset:  offset,
		length:  length,
		written: true,
	}
}

func (b *Block) Scan(group string, scanner Scanner) error {
	if !b.written {
		return errors.New("Can't scan a block which hasn't been written")
	}

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
	if !b.written {
		return errors.New("Can't scan a block which hasn't been written")
	}

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
	if !b.written {
		return errors.New("Can't scan a block which hasn't been written")
	}

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

func (b *Block) add(timestamp int, data []byte, group string, indexes []string) error {
	if b.written {
		return errors.New("Cannot add to block. We're immutable and this one has already been written.")
	}

	event := newEvent(timestamp, data)

	if b.groups[group] == nil {
		b.groups[group] = make([]*Event, 0)
	}

	b.groups[group] = append(b.groups[group], event)

	for _, name := range indexes {
		if b.indexes[name] == nil {
			b.indexes[name] = make([]*Event, 0)
		}

		b.indexes[name] = append(b.indexes[name], event)
	}

	return nil
}

func (b *Block) write() (err error) {
	if b.written {
		return
	}

	b.written = true

	b.doublyLinkEvents()

	buf := new(bytes.Buffer)

	buf.Write([]byte{42})

	if err := b.writeEvents(buf); err != nil {
		return err
	}
	if err := b.writeIndex(buf); err != nil {
		return err
	}

	b.length, err = buf.WriteTo(b.writer)

	return
}

func (b *Block) doublyLinkEvents() {
	for index, events := range b.indexes {
		sort.Stable(events)

		var prev *Event

		for _, event := range events {
			event.prev[index] = nil
			event.next[index] = nil

			if prev != nil {
				prev.next[index] = event
				event.prev[index] = prev
				event.next[index] = nil
			}

			prev = event
		}
	}
}

func (b *Block) writeEvents(buf io.Writer) error {
	var offset int64

	offset = 1

	for _, events := range b.groups {
		sort.Stable(sort.Reverse(events))

		for _, event := range events {
			event.offset = offset
			offset += event.length()
		}

		offset += 1
	}

	for _, events := range b.groups {
		for _, event := range events {
			if _, err := buf.Write(event.encode()); err != nil {
				return err
			}
		}

		buf.Write([]byte{0})
	}

	return nil
}

func (b *Block) writeIndex(out io.Writer) error {
	buf := new(bytes.Buffer)

	keys := make(sort.StringSlice, 0)
	starts := make(map[string]int64)
	stops := make(map[string]int64)

	record := func(prefix, name string, events Events) {
		if len(events) > 0 {
			keys = append(keys, prefix+name)
			starts[prefix+name] = events[0].offset
			stops[prefix+name] = events[len(events)-1].offset
		}
	}

	for name, events := range b.groups {
		record("g", name, events)
	}
	for name, events := range b.indexes {
		record("i", name, events)
	}

	keys.Sort()

	for _, key := range keys {
		buf.Write(varInt(len(key)))
		buf.Write([]byte(key))
		binary.Write(buf, binary.LittleEndian, starts[key])
		binary.Write(buf, binary.LittleEndian, stops[key])
	}

	binary.Write(buf, binary.LittleEndian, int32(buf.Len()))

	_, err := buf.WriteTo(out)

	return err
}
