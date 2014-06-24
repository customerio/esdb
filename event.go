package esdb

import (
	"bytes"
	"encoding/binary"
)

type Events []*Event

func (e Events) Len() int           { return len(e) }
func (e Events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e Events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Event struct {
	Data      []byte
	Timestamp int

	prevOffsets map[string]uint64
	nextOffsets map[string]uint64

	offset int64
	prev   map[string]*Event
	next   map[string]*Event
}

func newEvent(timestamp int, data []byte) *Event {
	return &Event{
		Timestamp: timestamp,
		Data:      data,
		prev:      make(map[string]*Event),
		next:      make(map[string]*Event),
	}
}

func decodeEvent(encoded []byte) *Event {
	if len(encoded) == 0 {
		return nil
	}

	dataLen, index := binary.Uvarint(encoded)

	data := encoded[index : index+int(dataLen)]
	index += int(dataLen)

	event := &Event{Data: data, prevOffsets: make(map[string]uint64), nextOffsets: make(map[string]uint64)}

	var indexLen uint8
	binary.Read(bytes.NewReader(encoded[index:index+1]), binary.LittleEndian, &indexLen)
	index += 1

	for i := 0; i < int(indexLen); i++ {
		nameLen, n := binary.Uvarint(encoded[index:])
		index += int(n)

		name := string(encoded[index : index+int(nameLen)])
		index += int(nameLen)

		var prev uint64
		var next uint64
		binary.Read(bytes.NewReader(encoded[index:index+8]), binary.LittleEndian, &prev)
		index += 8
		binary.Read(bytes.NewReader(encoded[index:index+8]), binary.LittleEndian, &next)
		index += 8

		event.prevOffsets[name] = prev
		event.nextOffsets[name] = next
	}

	return event
}

func (e *Event) encode() []byte {
	buf := new(bytes.Buffer)

	e.encodeData(buf)
	e.encodeLinks(buf)

	return append(varInt(buf.Len()), buf.Bytes()...)
}

func (e *Event) length() int64 {
	return int64(len(e.encode()))
}

func (e *Event) encodeData(buf *bytes.Buffer) {
	buf.Write(varInt(len(e.Data)))
	buf.Write(e.Data)
}

func (e *Event) encodeLinks(buf *bytes.Buffer) {
	numLinks := uint8(len(e.prev))

	binary.Write(buf, binary.LittleEndian, numLinks)

	for index, _ := range e.prev {
		buf.Write(varInt(len(index)))
		buf.Write([]byte(index))

		var prev int64
		var next int64

		if e.prev[index] != nil {
			prev = e.prev[index].offset
		}

		if e.next[index] != nil {
			next = e.next[index].offset
		}

		binary.Write(buf, binary.LittleEndian, prev)
		binary.Write(buf, binary.LittleEndian, next)
	}
}

func varInt(n int) []byte {
	bytes := make([]byte, 64)
	written := binary.PutUvarint(bytes, uint64(n))
	return bytes[:written]
}
