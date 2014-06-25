package esdb

import (
	"bytes"
	"encoding/binary"
	"sort"
)

type pairs []pair
type pair struct {
	key    string
	length int
}

func (p pairs) Len() int           { return len(p) }
func (p pairs) Less(i, j int) bool { return p[i].length < p[j].length }
func (p pairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type events []*Event

func (e events) Len() int           { return len(e) }
func (e events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type eventsMap map[string]events

func (m eventsMap) keysSortedByEventCount() []string {
	p := make(pairs, 0, len(m))

	for k, v := range m {
		p = append(p, pair{k, len(v)})
	}

	sort.Sort(p)

	keys := make([]string, len(m))

	for i, pair := range p {
		keys[i] = pair.key
	}

	return keys
}

type Event struct {
	Data      []byte
	Timestamp int

	prevOffsets map[int]uint64
	nextOffsets map[int]uint64

	offset int64
	prev   map[int]*Event
	next   map[int]*Event
}

func newEvent(timestamp int, data []byte) *Event {
	return &Event{
		Timestamp: timestamp,
		Data:      data,
		prev:      make(map[int]*Event),
		next:      make(map[int]*Event),
	}
}

func decodeEvent(encoded []byte) *Event {
	if len(encoded) == 0 {
		return nil
	}

	dataLen, index := binary.Uvarint(encoded)

	data := encoded[index : index+int(dataLen)]
	index += int(dataLen)

	event := &Event{Data: data, prevOffsets: make(map[int]uint64), nextOffsets: make(map[int]uint64)}

	var indexLen uint8
	binary.Read(bytes.NewReader(encoded[index:index+1]), binary.LittleEndian, &indexLen)
	index += 1

	for i := 0; i < int(indexLen); i++ {
		key, n := binary.Uvarint(encoded[index:])
		index += int(n)

		var prev uint64
		var next uint64
		binary.Read(bytes.NewReader(encoded[index:index+8]), binary.LittleEndian, &prev)
		index += 8
		binary.Read(bytes.NewReader(encoded[index:index+8]), binary.LittleEndian, &next)
		index += 8

		event.prevOffsets[int(key)] = prev
		event.nextOffsets[int(key)] = next
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

	for key, _ := range e.prev {
		buf.Write(varInt(key))

		var prev int64
		var next int64

		if e.prev[key] != nil {
			prev = e.prev[key].offset
		}

		if e.next[key] != nil {
			next = e.next[key].offset
		}

		binary.Write(buf, binary.LittleEndian, prev)
		binary.Write(buf, binary.LittleEndian, next)
	}
}

func varInt(n int) []byte {
	bytes := make([]byte, 8)
	written := binary.PutUvarint(bytes, uint64(n))
	return bytes[:written]
}
