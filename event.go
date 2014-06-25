package esdb

import (
	"bytes"
	"encoding/binary"
)

type events []*Event

func (e events) Len() int           { return len(e) }
func (e events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Event struct {
	Data      []byte
	Timestamp int
	offset    uint64
}

func newEvent(timestamp int, data []byte) *Event {
	return &Event{data, timestamp, 0}
}

func nextEvent(buf *buffer) *Event {
	bytes := buf.Pull(int(buf.PullUvarint()))
	return decodeEvent(bytes)
}

func decodeEvent(encoded []byte) *Event {
	if len(encoded) == 0 {
		return nil
	}

	dataLen, n := binary.Uvarint(encoded)

	data := encoded[n : n+int(dataLen)]
	n += int(dataLen)

	return &Event{Data: data}
}

func (e *Event) encode() []byte {
	buf := new(bytes.Buffer)

	buf.Write(varInt(len(e.Data)))
	buf.Write(e.Data)

	return append(varInt(buf.Len()), buf.Bytes()...)
}

func (e *Event) length() uint64 {
	return uint64(len(e.encode()))
}

func varInt(n int) []byte {
	bytes := make([]byte, 8)
	written := binary.PutUvarint(bytes, uint64(n))
	return bytes[:written]
}
