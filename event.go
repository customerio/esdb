package esdb

import (
	"bytes"

	"github.com/customerio/esdb/blocks"
)

type events []*Event

func (e events) Len() int           { return len(e) }
func (e events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Event struct {
	Data      []byte
	Timestamp int
	block     int
	offset    int
}

func newEvent(data []byte, timestamp int) *Event {
	return &Event{data, timestamp, 0, 0}
}

func (e *Event) push(buf *bytes.Buffer) {
	writeUvarint(buf, len(e.Data))
	buf.Write(e.Data)
}

func pullEvent(r *blocks.Reader) (e *Event) {
	size := readUvarint(r)
	data := readBytes(r, size)

	if len(data) > 0 {
		e = &Event{Data: data}
	}

	return
}
