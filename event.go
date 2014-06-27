package esdb

import (
	"bytes"
	"encoding/binary"

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

func pullEvent(r *blocks.Reader) (e *Event) {
	size, _ := binary.ReadUvarint(r)

	data := make([]byte, size)
	n, _ := r.Read(data)

	if len(data[:n]) > 0 {
		e = &Event{Data: data[:n]}
	}

	return
}

func (e *Event) push(buf *bytes.Buffer) {
	b := make([]byte, 8)
	n := binary.PutUvarint(b, uint64(len(e.Data)))
	buf.Write(append(b[:n], e.Data...))
}
