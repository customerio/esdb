package esdb

import (
	"io"

	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/blocks"
)

type events []*Event

func (e events) Len() int           { return len(e) }
func (e events) Less(i, j int) bool { return e[i].Timestamp < e[j].Timestamp }
func (e events) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

type Event struct {
	Data      []byte
	Timestamp int
	block     int64
	offset    int
}

func newEvent(data []byte, timestamp int) *Event {
	return &Event{data, timestamp, 0, 0}
}

// Events are encoded in the following byte format:
// [Uvarint:length][int32:timestamp][bytes(length):data]
func (e *Event) push(out io.Writer) {
	binary.WriteUvarint(out, len(e.Data))
	binary.WriteInt32(out, e.Timestamp)
	out.Write(e.Data)
	e.Data = nil
}

func pullEvent(r *blocks.Reader) (e *Event) {
	size := binary.ReadUvarint(r)
	timestamp := int(binary.ReadInt32(r))
	data := binary.ReadBytes(r, size)

	if len(data) > 0 {
		e = &Event{Data: data, Timestamp: timestamp}
	}

	return
}
