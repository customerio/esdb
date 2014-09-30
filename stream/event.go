package stream

import (
	"bytes"
	"errors"
	"io"

	"github.com/customerio/esdb/binary"
)

var CORRUPTED_EVENT = errors.New("corrupted event")

type Event struct {
	Data    []byte
	offsets map[string]int64
}

func newEvent(data []byte, offsets map[string]int64) *Event {
	return &Event{Data: data, offsets: offsets}
}

// Events are encoded in the following byte format:
// [Uvarint:length][int32:timestamp][bytes(length):data]
func (e *Event) push(out io.Writer) (int, error) {
	data := e.encode()
	binary.WriteInt32(out, len(data))
	out.Write(data)

	return len(data) + 4, nil
}

func (e *Event) length() int {
	return len(e.encode()) + 4
}

func (e *Event) encode() []byte {
	buf := bytes.NewBuffer([]byte{})

	binary.WriteUvarint(buf, len(e.Data))
	buf.Write(e.Data)

	binary.WriteUvarint(buf, len(e.offsets))

	for name, offset := range e.offsets {
		binary.WriteUvarint(buf, len(name))
		buf.Write([]byte(name))
		binary.WriteUvarint64(buf, offset)
	}

	return buf.Bytes()
}

func decodeEvent(b []byte) (*Event, error) {
	buf := bytes.NewBuffer(b)

	size := binary.ReadUvarint(buf)
	data := binary.ReadBytes(buf, size)

	numOffsets := int(binary.ReadUvarint(buf))

	offsets := make(map[string]int64)

	for i := 0; i < numOffsets; i++ {
		length := binary.ReadUvarint(buf)
		name := string(binary.ReadBytes(buf, length))
		offset := binary.ReadUvarint(buf)

		offsets[name] = offset
	}

	return newEvent(data, offsets), nil
}

func pullEvent(r io.Reader) (*Event, error) {
	if size := binary.ReadInt32(r); size > 0 {
		data := binary.ReadBytes(r, size)

		if len(data) < int(size) {
			return nil, CORRUPTED_EVENT
		}

		return decodeEvent(data)
	} else {
		return nil, io.EOF
	}
}
