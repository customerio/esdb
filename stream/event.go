package stream

import (
	"bytes"
	"errors"
	"io"
	"strings"

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

func (e *Event) Next(name, value string) int64 {
	return e.offsets[name+":"+value]
}

func (e *Event) Indexes() map[string]string {
	indexes := make(map[string]string)

	for name, _ := range e.offsets {
		parts := strings.SplitN(name, ":", 2)
		indexes[parts[0]] = parts[1]
	}

	return indexes
}

// Events are encoded in the following byte format:
// [int32:length][bytes(length):data]
func (e *Event) push(buf *bytes.Buffer) (int, error) {
	data := e.encode()
	binary.WriteInt32(buf, len(data))
	buf.Write(data)

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

func pullEvent(r io.ReaderAt, offset int64) (*Event, error) {
	if size := binary.ReadInt32At(r, offset); size > 0 {
		offset += 4

		data := binary.ReadBytesAt(r, size, offset)

		if len(data) < int(size) {
			return nil, CORRUPTED_EVENT
		}

		return decodeEvent(data)
	} else {
		return nil, io.EOF
	}
}
