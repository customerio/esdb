package esdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/customerio/esdb/sst"
)

type spaceWriter struct {
	Id []byte

	writer io.Writer

	written bool

	groupings map[string]events
	indexes   map[string]events
}

func newSpace(writer io.Writer, id []byte) *spaceWriter {
	return &spaceWriter{
		Id:        id,
		writer:    writer,
		groupings: make(map[string]events),
		indexes:   make(map[string]events),
	}
}

func (w *spaceWriter) add(data []byte, timestamp int, grouping string, indexes map[string]string) error {
	if w.written {
		return errors.New("Cannot add to space. We're immutable and this one has already been written.")
	}

	event := newEvent(timestamp, data)

	if w.groupings[grouping] == nil {
		w.groupings[grouping] = make([]*Event, 0)
	}

	w.groupings[grouping] = append(w.groupings[grouping], event)

	for name, value := range indexes {
		serialized := name + ":" + value

		if w.indexes[serialized] == nil {
			w.indexes[serialized] = make([]*Event, 0)
		}

		w.indexes[serialized] = append(w.indexes[serialized], event)
	}

	return nil
}

func (w *spaceWriter) write() (int64, error) {
	if w.written {
		return 0, nil
	}

	w.written = true

	buf := bytes.NewBuffer([]byte{42})

	if err := w.writeEvents(uint64(buf.Len()), buf); err != nil {
		return 0, err
	}
	if err := w.writeIndex(uint64(buf.Len()), buf); err != nil {
		return 0, err
	}

	return buf.WriteTo(w.writer)
}

func (w *spaceWriter) writeEvents(offset uint64, writer io.Writer) error {
	groupings := make(sort.StringSlice, 0, len(w.groupings))

	for grouping, _ := range w.groupings {
		groupings = append(groupings, grouping)
	}

	buf := newWriteBuffer([]byte{})

	groupings.Sort()

	for _, grouping := range groupings {
		events := w.groupings[grouping]

		sort.Stable(sort.Reverse(events))

		for _, event := range events {
			event.offset = offset + uint64(buf.Len())
			event.push(buf)
		}

		buf.Push([]byte{0})
	}

	_, err := buf.WriteTo(writer)

	return err
}

func (w *spaceWriter) writeIndex(offset uint64, out io.Writer) error {
	buf := new(bytes.Buffer)

	groupings := make(sort.StringSlice, 0)
	indexes := make(sort.StringSlice, 0)
	offsets := make(map[string]uint64)
	lengths := make(map[string]uint64)

	scratch := make([]byte, 8)

	for name, _ := range w.groupings {
		groupings = append(groupings, name)
	}

	for name, events := range w.indexes {
		indexes = append(indexes, name)

		sort.Stable(events)

		offsets[name] = offset

		for _, event := range events {
			binary.Write(buf, binary.LittleEndian, uint64(event.offset))
			lengths[name] += 8
		}

		offset += lengths[name]
	}

	if _, err := buf.WriteTo(out); err != nil {
		return err
	}

	groupings.Sort()
	indexes.Sort()

	buf = new(bytes.Buffer)
	st := sst.NewWriter(buf)

	for _, name := range groupings {
		events := w.groupings[name]

		b := new(bytes.Buffer)

		n := binary.PutUvarint(scratch, events[0].offset)
		b.Write(scratch[:n])

		if err := st.Set([]byte("g"+name), b.Bytes()); err != nil {
			return err
		}
	}

	for _, name := range indexes {
		b := new(bytes.Buffer)

		n := binary.PutUvarint(scratch, offsets[name])
		b.Write(scratch[:n])
		n = binary.PutUvarint(scratch, lengths[name])
		b.Write(scratch[:n])

		if err := st.Set([]byte("i"+name), b.Bytes()); err != nil {
			return err
		}
	}

	if err := st.Close(); err != nil {
		return err
	}

	binary.Write(buf, binary.LittleEndian, int32(buf.Len()))

	_, err := buf.WriteTo(out)
	return err
}
