package esdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/customerio/esdb/sst"
)

type blockWriter struct {
	Id []byte

	writer io.Writer

	written bool

	groups  map[string]Events
	indexes map[string]Events
}

func newBlock(writer io.Writer, id []byte) *blockWriter {
	return &blockWriter{
		Id:      id,
		writer:  writer,
		written: false, // to be explicit...
		groups:  make(map[string]Events),
		indexes: make(map[string]Events),
	}
}

func (w *blockWriter) add(timestamp int, data []byte, group string, indexes []string) error {
	if w.written {
		return errors.New("Cannot add to block. We're immutable and this one has already been written.")
	}

	event := newEvent(timestamp, data)

	if w.groups[group] == nil {
		w.groups[group] = make([]*Event, 0)
	}

	w.groups[group] = append(w.groups[group], event)

	for _, name := range indexes {
		if w.indexes[name] == nil {
			w.indexes[name] = make([]*Event, 0)
		}

		w.indexes[name] = append(w.indexes[name], event)
	}

	return nil
}

func (w *blockWriter) write() (int64, error) {
	if w.written {
		return 0, nil
	}

	w.written = true

	w.doublyLinkEvents()

	buf := new(bytes.Buffer)

	buf.Write([]byte{42})

	if err := w.writeEvents(buf); err != nil {
		return 0, err
	}
	if err := w.writeIndex(buf); err != nil {
		return 0, err
	}

	return buf.WriteTo(w.writer)
}

func (w *blockWriter) doublyLinkEvents() {
	for index, events := range w.indexes {
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

func (w *blockWriter) writeEvents(buf io.Writer) error {
	var offset int64

	offset = 1

	groups := make(sort.StringSlice, 0, len(w.groups))

	for group, _ := range w.groups {
		groups = append(groups, group)
	}

	groups.Sort()

	for _, group := range groups {
		events := w.groups[group]

		sort.Stable(sort.Reverse(events))

		for _, event := range events {
			event.offset = offset
			offset += event.length()
		}

		offset += 1
	}

	for _, group := range groups {
		events := w.groups[group]

		for _, event := range events {
			if _, err := buf.Write(event.encode()); err != nil {
				return err
			}
		}

		buf.Write([]byte{0})
	}

	return nil
}

func (w *blockWriter) writeIndex(out io.Writer) error {
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

	for name, events := range w.groups {
		record("g", name, events)
	}
	for name, events := range w.indexes {
		record("i", name, events)
	}

	keys.Sort()

	st := sst.NewWriter(buf)

	for _, key := range keys {
		b := new(bytes.Buffer)

		binary.Write(b, binary.LittleEndian, starts[key])
		binary.Write(b, binary.LittleEndian, stops[key])

		if err := st.Set([]byte(key), b.Bytes()); err != nil {
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
