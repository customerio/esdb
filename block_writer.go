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

	groupings eventsMap
	indexes   eventsMap
	indexKeys map[string]int
}

func newBlock(writer io.Writer, id []byte) *blockWriter {
	return &blockWriter{
		Id:        id,
		writer:    writer,
		groupings: make(eventsMap),
		indexes:   make(eventsMap),
	}
}

func (w *blockWriter) add(data []byte, timestamp int, grouping string, indexes map[string]string) error {
	if w.written {
		return errors.New("Cannot add to block. We're immutable and this one has already been written.")
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

func (w *blockWriter) write() (int64, error) {
	if w.written {
		return 0, nil
	}

	w.written = true

	w.generateIndexKeys()
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

func (w *blockWriter) generateIndexKeys() {
	w.indexKeys = make(map[string]int)

	for i, name := range w.indexes.keysSortedByEventCount() {
		w.indexKeys["i"+name] = i + 1
	}

	for i, name := range w.groupings.keysSortedByEventCount() {
		w.indexKeys["g"+name] = i + 1
	}

}

func (w *blockWriter) doublyLinkEvents() {
	for name, events := range w.indexes {
		sort.Stable(events)

		var prev *Event

		key := w.indexKeys["i"+name]

		for _, event := range events {
			event.prev[key] = nil
			event.next[key] = nil

			if prev != nil {
				prev.next[key] = event
				event.prev[key] = prev
				event.next[key] = nil
			}

			prev = event
		}
	}
}

func (w *blockWriter) writeEvents(buf io.Writer) error {
	var offset int64

	offset = 1

	groupings := make(sort.StringSlice, 0, len(w.groupings))

	for grouping, _ := range w.groupings {
		groupings = append(groupings, grouping)
	}

	groupings.Sort()

	for _, grouping := range groupings {
		events := w.groupings[grouping]

		sort.Stable(sort.Reverse(events))

		for _, event := range events {
			event.offset = offset
			offset += event.length()
		}

		offset += 1
	}

	for _, grouping := range groupings {
		events := w.groupings[grouping]

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

	names := make(sort.StringSlice, 0)
	keys := make(map[string]int)
	firsts := make(map[string]int64)
	lasts := make(map[string]int64)

	record := func(prefix, name string, es events) {
		if len(es) > 0 {
			names = append(names, prefix+name)
			keys[prefix+name] = w.indexKeys[prefix+name]
			firsts[prefix+name] = es[0].offset
			lasts[prefix+name] = es[len(es)-1].offset
		}
	}

	for name, events := range w.groupings {
		record("g", name, events)
	}
	for name, events := range w.indexes {
		record("i", name, events)
	}

	names.Sort()

	st := sst.NewWriter(buf)

	for _, name := range names {
		b := new(bytes.Buffer)

		key := make([]byte, 8)
		written := binary.PutUvarint(key, uint64(keys[name]))
		b.Write(key[:written])

		binary.Write(b, binary.LittleEndian, firsts[name])
		binary.Write(b, binary.LittleEndian, lasts[name])

		if err := st.Set([]byte(name), b.Bytes()); err != nil {
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
