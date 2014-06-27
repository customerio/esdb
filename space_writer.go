package esdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/customerio/esdb/sst"
)

type spaceWriter struct {
	Id []byte

	writer io.Writer

	written bool

	groupings map[string]*index
	indexes   map[string]*index
}

type index struct {
	offset int
	length int
	evs    events
}

func newSpace(writer io.Writer, id []byte) *spaceWriter {
	return &spaceWriter{
		Id:      id,
		writer:  writer,
		indexes: make(map[string]*index),
	}
}

func (w *spaceWriter) add(event *Event, grouping string, indexes map[string]string) error {
	if w.written {
		return errors.New("Cannot add to space. We're immutable and this one has already been written.")
	}

	addEventToIndex(w.indexes, "g"+grouping, event)

	for name, val := range indexes {
		addEventToIndex(w.indexes, "i"+name+":"+val, event)
	}

	return nil
}

func (w *spaceWriter) write() (int64, error) {
	if w.written {
		return 0, nil
	}

	buf := bytes.NewBuffer([]byte{42})

	for _, name := range sortIndexes(w.indexes) {
		w.indexes[name].offset = buf.Len()

		if strings.HasPrefix(name, "g") {
			writeEventBlocks(w.indexes[name], buf)
		} else {
			writeIndexBlocks(w.indexes[name], buf)
		}
	}

	if err := w.writeIndex(buf); err != nil {
		return 0, err
	}

	w.written = true

	return buf.WriteTo(w.writer)
}

func (w *spaceWriter) writeIndex(out io.Writer) error {
	buf := new(bytes.Buffer)
	st := sst.NewWriter(buf)

	for _, name := range sortIndexes(w.indexes) {
		buf := new(bytes.Buffer)

		b := make([]byte, 8)
		n := binary.PutUvarint(b, uint64(w.indexes[name].offset))
		buf.Write(b[:n])

		b = make([]byte, 8)
		n = binary.PutUvarint(b, uint64(w.indexes[name].length))
		buf.Write(b[:n])

		if err := st.Set([]byte(name), buf.Bytes()); err != nil {
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

func addEventToIndex(indexes map[string]*index, name string, event *Event) {
	if indexes[name] == nil {
		indexes[name] = &index{evs: make(events, 0, 1)}
	}

	indexes[name].evs = append(indexes[name].evs, event)
}

func sortIndexes(indexes map[string]*index) []string {
	names := make(sort.StringSlice, 0, len(indexes))

	for name, _ := range indexes {
		names = append(names, name)
	}

	sort.Stable(names)

	return names
}
