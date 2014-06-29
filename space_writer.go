package esdb

import (
	"bytes"
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

	indexes    map[string]*index
	indexNames sort.StringSlice
}

type index struct {
	offset int
	length int
	evs    events
}

func newSpace(writer io.Writer, id []byte) *spaceWriter {
	return &spaceWriter{
		Id:         id,
		writer:     writer,
		indexes:    make(map[string]*index),
		indexNames: make(sort.StringSlice, 0),
	}
}

func (w *spaceWriter) add(event *Event, grouping string, indexes map[string]string) error {
	if w.written {
		return errors.New("Cannot add to space. We're immutable and this one has already been written.")
	}

	w.addEventToIndex("g"+grouping, event)

	for name, val := range indexes {
		w.addEventToIndex("i"+name+":"+val, event)
	}

	return nil
}

func (w *spaceWriter) addEventToIndex(name string, event *Event) {
	if w.indexes[name] == nil {
		w.indexes[name] = &index{evs: make(events, 0, 1)}
		w.indexNames = append(w.indexNames, name)
	}

	w.indexes[name].evs = append(w.indexes[name].evs, event)
}

func (w *spaceWriter) write() (length int64, err error) {
	if w.written {
		return 0, nil
	}

	buf := new(bytes.Buffer)

	w.writeHeader(buf)
	w.writeBlocks(buf)
	if length, err = w.writeIndex(buf); err != nil {
		return
	}
	w.writeFooter(buf, length)

	w.written = true

	return buf.WriteTo(w.writer)
}

func (w *spaceWriter) writeHeader(out *bytes.Buffer) {
	// Magic character marking this as
	// the start of a space section.
	out.Write([]byte{42})
}

func (w *spaceWriter) writeBlocks(out *bytes.Buffer) {
	sort.Stable(w.indexNames)

	for _, name := range w.indexNames {
		w.indexes[name].offset = out.Len()

		if strings.HasPrefix(name, "g") {
			writeEventBlocks(w.indexes[name], out)
		} else {
			writeIndexBlocks(w.indexes[name], out)
		}
	}
}

// The space index is a SSTable mapping grouping/index
// names to their offsets in the file.
func (w *spaceWriter) writeIndex(out *bytes.Buffer) (length int64, err error) {
	buf := new(bytes.Buffer)
	st := sst.NewWriter(buf)

	sort.Stable(w.indexNames)

	// For each grouping or index, we index the section's
	// byte offset in the file and the length in bytes
	// of all data in the grouping/index.
	for _, name := range w.indexNames {
		buf := new(bytes.Buffer)

		writeUvarint(buf, w.indexes[name].offset)
		writeUvarint(buf, w.indexes[name].length)

		if err = st.Set([]byte(name), buf.Bytes()); err != nil {
			return
		}
	}

	if err = st.Close(); err != nil {
		return
	}

	return buf.WriteTo(out)
}

func (w *spaceWriter) writeFooter(out *bytes.Buffer, indexLen int64) {
	writeInt32(out, int(indexLen))
}
