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
	offset int64
	length int64
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

	n, err := w.writeHeader(length, w.writer)
	length += n
	if err != nil {
		return
	}

	n, err = w.writeBlocks(length, w.writer)
	length += n
	if err != nil {
		return
	}

	indexN, err := w.writeIndex(length, w.writer)
	length += indexN
	if err != nil {
		return
	}

	n, err = w.writeFooter(length, w.writer, indexN)
	length += n
	if err != nil {
		return
	}

	w.written = true

	return
}

func (w *spaceWriter) writeHeader(written int64, out io.Writer) (int64, error) {
	// Magic character marking this as
	// the start of a space section.
	n, err := out.Write([]byte{42})
	return int64(n), err
}

func (w *spaceWriter) writeBlocks(written int64, out io.Writer) (int64, error) {
	sort.Stable(w.indexNames)

	off := written

	for _, name := range w.indexNames {
		w.indexes[name].offset = off

		buf := new(bytes.Buffer)

		if strings.HasPrefix(name, "g") {
			writeEventBlocks(w.indexes[name], buf)
		} else {
			writeIndexBlocks(w.indexes[name], buf)
		}

		w.indexes[name].evs = nil

		n, err := buf.WriteTo(out)
		off += n
		if err != nil {
			return off, err
		}
	}

	return off - written, nil
}

// The space index is a SSTable mapping grouping/index
// names to their offsets in the file.
func (w *spaceWriter) writeIndex(written int64, out io.Writer) (length int64, err error) {
	buf := new(bytes.Buffer)
	st := sst.NewWriter(buf)

	sort.Stable(w.indexNames)

	// For each grouping or index, we index the section's
	// byte offset in the file and the length in bytes
	// of all data in the grouping/index.
	for _, name := range w.indexNames {
		buf := new(bytes.Buffer)

		writeUvarint64(buf, w.indexes[name].offset)
		writeUvarint64(buf, w.indexes[name].length)

		if err = st.Set([]byte(name), buf.Bytes()); err != nil {
			return
		}
	}

	if err = st.Close(); err != nil {
		return
	}

	return buf.WriteTo(out)
}

func (w *spaceWriter) writeFooter(written int64, out io.Writer, indexLen int64) (length int64, err error) {
	buf := new(bytes.Buffer)
	writeInt64(buf, indexLen)
	return buf.WriteTo(out)
}
