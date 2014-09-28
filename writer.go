package esdb

import (
	"bytes"
	"errors"
	"os"
	"sort"

	"github.com/customerio/esdb/binary"
	"github.com/customerio/esdb/sst"
)

// Writer provides an interface for creating a new ESDB file.
type Writer struct {
	file         *os.File
	spaces       map[string]*spaceWriter
	spaceIds     sort.StringSlice
	offset       int64
	spaceOffsets map[string]int64
	spaceLengths map[string]int64
	written      bool
}

// Creates a new ESDB database at the given path. If the
// file already exists, an error will be returned.
func New(path string) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:         file,
		spaces:       make(map[string]*spaceWriter),
		spaceIds:     make(sort.StringSlice, 0),
		spaceOffsets: make(map[string]int64),
		spaceLengths: make(map[string]int64),
	}, nil
}

// Adds a new event to the specified space, with grouping and indexes. Events aren't
// written to the file until writer.Flush(spaceId) or writer.Write() is called.
func (w *Writer) Add(spaceId []byte, data []byte, timestamp int, grouping string, indexes map[string]string) error {
	if w.written {
		return errors.New("Cannot add to database. We're immutable and this one has already been written.")
	}

	space := w.spaces[string(spaceId)]
	event := newEvent(data, timestamp)

	if space == nil {
		space = newSpace(w.file, spaceId)
		w.spaces[string(spaceId)] = space
	}

	return space.add(event, grouping, indexes)
}

// Flush writes an individual space to the file. This prevents any additional events from being
// added to the space. It may be advantagous to flush spaces individually once you've added
// events for that space, as flushing will reduce use the memory usage of creating a new ESDB file.
func (w *Writer) Flush(spaceId []byte) (err error) {
	if space := w.spaces[string(spaceId)]; space != nil {
		err = w.writeSpace(space)
	}

	return
}

// Write flushes any remaining spaces to the file, writes the index
// for locating spaces, and closes the file.
func (w *Writer) Write() (err error) {
	for _, space := range w.spaces {
		if err = w.writeSpace(space); err != nil {
			return
		}
	}

	length, err := w.writeIndex()
	if err != nil {
		return err
	}

	return w.writeFooter(length)
}

func (w *Writer) writeSpace(space *spaceWriter) (err error) {
	length, err := space.write()

	if err == nil {
		w.spaceIds = append(w.spaceIds, string(space.Id))
		w.spaceOffsets[string(space.Id)] = w.offset
		w.spaceLengths[string(space.Id)] = int64(length)
		delete(w.spaces, string(space.Id))
		w.offset += length
	}

	return
}

// The ESDB index is a SSTable mapping space
// ids to their offsets in the file.
func (w *Writer) writeIndex() (int64, error) {
	w.written = true

	buf := new(bytes.Buffer)
	st := sst.NewWriter(buf)

	w.spaceIds.Sort()

	// For each defined space, we index the space's
	// byte offset in the file and the length in bytes
	// of all data in the space.
	for _, spaceId := range w.spaceIds {
		b := new(bytes.Buffer)

		binary.WriteInt64(b, w.spaceOffsets[spaceId])
		binary.WriteInt64(b, w.spaceLengths[spaceId])

		if err := st.Set([]byte(spaceId), b.Bytes()); err != nil {
			return 0, err
		}
	}

	if err := st.Close(); err != nil {
		return 0, err
	}

	return buf.WriteTo(w.file)
}

func (w *Writer) writeFooter(indexLen int64) (err error) {
	buf := new(bytes.Buffer)

	binary.WriteInt64(buf, indexLen)

	_, err = buf.WriteTo(w.file)

	return
}
