package esdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sort"

	"github.com/customerio/esdb/sst"
)

type Writer struct {
	file         *os.File
	spaces       map[string]*spaceWriter
	offset       int64
	spaceOffsets map[string]int64
	spaceLengths map[string]int64
	written      bool
}

func New(path string) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:         file,
		spaces:       make(map[string]*spaceWriter),
		spaceOffsets: make(map[string]int64),
		spaceLengths: make(map[string]int64),
	}, nil
}

func (w *Writer) Add(spaceId []byte, data []byte, timestamp int, grouping string, indexes map[string]string) error {
	if w.written {
		return errors.New("Cannot add to database. We're immutable and this one has already been written.")
	}

	space := w.spaces[string(spaceId)]

	if space == nil {
		space = newSpace(w.file, spaceId)
		w.spaces[string(spaceId)] = space
	}

	return space.add(newEvent(data, timestamp), grouping, indexes)
}

func (w *Writer) Flush(spaceId []byte) (err error) {
	if space := w.spaces[string(spaceId)]; space != nil {
		err = w.writeSpace(space)
	}

	return
}

func (w *Writer) Write() (err error) {
	for _, space := range w.spaces {
		if err = w.writeSpace(space); err != nil {
			return
		}
	}

	return w.write()
}

func (w *Writer) write() error {
	w.written = true

	buf := new(bytes.Buffer)

	spaceIds := make(sort.StringSlice, 0)

	for _, space := range w.spaces {
		spaceIds = append(spaceIds, string(space.Id))
	}

	spaceIds.Sort()

	st := sst.NewWriter(buf)

	for _, spaceId := range spaceIds {
		b := new(bytes.Buffer)

		binary.Write(b, binary.LittleEndian, w.spaceOffsets[spaceId])
		binary.Write(b, binary.LittleEndian, w.spaceLengths[spaceId])

		if err := st.Set([]byte(spaceId), b.Bytes()); err != nil {
			return err
		}
	}

	if err := st.Close(); err != nil {
		return err
	}

	binary.Write(buf, binary.LittleEndian, int64(buf.Len()))
	_, err := buf.WriteTo(w.file)
	return err
}

func (w *Writer) writeSpace(space *spaceWriter) (err error) {
	length, err := space.write()

	if err == nil {
		w.spaceOffsets[string(space.Id)] = w.offset
		w.spaceLengths[string(space.Id)] = length
		w.offset += length
	}

	return
}
