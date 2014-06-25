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
	blocks       map[string]*blockWriter
	offset       int64
	blockOffsets map[string]int64
	blockLengths map[string]int64
	written      bool
}

func New(path string) (*Writer, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:         file,
		blocks:       make(map[string]*blockWriter),
		blockOffsets: make(map[string]int64),
		blockLengths: make(map[string]int64),
	}, nil
}

func (w *Writer) Add(blockId []byte, data []byte, timestamp int, grouping string, indexes []string) error {
	if w.written {
		return errors.New("Cannot add to database. We're immutable and this one has already been written.")
	}

	block := w.blocks[string(blockId)]

	if block == nil {
		block = newBlock(w.file, blockId)
		w.blocks[string(blockId)] = block
	}

	return block.add(data, timestamp, grouping, indexes)
}

func (w *Writer) Flush(blockId []byte) (err error) {
	if block := w.blocks[string(blockId)]; block != nil {
		err = w.writeBlock(block)
	}

	return
}

func (w *Writer) Write() (err error) {
	for _, block := range w.blocks {
		if err = w.writeBlock(block); err != nil {
			return
		}
	}

	return w.write()
}

func (w *Writer) write() error {
	w.written = true

	buf := new(bytes.Buffer)

	blockIds := make(sort.StringSlice, 0)

	for _, block := range w.blocks {
		blockIds = append(blockIds, string(block.Id))
	}

	blockIds.Sort()

	st := sst.NewWriter(buf)

	for _, blockId := range blockIds {
		b := new(bytes.Buffer)

		binary.Write(b, binary.LittleEndian, w.blockOffsets[blockId])
		binary.Write(b, binary.LittleEndian, w.blockLengths[blockId])

		if err := st.Set([]byte(blockId), b.Bytes()); err != nil {
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

func (w *Writer) writeBlock(block *blockWriter) (err error) {
	length, err := block.write()

	if err == nil {
		w.blockOffsets[string(block.Id)] = w.offset
		w.blockLengths[string(block.Id)] = length
		w.offset += length
	}

	return
}
