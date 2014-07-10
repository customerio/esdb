package esdb

import (
	"bytes"
	"io"
	"strings"

	"github.com/customerio/esdb/blocks"
	"github.com/customerio/esdb/sst"
)

type Scanner func(*Event) bool

type Space struct {
	Id []byte

	reader io.ReadSeeker
	blocks *blocks.Reader
	offset int64
	length int64
	index  *sst.Reader
}

// Opens a space for reading given a reader, and an offset/length of
// the spaces position within the file.
func openSpace(reader io.ReadSeeker, id []byte, offset, length int64) *Space {
	if st, err := findSpaceIndex(reader, offset, length); err == nil {

		return &Space{
			Id:     id,
			index:  st,
			reader: reader,
			blocks: blocks.NewReader(reader, 4096),
			offset: offset,
			length: length,
		}
	}

	return nil
}

// Iterates over grouping index and returns each grouping.
func (s *Space) Iterate(process func(g string) bool) error {
	if iter, err := s.index.Find([]byte("")); err == nil {

		for iter.Next() {
			if strings.HasPrefix(string(iter.Key()), "g") {
				if !process(string(iter.Key()[1:])) {
					break
				}
			}
		}

		return nil
	} else {
		return err
	}
}

func (s *Space) Scan(grouping string, scanner Scanner) {
	if offset := s.findGroupingOffset(grouping); offset > 0 {
		// Move to the beginning of the grouping section in the file.
		s.blocks.Seek(s.offset+offset, 0)

		// Event groupings are sequentially stored. So, pull
		// events out of the muck until we don't find any more.
		for {
			event := pullEvent(s.blocks)

			if event == nil || !scanner(event) {
				return
			}
		}
	}
}

func (s *Space) ScanIndex(name, value string, scanner Scanner) {
	if reader := s.findIndex(name, value); reader != nil {
		for {
			// If the next byte is 0, then that's an empty event offset,
			// marking the end of the index. Nothing more to see here.
			if next := reader.Peek(1); len(next) == 0 || next[0] == 0 {
				return
			}

			// Each entry in the index is a 64 bit integer for the
			// event's block offset in the file, and a 16 bit integer
			// for the event's offset within the block (as each block
			// is 4096 bytes long)
			block := readInt64(reader)
			offset := readInt16(reader)

			// Move to the event's block
			s.blocks.Seek(s.offset+block, 0)

			// Read all data prior to the current event's offset.
			readBytes(s.blocks, offset)

			event := pullEvent(s.blocks)

			if event == nil || !scanner(event) {
				return
			}
		}
	}
}

func (s *Space) findGroupingOffset(name string) int64 {
	if val, err := s.index.Get([]byte("g" + name)); err == nil {
		// The entry in the SSTable index for groupings
		// is variable length integers integers for the
		// offset and length of the index within
		// the file. In this case, we just need the
		// offset to find it's starting point.
		return readUvarint(bytes.NewReader(val))
	}

	return 0
}

// Finds and returns an index by it's name and value.
func (s *Space) findIndex(name, value string) *blocks.Reader {
	if val, err := s.index.Get([]byte("i" + name + ":" + value)); err == nil {
		// The entry in the SSTable index for indexes
		// is variable length integers integers for the
		// offset and length of the index within
		// the file. In this case, we just need the
		// offset to find it's starting point.
		offset := readUvarint(bytes.NewReader(val))

		// An index is block encoded, so fire up
		// a block reader, and seek to the start
		// of the index.
		reader := blocks.NewReader(s.reader, 4096)
		reader.Seek(s.offset+offset, 0)

		return reader
	}

	return nil
}

func findSpaceIndex(r io.ReadSeeker, offset, length int64) (*sst.Reader, error) {
	footerOffset := offset + length - 8

	// The last 8 bytes in the file is the length
	// of the SSTable grouping index.
	r.Seek(footerOffset, 0)
	indexLen := readInt64(r)

	return sst.NewReader(newBoundReader(r, footerOffset-indexLen, footerOffset), indexLen)
}
