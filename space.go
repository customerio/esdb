package esdb

import (
	"bytes"
	"io"

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

func (s *Space) Scan(grouping string, scanner Scanner) {
	if offset := s.findGroupingOffset(grouping); offset > 0 {
		s.blocks.Seek(s.offset+offset, 0)

		for event := pullEvent(s.blocks); event != nil; {
			if !scanner(event) {
				return
			}

			event = pullEvent(s.blocks)
		}
	}
}

func (s *Space) ScanIndex(name, value string, scanner Scanner) {
	if reader := s.findIndex(name, value); reader != nil {
		if next := reader.Peek(1); len(next) == 0 || next[0] == 0 {
			return
		}

		block := readInt64(reader)
		offset := readInt16(reader)

		s.blocks.Seek(s.offset+block, 0)
		readBytes(s.blocks, offset)

		for event := pullEvent(s.blocks); block > 0 && event != nil; {
			if !scanner(event) {
				return
			}

			if next := reader.Peek(1); len(next) == 0 || next[0] == 0 {
				return
			}

			block = readInt64(reader)
			offset = readInt16(reader)

			s.blocks.Seek(s.offset+block, 0)
			readBytes(s.blocks, offset)

			event = pullEvent(s.blocks)
		}
	}
}

func (s *Space) findGroupingOffset(name string) int64 {
	if val, err := s.index.Get([]byte("g" + name)); err == nil {
		return readUvarint(bytes.NewReader(val))
	}

	return 0
}

func (s *Space) findIndex(name, value string) *blocks.Reader {
	if val, err := s.index.Get([]byte("i" + name + ":" + value)); err == nil {
		offset := readUvarint(bytes.NewReader(val))

		reader := blocks.NewReader(s.reader, 4096)
		reader.Seek(s.offset+offset, 0)

		return reader
	}

	return nil
}

func findSpaceIndex(r io.ReadSeeker, offset, length int64) (*sst.Reader, error) {
	footerOffset := offset + length - 4

	r.Seek(footerOffset, 0)
	indexLen := readInt32(r)

	r.Seek(footerOffset-indexLen, 0)
	index := readBytes(r, indexLen)

	return sst.NewReader(bytes.NewReader(index), indexLen)
}
