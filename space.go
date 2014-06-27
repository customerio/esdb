package esdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/customerio/esdb/blocks"
	"github.com/customerio/esdb/sst"
)

type Scanner func(*Event) bool

type Space struct {
	Id []byte

	reader io.ReadSeeker
	blocks *blocks.Reader
	offset uint64
	length uint64
	index  *sst.Reader
}

func openSpace(reader io.ReadSeeker, id []byte, offset, length uint64) *Space {
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
		s.blocks.Seek(int64(s.offset+offset), 0)

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

		var block uint64
		var offset uint16
		binary.Read(reader, binary.LittleEndian, &block)
		binary.Read(reader, binary.LittleEndian, &offset)

		s.blocks.Seek(int64(s.offset+block), 0)
		s.blocks.Read(make([]byte, offset))

		for event := pullEvent(s.blocks); block > 0 && event != nil; {
			if !scanner(event) {
				return
			}

			if next := reader.Peek(1); len(next) == 0 || next[0] == 0 {
				return
			}

			binary.Read(reader, binary.LittleEndian, &block)
			binary.Read(reader, binary.LittleEndian, &offset)

			s.blocks.Seek(int64(s.offset+block), 0)
			s.blocks.Read(make([]byte, offset))

			event = pullEvent(s.blocks)
		}
	}
}

func (s *Space) findGroupingOffset(name string) uint64 {
	if val, err := s.index.Get([]byte("g" + name)); err == nil {
		num, _ := binary.ReadUvarint(bytes.NewReader(val))
		return num
	}

	return 0
}

func (s *Space) findIndex(name, value string) *blocks.Reader {
	if val, err := s.index.Get([]byte("i" + name + ":" + value)); err == nil {
		meta := bufio.NewReader(bytes.NewReader(val))

		offset, _ := binary.ReadUvarint(meta)
		//length, _ := binary.ReadUvarint(meta)

		reader := blocks.NewReader(s.reader, 4096)
		reader.Seek(int64(s.offset+offset), 0)
		return reader
	}

	return nil
}

func findSpaceIndex(r io.ReadSeeker, offset, length uint64) (*sst.Reader, error) {
	r.Seek(int64(offset+length-4), 0)

	var indexLength int32
	binary.Read(r, binary.LittleEndian, &indexLength)

	r.Seek(int64(offset+length-4-uint64(indexLength)), 0)
	index := make([]byte, indexLength)
	r.Read(index)

	return sst.NewReader(bytes.NewReader(index), int64(indexLength))
}
